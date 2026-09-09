#include <filesystem>
#include <memory>
#include <optional>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Core/Field.h>
#include <Common/thread_local_rng.h>
#include <Disks/IDisk.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <base/sort.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <zstd.h>

namespace ProfileEvents
{
    extern const Event KeeperSnapshotWrittenBytes;
    extern const Event KeeperSnapshotFileSyncMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric KeeperSnapshotRecoveryThreads;
    extern const Metric KeeperSnapshotRecoveryThreadsActive;
    extern const Metric KeeperSnapshotRecoveryThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int KEEPER_EXCEPTION;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNKNOWN_SNAPSHOT;
    extern const int LOGICAL_ERROR;
}

namespace
{
    int validateSnapshotZstdCompressionLevel(Int64 level)
    {
        const int min_level = ZSTD_minCLevel();
        const int max_level = ZSTD_maxCLevel();
        if (level < min_level || level > max_level)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "snapshot_zstd_compression_level must be between {} and {}, got {}",
                min_level,
                max_level,
                level);

        return static_cast<int>(level);
    }

    std::string getSnapshotFileName(uint64_t up_to_log_idx, bool compress_zstd)
    {
        /// Unique-from-birth name avoids collisions between concurrent same-index writes.
        auto base = fmt::format("snapshot_{}_{:016x}.bin", up_to_log_idx, thread_local_rng());
        if (compress_zstd)
            base += ".zstd";
        return base;
    }

    void cancelAndResetWriteBuffer(std::unique_ptr<WriteBuffer> & buffer)
    {
        if (buffer && !buffer->isFinalized() && !buffer->isCanceled())
            buffer->cancel();

        buffer.reset();
    }

    void removeFailedSnapshotArtifacts(
        const DiskPtr & disk,
        const std::string & snapshot_file_name,
        const std::string & tmp_snapshot_file_name,
        const LoggerPtr & log)
    {
        try
        {
            disk->removeFileIfExists(snapshot_file_name);
            LOG_DEBUG(log, "Ensured partial snapshot artifact {} is absent from disk {}", snapshot_file_name, disk->getName());
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format("Failed to remove partial snapshot artifact {} from disk {}", snapshot_file_name, disk->getName()));
            LOG_WARNING(
                log,
                "Keeping partial snapshot marker {} on disk {} because data file {} could not be removed",
                tmp_snapshot_file_name,
                disk->getName(),
                snapshot_file_name);
            return;
        }

        try
        {
            disk->removeFileIfExists(tmp_snapshot_file_name);
            LOG_DEBUG(log, "Ensured partial snapshot marker {} is absent from disk {}", tmp_snapshot_file_name, disk->getName());
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format("Failed to remove partial snapshot marker {} from disk {}", tmp_snapshot_file_name, disk->getName()));
        }
    }

    /// A file fsync does not persist a directory-entry change, so the marker unlink needs the
    /// directory fsynced too, with the fd opened before the unlink. Snapshots live at the disk
    /// root; getDirectorySyncGuard("") returns nullptr (no-op) on non-local disks.
    void removeSnapshotMarker(const DiskPtr & disk, const std::string & tmp_snapshot_file_name)
    {
        SyncGuardPtr dir_sync_guard = disk->getDirectorySyncGuard("");
        disk->removeFile(tmp_snapshot_file_name);
    }

    void writeNode(std::string_view data, const KeeperNodeStats & stats, SnapshotVersion version, WriteBuffer & out)
    {
        writeBinary(data, out);

        /// Serialize ACL
        if (version >= SnapshotVersion::V7)
            writeBinary(stats.acl_id, out);
        else
            writeBinary(static_cast<uint64_t>(stats.acl_id), out);
        /// Write is_sequential for backwards compatibility
        if (version < SnapshotVersion::V6)
            writeBinary(false, out);

        /// Serialize stat
        writeBinary(stats.czxid, out);
        writeBinary(stats.mzxid, out);
        writeBinary(stats.getCTime(), out);
        writeBinary(stats.mtime, out);
        writeBinary(stats.version, out);
        writeBinary(stats.cversion, out);
        writeBinary(stats.aversion, out);
        writeBinary(stats.getEphemeralOwner(), out);
        if (version < SnapshotVersion::V6)
            writeBinary(static_cast<int32_t>(stats.data_size), out);
        writeBinary(stats.getNumChildren(), out);
        writeBinary(stats.pzxid, out);

        if (version >= SnapshotVersion::V7)
            writeBinary(stats.getSeqNum(), out);
        else
        {
            auto seq_num = stats.getSeqNum();
            if (seq_num < std::numeric_limits<int32_t>::min() || seq_num > std::numeric_limits<int32_t>::max())
                throw Exception(ErrorCodes::KEEPER_EXCEPTION,
                    "Sequential node counter {} overflows int32, upgrade to snapshot version >= V7", seq_num);
            writeBinary(static_cast<int32_t>(seq_num), out);
        }

        if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
        {
            size_t approx_size_in_memory = sizeof(KeeperNodeStats) + stats.data_size + stats.getNumChildren() * 20;
            writeBinary(approx_size_in_memory, out);
        }

        if (version >= SnapshotVersion::V8)
        {
            writeBinary(stats.isTTL(), out);
            if (stats.isTTL())
                writeBinary(stats.getTTL(), out);
        }
        else if (stats.isTTL())
        {
            /// KeeperContext::validateWriteSnapshotVersion already checked that CREATE_TTL feature
            /// flag is disabled, so this is pretty unexpected. Still possible if the feature flag
            /// was disabled after the node was created.
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot serialize snapshot with version {}: storage contains TTL node, which requires write_snapshot_version "
                "{} or higher.",
                static_cast<uint8_t>(version),
                static_cast<uint8_t>(SnapshotVersion::V8));
        }

        if (stats.isContainer() && version < SnapshotVersion::V9)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot serialize snapshot with version {}: storage contains container node, which requires write_snapshot_version "
                "{} or higher.",
                static_cast<uint8_t>(version),
                static_cast<uint8_t>(SnapshotVersion::V9));
        }
    }

    void serializeSnapshotMetadata(const SnapshotMetadataPtr & snapshot_meta, WriteBuffer & out)
    {
        auto buffer = snapshot_meta->serialize();
        writeVarUInt(buffer->size(), out);
        out.write(reinterpret_cast<const char *>(buffer->data_begin()), buffer->size());
    }

    SnapshotMetadataPtr deserializeSnapshotMetadata(ReadBuffer & in)
    {
        size_t data_size = 0;
        readVarUInt(data_size, in);
        auto buffer = nuraft::buffer::alloc(data_size);
        in.readStrict(reinterpret_cast<char *>(buffer->data_begin()), data_size);
        buffer->pos(0);
        return SnapshotMetadata::deserialize(*buffer);
    }
}

void KeeperStorageSnapshot::serialize(const KeeperStorageSnapshot & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context)
{
    writeBinary(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);

    if (snapshot.version >= SnapshotVersion::V5)
    {
        writeBinary(snapshot.zxid, out);
        if (keeper_context->digestEnabled())
        {
            writeBinary(static_cast<uint8_t>(KEEPER_CURRENT_DIGEST_VERSION), out);
            writeBinary(snapshot.nodes_digest, out);
        }
        else
            writeBinary(static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST), out);
    }

    writeBinary(snapshot.session_id, out);

    /// Better to sort before serialization, otherwise snapshots can be different on different replicas
    std::vector<std::pair<ACLId, Coordination::ACLs>> sorted_acl_map(snapshot.acl_map.begin(), snapshot.acl_map.end());
    ::sort(sorted_acl_map.begin(), sorted_acl_map.end());
    /// Serialize ACLs map
    writeBinary(sorted_acl_map.size(), out);
    for (const auto & [acl_id, acls] : sorted_acl_map)
    {
        if (snapshot.version >= SnapshotVersion::V7)
            writeBinary(acl_id, out);
        else
            writeBinary(static_cast<uint64_t>(acl_id), out);
        writeBinary(acls.size(), out);
        for (const auto & acl : acls)
        {
            writeBinary(acl.permissions, out);
            writeBinary(acl.scheme, out);
            writeBinary(acl.id, out);
        }
    }

    /// Serialize data tree
    writeBinary(snapshot.view->getNodeCount() - keeper_context->getSystemNodesWithData().size(), out);
    std::string_view node_path;
    std::string_view node_data;
    KeeperNodeStats node_stats;
    size_t nodes_seen = 0;
    while (snapshot.view->next(node_path, node_data, node_stats))
    {
        ++nodes_seen;
        // write only the root system path because of digest
        if (Coordination::matchPath(node_path, keeper_system_path) == Coordination::PathMatchResult::IS_CHILD)
            continue;

        /// (This is guaranteed because KeeperStorageSnapshot constructor is called with nuraft's
        ///  commit_lock_ held, and therefore storage can't change between when we get storage->zxid
        ///  and when we issue the read view.)
        if (node_stats.mzxid > snapshot.zxid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to serialize node with mzxid {}, but last snapshot index {}", node_stats.mzxid, snapshot.zxid);

        writeBinary(node_path, out);
        writeNode(node_data, node_stats, snapshot.version, out);
    }
    chassert(nodes_seen == snapshot.view->getNodeCount());

    /// Session must be saved in a sorted order,
    /// otherwise snapshots will be different
    std::vector<std::pair<int64_t, int64_t>> sorted_session_and_timeout(
        snapshot.session_and_timeout.begin(), snapshot.session_and_timeout.end());
    ::sort(sorted_session_and_timeout.begin(), sorted_session_and_timeout.end());

    /// Serialize sessions
    size_t size = sorted_session_and_timeout.size();

    writeBinary(size, out);
    for (const auto & [session_id, timeout] : sorted_session_and_timeout)
    {
        writeBinary(session_id, out);
        writeBinary(timeout, out);

        KeeperStorage::AuthIDs ids;
        if (snapshot.session_and_auth.contains(session_id))
            ids = snapshot.session_and_auth.at(session_id);

        writeBinary(ids.size(), out);
        for (const auto & [scheme, id] : ids)
        {
            writeBinary(scheme, out);
            writeBinary(id, out);
        }
    }

    /// Serialize cluster config
    if (snapshot.cluster_config)
    {
        auto buffer = snapshot.cluster_config->serialize();
        writeVarUInt(buffer->size(), out);
        out.write(reinterpret_cast<const char *>(buffer->data_begin()), buffer->size());
    }
}

KeeperStorageSnapshot::KeeperStorageSnapshot(KeeperStorage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_)
    : KeeperStorageSnapshot(
          storage_, std::make_shared<SnapshotMetadata>(up_to_log_idx_, 0, std::make_shared<nuraft::cluster_config>()), cluster_config_, version_)
{
}

KeeperStorageSnapshot::KeeperStorageSnapshot(
    KeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_)
    : storage(storage_)
    , version(version_)
    , snapshot_meta(snapshot_meta_)
    , session_id(storage->session_id_counter)
    , view(storage->issueReadView())
    , cluster_config(cluster_config_)
    , zxid(storage->zxid)
    , nodes_digest(storage->nodes_digest)
{
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
}

SnapshotFileInfoPtr
KeeperSnapshotManager::makeManagedSnapshotFileInfo(std::string path, DiskPtr disk, uint64_t log_idx) const
{
    return std::shared_ptr<SnapshotFileInfo>(
        new SnapshotFileInfo{std::move(path), std::move(disk)},
        [logger = log, log_idx](SnapshotFileInfo * p) noexcept
        {
            try
            {
                /// Unlink only explicitly retired snapshots (publish-race losers, failed creates,
                /// outdated-snapshot maintenance, corruption recovery). Manager destruction keeps files.
                if (p->retired_for_removal.load(std::memory_order_acquire))
                {
                    p->disk->removeFileIfExists(p->path);
                    if (p->recovery_marker_path)
                        p->disk->removeFileIfExists(*p->recovery_marker_path);
                    LOG_DEBUG(logger, "Removed retired snapshot {} at path {}", log_idx, p->path);
                }
            }
            catch (...)
            {
                /// Log failed unlinks; constructor scan handles leftover files.
                LOG_ERROR(logger, "Failed to remove snapshot file {} via deleter: {}",
                          p->path, getCurrentExceptionMessage(/*with_stacktrace=*/true));
            }
            delete p;
        });
}

KeeperSnapshotManager::KeeperSnapshotManager(
    size_t snapshots_to_keep_,
    const KeeperContextPtr & keeper_context_,
    bool compress_snapshots_zstd_,
    Int64 snapshot_zstd_compression_level_)
    : snapshots_to_keep(snapshots_to_keep_)
    , compress_snapshots_zstd(compress_snapshots_zstd_)
    , snapshot_zstd_compression_level(validateSnapshotZstdCompressionLevel(snapshot_zstd_compression_level_))
    , keeper_context(keeper_context_)
{
    struct DiskInventory
    {
        DiskPtr disk;
        size_t precedence = 0;
        std::vector<std::pair<uint64_t, String>> snapshots;
        std::vector<std::pair<String, String>> markers; /// target basename, marker path
    };

    std::vector<DiskInventory> inventories;
    const auto add_disk = [&](const DiskPtr & candidate, size_t precedence)
    {
        auto it = std::ranges::find(inventories, candidate, &DiskInventory::disk);
        if (it == inventories.end())
            inventories.push_back({.disk = candidate, .precedence = precedence, .snapshots = {}, .markers = {}});
        else
            it->precedence = std::max(it->precedence, precedence);
    };
    size_t precedence = 0;
    for (const auto & old_disk : keeper_context->getOldSnapshotDisks())
        add_disk(old_disk, precedence++);
    add_disk(getDisk(), precedence++);
    add_disk(getLatestSnapshotDisk(), precedence++);
    const auto inventory_disk = [&](DiskInventory & inventory)
    {
        LOG_TRACE(log, "Inventorying snapshots on disk {}", inventory.disk->getName());
        for (auto it = inventory.disk->iterateDirectory(""); it->isValid(); it->next())
        {
            const auto & name = it->name();
            if (name.starts_with(tmp_keeper_file_prefix))
            {
                LOG_TRACE(log, "Found snapshot move marker {} on disk {}", it->path(), inventory.disk->getName());
                inventory.markers.emplace_back(name.substr(tmp_keeper_file_prefix.size()), it->path());
            }
            else if (name.starts_with("snapshot_"))
            {
                LOG_TRACE(log, "Found snapshot {} on disk {}", it->path(), inventory.disk->getName());
                inventory.snapshots.emplace_back(getLogIdxFromSnapshotPath(it->path()), it->path());
            }
        }
    };

    if (inventories.size() == 1)
    {
        inventory_disk(inventories.front());
    }
    else
    {
        ThreadPool pool(
            CurrentMetrics::KeeperSnapshotRecoveryThreads,
            CurrentMetrics::KeeperSnapshotRecoveryThreadsActive,
            CurrentMetrics::KeeperSnapshotRecoveryThreadsScheduled,
            inventories.size() - 1,
            /*max_free_threads_*/ 0,
            /*queue_size_*/ 0);
        for (auto & inventory : inventories)
            if (&inventory != &inventories.front())
                pool.scheduleOrThrowOnError([&inventory, &inventory_disk] { inventory_disk(inventory); });
        inventory_disk(inventories.front());
        pool.wait();
    }

    struct SnapshotRecoveryCandidate
    {
        SnapshotFileInfoPtr info;
        std::optional<String> marker_path;
        bool has_unknown_marker_version = false;
    };

    std::map<uint64_t, std::vector<SnapshotRecoveryCandidate>> groups;
    std::vector<std::pair<DiskPtr, String>> orphan_markers;
    for (auto & inventory : inventories)
    {
        std::unordered_map<String, String> markers;
        for (auto & [target_name, marker_path] : inventory.markers)
            markers.emplace(target_name, marker_path);

        for (auto & [log_idx, path] : inventory.snapshots)
        {
            SnapshotRecoveryCandidate candidate{
                .info = makeManagedSnapshotFileInfo(path, inventory.disk, log_idx),
                .marker_path = std::nullopt};
            candidate.info->recovery_precedence = inventory.precedence;
            const String basename = fs::path(path).filename();
            if (auto marker_it = markers.find(basename); marker_it != markers.end())
            {
                candidate.marker_path = marker_it->second;
                markers.erase(marker_it);
            }
            groups[log_idx].push_back(std::move(candidate));
        }
        for (auto & [name, marker_path] : markers)
            orphan_markers.emplace_back(inventory.disk, marker_path);
    }

    /// Process independent logical groups concurrently. Markers are resolved before disk precedence.
    std::vector<std::vector<SnapshotRecoveryCandidate> *> unresolved_group_refs;
    for (auto & [log_idx, candidates] : groups)
    {
        if (candidates.size() == 1 && !candidates.front().marker_path)
        {
            const auto & candidate = candidates.front();
            LOG_TRACE(log, "Using snapshot {} from disk {}", candidate.info->path, candidate.info->disk->getName());
            existing_snapshots.emplace(log_idx, candidate.info);
        }
        else
            unresolved_group_refs.push_back(&candidates);
    }

    std::vector<SnapshotFileInfoPtr> selected_snapshots(unresolved_group_refs.size());
    std::vector<std::vector<SnapshotFileInfoPtr>> duplicate_snapshots(unresolved_group_refs.size());
    if (!unresolved_group_refs.empty())
    {
        ThreadPool pool(
            CurrentMetrics::KeeperSnapshotRecoveryThreads,
            CurrentMetrics::KeeperSnapshotRecoveryThreadsActive,
            CurrentMetrics::KeeperSnapshotRecoveryThreadsScheduled,
            std::min(inventories.size(), unresolved_group_refs.size()),
            /*max_free_threads_*/ 0,
            /*queue_size_*/ 0);
        for (size_t group_index = 0; group_index < unresolved_group_refs.size(); ++group_index)
        {
            pool.scheduleOrThrowOnError([&, group_index]
            {
                auto & candidates = *unresolved_group_refs[group_index];
                for (auto it = candidates.begin(); it != candidates.end();)
                {
                    if (!it->marker_path)
                    {
                        ++it;
                        continue;
                    }

                    const auto marker = readKeeperMoveMarker(it->info->disk, *it->marker_path);
                    const bool matches_marker = marker
                        && it->info->disk->getFileSize(it->info->path) == marker->size
                        && computeKeeperFileDigest(it->info->disk, it->info->path) == *marker;
                    if (matches_marker)
                    {
                        LOG_TRACE(
                            log,
                            "Snapshot {} on disk {} matches move marker {}, removing the marker",
                            it->info->path,
                            it->info->disk->getName(),
                            *it->marker_path);
                        removeKeeperFileIfExists(it->info->disk, *it->marker_path);
                        it->marker_path.reset();
                        ++it;
                        continue;
                    }

                    if (!marker && marker.error() == KeeperMoveMarkerParseError::LegacyEmpty)
                    {
                        LOG_TRACE(
                            log,
                            "Removing incomplete snapshot {} and legacy marker {} from disk {}",
                            it->info->path,
                            *it->marker_path,
                            it->info->disk->getName());
                        removeKeeperFileIfExists(it->info->disk, it->info->path);
                        removeKeeperFileIfExists(it->info->disk, *it->marker_path);
                        it = candidates.erase(it);
                        continue;
                    }

                    if (!marker && marker.error() == KeeperMoveMarkerParseError::UnknownVersion)
                    {
                        it->has_unknown_marker_version = true;
                        ++it;
                        continue;
                    }

                    if (candidates.size() == 1)
                    {
                        LOG_WARNING(
                            log,
                            "Snapshot {} on disk {} does not match move marker {}; removing the marker and keeping the only recovery candidate",
                            it->info->path,
                            it->info->disk->getName(),
                            *it->marker_path);
                        removeKeeperFileIfExists(it->info->disk, *it->marker_path);
                        it->marker_path.reset();
                        ++it;
                        continue;
                    }

                    LOG_TRACE(
                        log,
                        "Snapshot {} on disk {} does not match move marker {}, removing the snapshot and marker",
                        it->info->path,
                        it->info->disk->getName(),
                        *it->marker_path);
                    removeKeeperFileIfExists(it->info->disk, it->info->path);
                    removeKeeperFileIfExists(it->info->disk, *it->marker_path);
                    it = candidates.erase(it);
                }

                if (candidates.empty())
                    return;

                if (std::ranges::all_of(candidates, &SnapshotRecoveryCandidate::has_unknown_marker_version))
                {
                    const auto fallback = std::ranges::max_element(candidates, {}, [](const auto & candidate)
                    {
                        return candidate.info->recovery_precedence;
                    });
                    LOG_WARNING(
                        log,
                        "All snapshot recovery candidates for index {} have an unknown marker version; removing marker {} and using {} on disk {}",
                        getLogIdxFromSnapshotPath(fallback->info->path),
                        *fallback->marker_path,
                        fallback->info->path,
                        fallback->info->disk->getName());
                    removeKeeperFileIfExists(fallback->info->disk, *fallback->marker_path);
                    fallback->marker_path.reset();
                    fallback->has_unknown_marker_version = false;
                }

                std::erase_if(candidates, [&](const auto & candidate)
                {
                    if (!candidate.has_unknown_marker_version)
                        return false;

                    LOG_WARNING(
                        log,
                        "Keeping snapshot {} and unknown-version marker {} on disk {} as a recovery copy; excluding it from replay selection",
                        candidate.info->path,
                        *candidate.marker_path,
                        candidate.info->disk->getName());
                    candidate.info->recovery_marker_path = *candidate.marker_path;
                    duplicate_snapshots[group_index].push_back(candidate.info);
                    return true;
                });

                const auto selected = std::ranges::max_element(candidates, {}, [](const auto & candidate)
                {
                    return candidate.info->recovery_precedence;
                });
                for (const auto & candidate : candidates)
                {
                    if (&candidate != &*selected)
                    {
                        LOG_WARNING(
                            log,
                            "Keeping duplicate snapshot {} on disk {} as a recovery copy; using {} on disk {}",
                            candidate.info->path,
                            candidate.info->disk->getName(),
                            selected->info->path,
                            selected->info->disk->getName());
                        duplicate_snapshots[group_index].push_back(candidate.info);
                    }
                }
                auto selected_snapshot = selected->info;
                LOG_TRACE(log, "Using snapshot {} from disk {}", selected_snapshot->path, selected_snapshot->disk->getName());
                selected_snapshots[group_index] = std::move(selected_snapshot);
            });
        }
        pool.wait();
    }

    for (const auto & selected : selected_snapshots)
    {
        if (selected)
            existing_snapshots.emplace(getLogIdxFromSnapshotPath(selected->path), selected);
    }

    std::optional<uint64_t> oldest_retained_idx;
    if (!existing_snapshots.empty() && snapshots_to_keep > 0)
    {
        const size_t purged_count
            = existing_snapshots.size() > snapshots_to_keep ? existing_snapshots.size() - snapshots_to_keep : 0;
        oldest_retained_idx = std::next(existing_snapshots.begin(), purged_count)->first;
    }
    for (size_t group_index = 0; group_index < duplicate_snapshots.size(); ++group_index)
    {
        if (!selected_snapshots[group_index] || duplicate_snapshots[group_index].empty())
            continue;

        const uint64_t log_idx = getLogIdxFromSnapshotPath(selected_snapshots[group_index]->path);
        if (oldest_retained_idx && log_idx >= *oldest_retained_idx)
        {
            retained_duplicate_snapshots.emplace(log_idx, std::move(duplicate_snapshots[group_index]));
            continue;
        }

        for (const auto & duplicate : duplicate_snapshots[group_index])
        {
            LOG_WARNING(
                log,
                "Removing duplicate snapshot {} on disk {} for log index {} outside the retained window",
                duplicate->path,
                duplicate->disk->getName(),
                log_idx);
            removeKeeperFileIfExists(duplicate->disk, duplicate->path);
            if (duplicate->recovery_marker_path)
                removeKeeperFileIfExists(duplicate->disk, *duplicate->recovery_marker_path);
        }
    }
    for (const auto & marker : orphan_markers)
    {
        LOG_TRACE(log, "Removing orphaned snapshot move marker {} from disk {}", marker.second, marker.first->getName());
        removeKeeperFileIfExists(marker.first, marker.second);
    }
}

SnapshotFileInfoPtr KeeperSnapshotManager::writeSnapshotBufferToFile(nuraft::buffer & buffer, uint64_t up_to_log_idx)
{
    const auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);

    ReadBufferFromNuraftBuffer reader(buffer);

    auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;

    auto disk = getLatestSnapshotDisk();
    LOG_DEBUG(log, "Receiving snapshot {} to {} disk", up_to_log_idx, isLocalDisk(*disk) ? "local" : "remote");

    std::unique_ptr<WriteBuffer> plain_buf;
    try
    {
        /// Create empty marker: if both tmp_<name> and <name> exist on restart, the snapshot
        /// is treated as incomplete and both are removed (see KeeperSnapshotManager constructor).
        {
            auto buf = disk->writeFile(tmp_snapshot_file_name);
            buf->finalize();
        }

        plain_buf = disk->writeFile(snapshot_file_name);
        copyData(reader, *plain_buf);

        const size_t bytes_written = plain_buf->count();
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, bytes_written);

        plain_buf->finalize();

        Stopwatch watch;
        plain_buf->sync();
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotFileSyncMicroseconds, watch.elapsedMicroseconds());

        plain_buf.reset();
        removeSnapshotMarker(disk, tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(plain_buf);
        removeFailedSnapshotArtifacts(disk, snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    return makeManagedSnapshotFileInfo(snapshot_file_name, disk, up_to_log_idx);
}

SnapshotFileInfoPtr KeeperSnapshotManager::tryReuseRegisteredSnapshot(uint64_t up_to_log_idx) const
{
    if (auto it = existing_snapshots.find(up_to_log_idx); it != existing_snapshots.end())
    {
        LOG_INFO(
            log,
            "Snapshot with log index {} is already registered at path {} on disk {}, reusing existing metadata without rewriting it",
            up_to_log_idx,
            it->second->path,
            it->second->disk->getName());
        return it->second;
    }
    return nullptr;
}

SnapshotFileInfoPtr KeeperSnapshotManager::publishAndRunMaintenance(uint64_t up_to_log_idx, SnapshotFileInfoPtr written)
{
    auto published_snapshot_file_info = publishSnapshotFile(up_to_log_idx, written);
    if (published_snapshot_file_info != written)
        retireUnpublishedSnapshotFile(written);
    runMaintenanceInline(up_to_log_idx);
    return published_snapshot_file_info;
}

SnapshotFileInfoPtr KeeperSnapshotManager::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx)
{
    if (auto existing = tryReuseRegisteredSnapshot(up_to_log_idx))
        return existing;

    auto snapshot_file_info = writeSnapshotBufferToFile(buffer, up_to_log_idx);
    return publishAndRunMaintenance(up_to_log_idx, snapshot_file_info);
}

std::unique_ptr<SnapshotReceiveCtx> KeeperSnapshotManager::beginSnapshotReceiveToDisk(uint64_t up_to_log_idx)
{
    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);

    auto disk = getLatestSnapshotDisk();
    LOG_DEBUG(log, "Receiving snapshot {} to {} disk", up_to_log_idx, isLocalDisk(*disk) ? "local" : "remote");
    const auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;

    try
    {
        /// Create an empty tmp_ marker file. On restart, if both tmp_<name> and <name> exist,
        /// the snapshot is treated as incomplete and both are removed (see constructor).
        {
            auto buf = disk->writeFile(tmp_snapshot_file_name);
            buf->finalize();
        }

        auto write_buf = disk->writeFile(snapshot_file_name);
        return std::make_unique<SnapshotReceiveCtx>(std::move(write_buf), disk, std::move(snapshot_file_name), up_to_log_idx);
    }
    catch (...)
    {
        removeFailedSnapshotArtifacts(disk, snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }
}

SnapshotFileInfoPtr KeeperSnapshotManager::finalizeSnapshotReceiveToDisk(SnapshotReceiveCtx & ctx)
{
    const auto tmp_snapshot_file_name = "tmp_" + ctx.snapshot_file_name;

    try
    {
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, ctx.write_buf->count());

        ctx.write_buf->finalize();

        Stopwatch watch;
        ctx.write_buf->sync();
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotFileSyncMicroseconds, watch.elapsedMicroseconds());

        ctx.write_buf.reset();
        removeSnapshotMarker(ctx.disk, tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(ctx.write_buf);
        removeFailedSnapshotArtifacts(ctx.disk, ctx.snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    auto snapshot_file_info = makeManagedSnapshotFileInfo(ctx.snapshot_file_name, ctx.disk, ctx.log_idx);
    return publishAndRunMaintenance(ctx.log_idx, snapshot_file_info);
}

nuraft::ptr<nuraft::buffer> KeeperSnapshotManager::deserializeLatestSnapshotBufferFromDisk()
{
    while (!existing_snapshots.empty())
    {
        auto latest_itr = existing_snapshots.rbegin();
        try
        {
            return deserializeSnapshotBufferFromDisk(latest_itr->first);
        }
        catch (const DB::Exception &)
        {
            /// Retire unreadable snapshots through the managed deleter.
            auto retired_info = latest_itr->second;
            retired_info->retired_for_removal.store(true, std::memory_order_release);
            LOG_WARNING(log, "Removing corrupt snapshot {} at path {}",
                        latest_itr->first, retired_info->path);
            existing_snapshots.erase(latest_itr->first);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    return nullptr;
}

nuraft::ptr<nuraft::buffer> KeeperSnapshotManager::deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const
{
    const auto & snapshot_info = *existing_snapshots.at(up_to_log_idx);
    return deserializeSnapshotBufferFromDisk(snapshot_info);
}

nuraft::ptr<nuraft::buffer> KeeperSnapshotManager::deserializeSnapshotBufferFromDisk(const SnapshotFileInfo & snapshot_info) const
{
    WriteBufferFromNuraftBuffer writer;
    auto reader = snapshot_info.disk->readFile(snapshot_info.path, getReadSettings());
    copyData(*reader, writer);
    return writer.getBuffer();
}

nuraft::ptr<nuraft::buffer> KeeperSnapshotManager::serializeSnapshotToBuffer(const KeeperStorageSnapshot & snapshot) const
{
    std::unique_ptr<WriteBufferFromNuraftBuffer> writer = std::make_unique<WriteBufferFromNuraftBuffer>();
    auto * buffer_raw_ptr = writer.get();
    std::unique_ptr<WriteBuffer> compressed_writer;
    if (compress_snapshots_zstd)
        compressed_writer = wrapWriteBufferWithCompressionMethod(
            std::move(writer), CompressionMethod::Zstd, snapshot_zstd_compression_level);
    else
        /// Pin `LZ4` explicitly: this is the legacy custom-frame snapshot format, so it must stay `LZ4`
        /// independently of the server's default compression codec.
        compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer, CompressionCodecFactory::instance().get("LZ4", {}));

    KeeperStorageSnapshot::serialize(snapshot, *compressed_writer, keeper_context);
    compressed_writer->finalize();
    return buffer_raw_ptr->getBuffer();
}

bool KeeperSnapshotManager::isZstdCompressed(nuraft::ptr<nuraft::buffer> buffer)
{
    static constexpr unsigned char ZSTD_COMPRESSED_MAGIC[4] = {0x28, 0xB5, 0x2F, 0xFD};

    ReadBufferFromNuraftBuffer reader(buffer);
    unsigned char magic_from_buffer[4]{};
    reader.readStrict(reinterpret_cast<char *>(&magic_from_buffer), sizeof(magic_from_buffer));
    buffer->pos(0);
    return memcmp(magic_from_buffer, ZSTD_COMPRESSED_MAGIC, 4) == 0;
}

std::unique_ptr<KeeperSnapshotReader> KeeperSnapshotManager::makeSnapshotReader(nuraft::ptr<nuraft::buffer> buffer) const
{
    bool is_zstd_compressed = isZstdCompressed(buffer);

    if (is_zstd_compressed)
    {
        const size_t frame_size = ZSTD_findFrameCompressedSize(buffer->data_begin(), buffer->size());
        if (ZSTD_isError(frame_size))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid ZSTD snapshot frame: {}",
                ZSTD_getErrorName(frame_size));
        if (frame_size != buffer->size())
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "ZSTD snapshot frame has {} trailing bytes",
                buffer->size() - frame_size);
    }

    std::unique_ptr<ReadBuffer> in = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);

    if (is_zstd_compressed)
        in = wrapReadBufferWithCompressionMethod(std::move(in), CompressionMethod::Zstd);
    else
        in = std::make_unique<CompressedReadBuffer>(std::move(in));

    return std::make_unique<KeeperSnapshotReader>(std::move(in), keeper_context);
}

SnapshotDeserializationResult KeeperSnapshotManager::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer, KeeperStorage & storage) const
{
    auto reader = makeSnapshotReader(buffer);
    storage.loadFromSnapshot(*reader);

    SnapshotDeserializationResult result;
    result.snapshot_meta = reader->snapshot_meta;
    result.cluster_config = reader->cluster_config;
    return result;
}

SnapshotMetadataPtr KeeperSnapshotManager::deserializeSnapshotMetadataFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const
{
    auto reader = makeSnapshotReader(buffer);
    reader->readMetadata();
    return reader->snapshot_meta;
}

SnapshotDeserializationResult KeeperSnapshotManager::restoreFromLatestSnapshot(KeeperStorage & storage)
{
    if (existing_snapshots.empty())
        return {};

    auto buffer = deserializeLatestSnapshotBufferFromDisk();
    if (!buffer)
        return {};
    return deserializeSnapshotFromBuffer(buffer, storage);
}

DiskPtr KeeperSnapshotManager::getDisk() const
{
    return keeper_context->getSnapshotDisk();
}

DiskPtr KeeperSnapshotManager::getLatestSnapshotDisk() const
{
    return keeper_context->getLatestSnapshotDisk();
}

void KeeperSnapshotManager::setProtectedSnapshotIndex(uint64_t log_idx)
{
    protected_snapshot_log_idx = log_idx;
}

void KeeperSnapshotManager::setProtectedPendingSnapshotIndex(uint64_t log_idx)
{
    protected_pending_snapshot_log_idx = log_idx;
}

std::vector<SnapshotFileInfoPtr>
KeeperSnapshotManager::detachSnapshotForRemoval(std::map<uint64_t, SnapshotFileInfoPtr>::iterator itr)
{
    const uint64_t log_idx = itr->first;
    std::vector<SnapshotFileInfoPtr> retired;
    auto snapshot_file_info = itr->second;
    snapshot_file_info->retired_for_removal.store(true, std::memory_order_release);
    existing_snapshots.erase(itr);
    retired.push_back(std::move(snapshot_file_info));

    if (auto duplicate_it = retained_duplicate_snapshots.find(log_idx); duplicate_it != retained_duplicate_snapshots.end())
    {
        for (auto & duplicate : duplicate_it->second)
        {
            duplicate->retired_for_removal.store(true, std::memory_order_release);
            retired.push_back(std::move(duplicate));
        }
        retained_duplicate_snapshots.erase(duplicate_it);
    }

    return retired;
}

std::vector<SnapshotFileInfoPtr> KeeperSnapshotManager::detachOutdatedSnapshotsIfNeeded(uint64_t just_written_log_idx)
{
    /// Keep the `snapshots_to_keep` newest snapshots, plus the protected (mark-backing) entry,
    /// the pending-install entry, and the just-written entry. Worst-case: snapshots_to_keep + 3.
    std::vector<SnapshotFileInfoPtr> retired_snapshots;
    size_t pinned_below = 0;
    auto candidate = existing_snapshots.begin();
    while (candidate != existing_snapshots.end()
           && existing_snapshots.size() > snapshots_to_keep + pinned_below)
    {
        if (candidate->first == protected_snapshot_log_idx
            || candidate->first == protected_pending_snapshot_log_idx
            || candidate->first == just_written_log_idx)
        {
            ++pinned_below;
            ++candidate;
            continue;
        }
        auto to_remove = candidate++;
        auto detached = detachSnapshotForRemoval(to_remove);
        retired_snapshots.insert(
            retired_snapshots.end(), std::make_move_iterator(detached.begin()), std::make_move_iterator(detached.end()));
    }
    return retired_snapshots;
}

std::vector<SnapshotMoveCandidate> KeeperSnapshotManager::selectSnapshotsToMove()
{
    std::vector<SnapshotMoveCandidate> move_candidates;
    auto regular_disk = getDisk();
    auto latest_snapshot_disk = getLatestSnapshotDisk();
    auto latest_snapshot_idx = getLatestSnapshotIndex();

    for (auto & [idx, file_info] : existing_snapshots)
    {
        DiskPtr target_disk = (idx == latest_snapshot_idx) ? latest_snapshot_disk : regular_disk;

        if (file_info->disk == target_disk)
            continue;

        if (file_info->retired_for_removal.load(std::memory_order_acquire))
            continue;

        const int64_t count = file_info.use_count();
        if (count > 1)
        {
            LOG_DEBUG(log, "Deferring move of snapshot {} - has {} outside references", idx, count - 1);
            continue;
        }

        move_candidates.push_back(
            SnapshotMoveCandidate{
                .log_idx = idx,
                .file_info = file_info,
                .source_disk = file_info->disk,
                .source_path = file_info->path,
                .target_disk = target_disk,
                .target_path = file_info->path,
            });
    }

    return move_candidates;
}

SnapshotMaintenanceTasks KeeperSnapshotManager::prepareSnapshotMaintenanceTasks(uint64_t just_written_log_idx)
{
    SnapshotMaintenanceTasks tasks;
    tasks.retired_snapshots = detachOutdatedSnapshotsIfNeeded(just_written_log_idx);
    tasks.move_candidates = selectSnapshotsToMove();
    return tasks;
}

void KeeperSnapshotManager::removeSnapshot(uint64_t log_idx)
{
    /// Tests/tools only: the dropped pins unlink synchronously here; the server reclaims
    /// via the deferred Phase 4 path.
    auto it = existing_snapshots.find(log_idx);
    if (it == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);
    detachSnapshotForRemoval(it);
}

SnapshotFileInfoPtr KeeperSnapshotManager::writeSnapshotFile(const KeeperStorageSnapshot & snapshot)
{
    auto up_to_log_idx = snapshot.snapshot_meta->get_last_log_idx();
    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);
    auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;

    auto disk = getLatestSnapshotDisk();
    std::unique_ptr<WriteBuffer> writer;
    std::unique_ptr<WriteBuffer> compressed_writer;
    try
    {
        /// Create empty marker: if both tmp_<name> and <name> exist on restart, the snapshot
        /// is treated as incomplete and both are removed (see KeeperSnapshotManager constructor).
        {
            auto buf = disk->writeFile(tmp_snapshot_file_name);
            buf->finalize();
        }

        writer = disk->writeFile(snapshot_file_name);
        /// A CompressedWriteBuffer does not own the buffer it writes into, so in that case the file
        /// buffer needs its own finalize and sync. The zstd decorator owns it and forwards both,
        /// which is why only the uncompressed branch tracks the file buffer separately.
        WriteBuffer * unowned_file_buffer = nullptr;
        if (compress_snapshots_zstd)
            compressed_writer = wrapWriteBufferWithCompressionMethod(
                std::move(writer), CompressionMethod::Zstd, snapshot_zstd_compression_level);
        else
        {
            unowned_file_buffer = writer.get();
            /// Pin `LZ4` explicitly: this is the legacy custom-frame snapshot format, so it must stay `LZ4`
            /// independently of the server's default compression codec.
            compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer, CompressionCodecFactory::instance().get("LZ4", {}));
        }

        const size_t bytes_before = compressed_writer->count();
        KeeperStorageSnapshot::serialize(snapshot, *compressed_writer, keeper_context);
        const size_t bytes_written = compressed_writer->count() - bytes_before;
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, bytes_written);

        compressed_writer->finalize();

        if (unowned_file_buffer)
            unowned_file_buffer->finalize();
        WriteBuffer & file_buffer = unowned_file_buffer ? *unowned_file_buffer : *compressed_writer;

        Stopwatch watch;
        file_buffer.sync();
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotFileSyncMicroseconds, watch.elapsedMicroseconds());

        compressed_writer.reset();
        writer.reset();
        removeSnapshotMarker(disk, tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(compressed_writer);
        cancelAndResetWriteBuffer(writer);
        removeFailedSnapshotArtifacts(disk, snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    return makeManagedSnapshotFileInfo(snapshot_file_name, disk, up_to_log_idx);
}

SnapshotFileInfoPtr KeeperSnapshotManager::serializeSnapshotToDisk(const KeeperStorageSnapshot & snapshot)
{
    auto up_to_log_idx = snapshot.snapshot_meta->get_last_log_idx();
    if (auto existing = tryReuseRegisteredSnapshot(up_to_log_idx))
        return existing;

    auto snapshot_file_info = writeSnapshotFile(snapshot);
    return publishAndRunMaintenance(up_to_log_idx, snapshot_file_info);
}

SnapshotFileInfoPtr KeeperSnapshotManager::publishSnapshotFile(uint64_t up_to_log_idx, SnapshotFileInfoPtr file_info)
{
    if (!file_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot publish empty snapshot file info for log index {}", up_to_log_idx);

    auto [it, inserted] = existing_snapshots.emplace(up_to_log_idx, file_info);
    if (inserted)
        return file_info;

    LOG_INFO(
        log,
        "Snapshot with log index {} is already registered at path {} on disk {}, reusing existing metadata",
        up_to_log_idx,
        it->second->path,
        it->second->disk->getName());
    return it->second;
}

void KeeperSnapshotManager::retireUnpublishedSnapshotFile(const SnapshotFileInfoPtr & file_info) const
{
    if (file_info)
        file_info->retired_for_removal.store(true, std::memory_order_release);
}

bool KeeperSnapshotManager::publishMovedSnapshotIfValid(const SnapshotMoveCandidate & candidate)
{
    const auto it = existing_snapshots.find(candidate.log_idx);
    if (it == existing_snapshots.end())
    {
        LOG_DEBUG(log, "Rejecting move publication for snapshot {} because metadata is absent", candidate.log_idx);
        return false;
    }

    if (it->second != candidate.file_info)
    {
        LOG_DEBUG(log, "Rejecting move publication for snapshot {} because metadata was replaced", candidate.log_idx);
        return false;
    }

    /// `disk`/`path` are mutated only under `snapshots_lock`; a concurrent pin release
    /// can only lower `use_count`, i.e. reject a move, never make one unsafe.
    if (candidate.file_info->disk != candidate.source_disk || candidate.file_info->path != candidate.source_path)
    {
        LOG_DEBUG(log, "Rejecting move publication for snapshot {} because source metadata changed", candidate.log_idx);
        return false;
    }

    if (candidate.file_info->retired_for_removal.load(std::memory_order_acquire))
    {
        LOG_DEBUG(log, "Rejecting move publication for retired snapshot {}", candidate.log_idx);
        return false;
    }

    const int64_t count = candidate.file_info.use_count();
    if (count != 2)
    {
        LOG_DEBUG(log, "Rejecting move publication for snapshot {} because it has {} shared references", candidate.log_idx, count);
        return false;
    }

    auto latest_snapshot_idx = getLatestSnapshotIndex();
    DiskPtr current_target_disk = (candidate.log_idx == latest_snapshot_idx) ? getLatestSnapshotDisk() : getDisk();
    if (current_target_disk != candidate.target_disk)
    {
        LOG_DEBUG(log, "Rejecting move publication for snapshot {} because target disk role changed", candidate.log_idx);
        return false;
    }

    candidate.file_info->disk = candidate.target_disk;
    candidate.file_info->path = candidate.target_path;
    return true;
}

void KeeperSnapshotManager::cleanupCopiedMoveTarget(const SnapshotMoveCandidate & candidate) const
{
    try
    {
        candidate.target_disk->removeFileIfExists(candidate.target_path);
        LOG_DEBUG(log, "Removed rejected copied snapshot target {} from disk {}", candidate.target_path, candidate.target_disk->getName());
    }
    catch (...)
    {
        tryLogCurrentException(
            log,
            fmt::format(
                "Failed to remove rejected copied snapshot target {} from disk {}",
                candidate.target_path,
                candidate.target_disk->getName()));
    }
}

bool KeeperSnapshotManager::moveSnapshotCandidate(
    const SnapshotMoveCandidate & candidate,
    const std::function<bool(const SnapshotMoveCandidate &)> & publish_moved_snapshot)
{
    bool metadata_published = false;
    try
    {
        const auto move_result = moveFileBetweenDisks(
            candidate.source_disk,
            candidate.source_path,
            candidate.target_disk,
            candidate.target_path,
            /// Returning false keeps the source file; the caller owns target cleanup.
            [&] { metadata_published = publish_moved_snapshot(candidate); return metadata_published; },
            log,
            keeper_context);

        /// Callback rejection is reported only after the destination was validated and its marker was removed.
        if (!metadata_published && !move_result && move_result.error() == KeeperMoveError::CallbackRejectedOrThrew)
            cleanupCopiedMoveTarget(candidate);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to move snapshot {}", candidate.log_idx));
    }
    return metadata_published;
}

void KeeperSnapshotManager::runMaintenanceInline(uint64_t just_written_log_idx)
{
    /// Best-effort: swallow all exceptions so a failure here never unregisters the just-written snapshot.
    try
    {
        SnapshotMaintenanceTasks tasks = prepareSnapshotMaintenanceTasks(just_written_log_idx);
        tasks.retired_snapshots.clear();
        for (const auto & candidate : tasks.move_candidates)
            moveSnapshotCandidate(candidate, [this](const SnapshotMoveCandidate & move_candidate)
            {
                return publishMovedSnapshotIfValid(move_candidate);
            });
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup and/or move older snapshots");
    }
}

size_t KeeperSnapshotManager::getLatestSnapshotIndex() const
{
    if (!existing_snapshots.empty())
        return existing_snapshots.rbegin()->first;
    return 0;
}

SnapshotFileInfoPtr KeeperSnapshotManager::getLatestSnapshotInfo() const
{
    if (existing_snapshots.empty())
        return nullptr;
    auto it = existing_snapshots.find(getLatestSnapshotIndex());
    if (it == existing_snapshots.end())
        return nullptr;

    /// Return the map entry so callers pin the same file and deleter state.
    try
    {
        if (it->second->disk->existsFile(it->second->path))
            return it->second;
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
    return nullptr;
}

std::map<uint64_t, SnapshotFileInfoPtr> KeeperSnapshotManager::getExistingSnapshots(const std::lock_guard<std::mutex> & /*snapshots_lock*/) const
{
    return existing_snapshots;
}

SnapshotFileInfoPtr KeeperSnapshotManager::getSnapshotPin(uint64_t log_idx) const
{
    auto it = existing_snapshots.find(log_idx);
    if (it == existing_snapshots.end())
        return nullptr;
    return it->second;
}

KeeperSnapshotReader::KeeperSnapshotReader(std::unique_ptr<ReadBuffer> in_, KeeperContextPtr keeper_context_)
    : keeper_context(keeper_context_), in(std::move(in_)) {}

void KeeperSnapshotReader::readMetadata()
{
    uint8_t version = 0;
    readBinary(version, *in);
    if (version > static_cast<uint8_t>(MAX_SUPPORTED_SNAPSHOT_VERSION))
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);
    current_version = static_cast<SnapshotVersion>(version);

    snapshot_meta = deserializeSnapshotMetadata(*in);

    if (version >= SnapshotVersion::V5)
    {
        readBinary(commit_zxid, *in);
        uint8_t digest_version = 0;
        readBinary(digest_version, *in);
        if (digest_version != static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST))
        {
            readBinary(nodes_digest, *in);
            if (digest_version != static_cast<uint8_t>(KEEPER_CURRENT_DIGEST_VERSION))
                nodes_digest = 0;
        }
    }
    else
    {
        commit_zxid = snapshot_meta->get_last_log_idx();
        old_snapshot_zxid = commit_zxid;
    }

    readBinary(session_id_counter, *in);
}

void KeeperSnapshotReader::readACLMapAndNodeCount()
{
    /// Before V1 we serialized ACL without acl_map
    if (current_version >= SnapshotVersion::V1)
    {
        size_t acls_map_size = 0;

        readBinary(acls_map_size, *in);
        size_t current_map_size = 0;
        while (current_map_size < acls_map_size)
        {
            ACLId acl_id = 0;
            if (current_version >= SnapshotVersion::V7)
            {
                readBinary(acl_id, *in);
            }
            else
            {
                /// V1-V6 stored acl_id as uint64_t (8 bytes)
                uint64_t acl_id_64 = 0;
                readBinary(acl_id_64, *in);
                if (acl_id_64 > std::numeric_limits<ACLId>::max())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "ACL ID in snapshot is too large: {}", acl_id_64);
                acl_id = static_cast<ACLId>(acl_id_64);
            }

            size_t acls_size = 0;
            readBinary(acls_size, *in);
            Coordination::ACLs acls;
            for (size_t i = 0; i < acls_size; ++i)
            {
                Coordination::ACL acl;
                readBinary(acl.permissions, *in);
                readBinary(acl.scheme, *in);
                readBinary(acl.id, *in);
                acls.push_back(acl);
            }

            if (!keeper_context->shouldBlockACL())
                acl_map.addMapping(acl_id, acls);
            current_map_size++;
        }
    }

    readBinary(node_count, *in);
}

std::vector<std::unique_ptr<KeeperSnapshotReader::Stream>> KeeperSnapshotReader::createStreams(size_t n)
{
    /// TODO: Chunked snapshots that can be read from multiple threads.
    chassert(n == 1);
    std::vector<std::unique_ptr<Stream>> streams;
    streams.push_back(std::unique_ptr<Stream>(new Stream(*this)));
    return streams;
}

bool KeeperSnapshotReader::Stream::readNodePathSize(size_t & out_path_size)
{
    if (nodes_read >= parent.node_count)
        return false;

    ++nodes_read;
    readVarUInt(out_path_size, *in);
    if (out_path_size == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Snapshot contains an empty node path");
    return true;
}

void KeeperSnapshotReader::Stream::readNodePathAndDataSize(char * out_path, size_t path_size, size_t & out_data_size)
{
    in->readStrict(out_path, path_size);
    readVarUInt(out_data_size, *in);

    if (out_data_size > static_cast<size_t>(INT32_MAX))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Node data size in snapshot is too big: {}", out_data_size);
}

void KeeperSnapshotReader::Stream::readNodeDataAndStats(std::string_view path, char * out_data, size_t data_size, KeeperNodeStats & out_stats)
{
    bool cleanup_acl = parent.keeper_context->shouldBlockACL();
    SnapshotVersion version = parent.current_version;

    in->readStrict(out_data, data_size);

    bool add_usage = true;
    if (version >= SnapshotVersion::V7)
    {
        readBinary(out_stats.acl_id, *in);
    }
    else if (version >= SnapshotVersion::V1)
    {
        /// V1-V6 stored acl_id as uint64_t
        uint64_t acl_id_64 = 0;
        readBinary(acl_id_64, *in);

        /// Some strange ACL ID during deserialization from ZooKeeper
        if (acl_id_64 == std::numeric_limits<uint64_t>::max())
            acl_id_64 = 0;

        if (acl_id_64 > std::numeric_limits<ACLId>::max())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "ACL ID in snapshot is too large: {}", acl_id_64);
        out_stats.acl_id = static_cast<ACLId>(acl_id_64);
    }
    else if (version == SnapshotVersion::V0)
    {
        /// Deserialize ACL
        size_t acls_size = 0;
        readBinary(acls_size, *in);
        Coordination::ACLs acls;
        for (size_t i = 0; i < acls_size; ++i)
        {
            Coordination::ACL acl;
            readBinary(acl.permissions, *in);
            readBinary(acl.scheme, *in);
            readBinary(acl.id, *in);
            acls.push_back(acl);
        }

        add_usage = false; // convertACLs increments usage counter
        if (!cleanup_acl)
            out_stats.acl_id = parent.acl_map.convertACLs(acls);
    }

    if (cleanup_acl)
        out_stats.acl_id = 0;
    else if (add_usage)
        parent.acl_map.addUsage(out_stats.acl_id);

    if (version < SnapshotVersion::V6)
    {
        bool is_sequential = false;
        readBinary(is_sequential, *in);
    }

    /// Deserialize stat
    /// (Reset flags and children count first, in case the caller reuses `out_stats` across nodes.)
    out_stats.ctime_and_flags = 0;
    out_stats.num_children = 0;
    out_stats.data_size = static_cast<uint32_t>(data_size);
    readBinary(out_stats.czxid, *in);
    readBinary(out_stats.mzxid, *in);
    int64_t ctime = 0;
    readBinary(ctime, *in);
    out_stats.setCTime(ctime);
    readBinary(out_stats.mtime, *in);
    readBinary(out_stats.version, *in);
    readBinary(out_stats.cversion, *in);
    readBinary(out_stats.aversion, *in);
    int64_t ephemeral_owner = 0;
    readBinary(ephemeral_owner, *in);

    if (version < SnapshotVersion::V6)
    {
        int32_t data_length = 0;
        readBinary(data_length, *in);
    }
    int32_t num_children = 0;
    readBinary(num_children, *in);

    if (ephemeral_owner == KeeperNodeStats::CONTAINER_EPHEMERAL_OWNER)
        out_stats.makeContainer();
    else if (ephemeral_owner != 0)
        out_stats.makeEphemeral(ephemeral_owner);

    readBinary(out_stats.pzxid, *in);

    if (version >= SnapshotVersion::V7)
    {
        int64_t seq_num = 0;
        readBinary(seq_num, *in);
        if (!out_stats.isEphemeral())
            out_stats.setSeqNum(seq_num);
    }
    else
    {
        int32_t seq_num = 0;
        readBinary(seq_num, *in);
        if (!out_stats.isEphemeral())
            out_stats.setSeqNum(seq_num);
    }

    if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
    {
        uint64_t size_bytes = 0;
        readBinary(size_bytes, *in);
    }

    if (version >= SnapshotVersion::V8)
    {
        bool has_ttl = false;
        readBinary(has_ttl, *in);
        if (has_ttl)
        {
            int64_t ttl_ms = 0;
            readBinary(ttl_ms, *in);
            out_stats.makeTTL(ttl_ms);
        }
    }

    if (!out_stats.isEphemeral() && !out_stats.isTTL())
        out_stats.setNumChildren(num_children);

    /// Refuse to load system nodes from snapshot.
    auto match_result = Coordination::matchPath(path, keeper_system_path);
    if (match_result == Coordination::PathMatchResult::IS_CHILD)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Snapshot contains system node: {}", path);
    if (match_result == Coordination::PathMatchResult::EXACT &&
        (out_stats.data_size != 0 || out_stats.mzxid != 0))
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Snapshot contains system root node {} with unexpected data ({} bytes) or stats (mzxid={})",
            path, out_stats.data_size, out_stats.mzxid);
}

void KeeperSnapshotReader::finishStreams(std::vector<std::unique_ptr<Stream>> /*streams*/)
{
    /// The snapshot's ACL map may contain ACLs that are not referenced by any node, e.g. ACLs
    /// that were referenced only by uncommitted nodes.
    acl_map.removeUnusedACLs();
}

void KeeperSnapshotReader::readSessionsAndClusterConfig(KeeperStorage & storage)
{
    size_t active_sessions_size = 0;
    readBinary(active_sessions_size, *in);

    size_t current_session_size = 0;
    while (current_session_size < active_sessions_size)
    {
        int64_t active_session_id = 0;
        int64_t timeout = 0;
        readBinary(active_session_id, *in);
        readBinary(timeout, *in);
        storage.addSessionID(active_session_id, timeout);

        if (current_version >= SnapshotVersion::V1)
        {
            size_t session_auths_size = 0;
            readBinary(session_auths_size, *in);

            typename KeeperStorage::AuthIDs ids;
            size_t session_auth_counter = 0;
            while (session_auth_counter < session_auths_size)
            {
                String scheme;
                String id;
                readBinary(scheme, *in);
                readBinary(id, *in);
                ids.emplace_back(typename KeeperStorage::AuthID{scheme, id});

                session_auth_counter++;
            }
            if (!ids.empty())
                storage.committed_session_and_auth[active_session_id] = ids;
        }
        current_session_size++;
    }

    /// Optional cluster config
    if (!in->eof())
    {
        size_t data_size = 0;
        readVarUInt(data_size, *in);
        auto buffer = nuraft::buffer::alloc(data_size);
        in->readStrict(reinterpret_cast<char *>(buffer->data_begin()), data_size);
        buffer->pos(0);
        cluster_config = ClusterConfig::deserialize(*buffer);
    }
}

}
