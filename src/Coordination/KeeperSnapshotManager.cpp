#include <filesystem>
#include <memory>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Core/Field.h>
#include <Disks/IDisk.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <base/sort.h>
#include <base/scope_guard.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event KeeperSnapshotWrittenBytes;
    extern const Event KeeperSnapshotFileSyncMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int KEEPER_EXCEPTION;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNKNOWN_SNAPSHOT;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void moveSnapshotBetweenDisks(
        DiskPtr disk_from,
        const std::string & path_from,
        DiskPtr disk_to,
        const std::string & path_to,
        const KeeperContextPtr & keeper_context)
    {
        moveFileBetweenDisks(
            std::move(disk_from),
            path_from,
            std::move(disk_to),
            path_to,
            /*before_file_remove_op=*/{},
            getLogger("KeeperSnapshotManager"),
            keeper_context);
    }

    uint64_t getSnapshotPathUpToLogIdx(const String & snapshot_path)
    {
        std::filesystem::path path(snapshot_path);
        std::string filename = path.stem();
        std::vector<std::string_view> name_parts;
        splitInto<'_', '.'>(name_parts, filename);
        return parse<uint64_t>(name_parts[1]);
    }

    std::string getSnapshotFileName(uint64_t up_to_log_idx, bool compress_zstd)
    {
        auto base = fmt::format("snapshot_{}.bin", up_to_log_idx);
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

    template<typename Node>
    void writeNode(const Node & node, SnapshotVersion version, WriteBuffer & out)
    {
        writeBinary(node.getData(), out);

        /// Serialize ACL
        if (version >= SnapshotVersion::V7)
            writeBinary(node.stats.acl_id, out);
        else
            writeBinary(static_cast<uint64_t>(node.stats.acl_id), out);
        /// Write is_sequential for backwards compatibility
        if (version < SnapshotVersion::V6)
            writeBinary(false, out);

        /// Serialize stat
        writeBinary(node.stats.czxid, out);
        writeBinary(node.stats.mzxid, out);
        writeBinary(node.stats.ctime, out);
        writeBinary(node.stats.mtime, out);
        writeBinary(node.stats.version, out);
        writeBinary(node.stats.cversion, out);
        writeBinary(node.stats.aversion, out);
        writeBinary(node.stats.getEphemeralOwner(), out);
        if (version < SnapshotVersion::V6)
            writeBinary(static_cast<int32_t>(node.stats.data_size), out);
        writeBinary(node.stats.getNumChildren(), out);
        writeBinary(node.stats.pzxid, out);

        if (version >= SnapshotVersion::V7)
            writeBinary(node.stats.getSeqNum(), out);
        else
        {
            auto seq_num = node.stats.getSeqNum();
            if (seq_num < std::numeric_limits<int32_t>::min() || seq_num > std::numeric_limits<int32_t>::max())
                throw Exception(ErrorCodes::KEEPER_EXCEPTION,
                    "Sequential node counter {} overflows int32, upgrade to snapshot version >= V7", seq_num);
            writeBinary(static_cast<int32_t>(seq_num), out);
        }

        if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
            writeBinary(node.sizeInBytes(), out);
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
    writeBinary(snapshot.snapshot_container_size - keeper_context->getSystemNodesWithData().size(), out);
    size_t counter = 0;
    for (auto it = snapshot.begin; counter < snapshot.snapshot_container_size; ++counter)
    {
        const auto & path = it->key;

        // write only the root system path because of digest
        if (Coordination::matchPath(path, keeper_system_path) == Coordination::PathMatchResult::IS_CHILD)
        {
            if (counter == snapshot.snapshot_container_size - 1)
                break;

            ++it;
            continue;
        }

        const auto & node = it->value;

        /// (This is guaranteed because KeeperStorageSnapshot constructor is called with nuraft's
        ///  commit_lock_ held, and therefore storage can't change between when we get storage->zxid
        ///  and when we call storage->enableSnapshotMode().)
        if (node.stats.mzxid > snapshot.zxid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to serialize node with mzxid {}, but last snapshot index {}", node.stats.mzxid, snapshot.zxid);

        writeBinary(path, out);
        writeNode(node, snapshot.version, out);

        /// Last iteration: check and exit here without iterator increment. Otherwise
        /// false positive race condition on list end is possible.
        if (counter == snapshot.snapshot_container_size - 1)
            break;

        ++it;
    }

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
    : storage(storage_)
    , version(version_)
    , snapshot_meta(std::make_shared<SnapshotMetadata>(up_to_log_idx_, 0, std::make_shared<nuraft::cluster_config>()))
    , session_id(storage->session_id_counter)
    , cluster_config(cluster_config_)
    , zxid(storage->zxid)
    , nodes_digest(storage->nodes_digest)
{
    auto [size, ver] = storage->container.snapshotSizeWithVersion();
    snapshot_container_size = size;
    storage->enableSnapshotMode(ver);
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
}

KeeperStorageSnapshot::KeeperStorageSnapshot(
    KeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_)
    : storage(storage_)
    , version(version_)
    , snapshot_meta(snapshot_meta_)
    , session_id(storage->session_id_counter)
    , cluster_config(cluster_config_)
    , zxid(storage->zxid)
    , nodes_digest(storage->nodes_digest)
{
    auto [size, ver] = storage->container.snapshotSizeWithVersion();
    snapshot_container_size = size;
    storage->enableSnapshotMode(ver);
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
}

KeeperStorageSnapshot::~KeeperStorageSnapshot()
{
    storage->disableSnapshotMode();
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
                /// Unlink only snapshots explicitly retired by `removeSnapshot`
                /// or corruption recovery. Manager destruction keeps files.
                if (p->retired_for_removal.load(std::memory_order_acquire))
                {
                    p->disk->removeFileIfExists(p->path);
                    LOG_DEBUG(logger, "Removed outdated snapshot {} at path {}", log_idx, p->path);
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
    bool compress_snapshots_zstd_)
    : snapshots_to_keep(snapshots_to_keep_)
    , compress_snapshots_zstd(compress_snapshots_zstd_)
    , keeper_context(keeper_context_)
{
    std::unordered_set<DiskPtr> read_disks;
    const auto load_snapshot_from_disk = [&](const auto & disk)
    {
        if (read_disks.contains(disk))
            return;

        LOG_TRACE(log, "Reading from disk {}", disk->getName());
        std::unordered_map<std::string, std::string> incomplete_files;

        const auto clean_incomplete_file = [&](const auto & file_path)
        {
            if (auto incomplete_it = incomplete_files.find(fs::path(file_path).filename()); incomplete_it != incomplete_files.end())
            {
                LOG_TRACE(log, "Removing {} from {}", file_path, disk->getName());
                disk->removeFile(file_path);
                disk->removeFile(incomplete_it->second);
                incomplete_files.erase(incomplete_it);
                return true;
            }

            return false;
        };

        std::vector<std::string> snapshot_files;
        for (auto it = disk->iterateDirectory(""); it->isValid(); it->next())
        {
            if (it->name().starts_with(tmp_keeper_file_prefix))
            {
                incomplete_files.emplace(it->name().substr(tmp_keeper_file_prefix.size()), it->path());
                continue;
            }

            if (it->name().starts_with("snapshot_") && !clean_incomplete_file(it->path()))
                snapshot_files.push_back(it->path());
        }

        for (const auto & snapshot_file : snapshot_files)
        {
            if (clean_incomplete_file(fs::path(snapshot_file).filename()))
                continue;

            LOG_TRACE(log, "Found {} on {}", snapshot_file, disk->getName());
            size_t snapshot_up_to = getSnapshotPathUpToLogIdx(snapshot_file);
            auto [_, inserted] = existing_snapshots.insert_or_assign(snapshot_up_to,
                makeManagedSnapshotFileInfo(snapshot_file, disk, snapshot_up_to));

            if (!inserted)
                LOG_WARNING(
                    log,
                    "Found another snapshots with last log idx {}, will use snapshot from disk {}",
                    snapshot_up_to,
                    disk->getName());
        }

        for (const auto & [name, path] : incomplete_files)
            disk->removeFile(path);

        if (snapshot_files.empty())
            LOG_TRACE(log, "No snapshots were found on {}", disk->getName());

        read_disks.insert(disk);
    };

    for (const auto & disk : keeper_context->getOldSnapshotDisks())
        load_snapshot_from_disk(disk);

    auto disk = getDisk();
    load_snapshot_from_disk(disk);

    auto latest_snapshot_disk = getLatestSnapshotDisk();
    if (latest_snapshot_disk != disk)
        load_snapshot_from_disk(latest_snapshot_disk);

    removeOutdatedSnapshotsIfNeeded(/*just_written_log_idx=*/0);
    moveSnapshotsIfNeeded();
}

SnapshotFileInfoPtr KeeperSnapshotManager::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx)
{
    ReadBufferFromNuraftBuffer reader(buffer);

    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);
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
        disk->removeFile(tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(plain_buf);
        removeFailedSnapshotArtifacts(disk, snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    auto snapshot_file_info = registerSnapshotFile(up_to_log_idx, makeManagedSnapshotFileInfo(snapshot_file_name, disk, up_to_log_idx));
    try
    {
        removeOutdatedSnapshotsIfNeeded(up_to_log_idx);
        moveSnapshotsIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup and/or move older snapshots");
    }

    return snapshot_file_info;
}

std::unique_ptr<SnapshotReceiveCtx> KeeperSnapshotManager::beginSnapshotReceiveToDisk(uint64_t up_to_log_idx)
{
    auto disk = getLatestSnapshotDisk();
    LOG_DEBUG(log, "Receiving snapshot {} to {} disk", up_to_log_idx, isLocalDisk(*disk) ? "local" : "remote");
    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);

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
        ctx.disk->removeFile(tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(ctx.write_buf);
        removeFailedSnapshotArtifacts(ctx.disk, ctx.snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    auto snapshot_file_info = registerSnapshotFile(ctx.log_idx, makeManagedSnapshotFileInfo(ctx.snapshot_file_name, ctx.disk, ctx.log_idx));
    try
    {
        removeOutdatedSnapshotsIfNeeded(ctx.log_idx);
        moveSnapshotsIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup and/or move older snapshots");
    }

    return snapshot_file_info;
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
        compressed_writer = wrapWriteBufferWithCompressionMethod(std::move(writer), CompressionMethod::Zstd, 3);
    else
        compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer);

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
    reader->readMetadata();
    reader->readACLMapAndNodeCount();
    storage.loadNodesFromSnapshot(*reader);

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

SnapshotFileInfoPtr KeeperSnapshotManager::registerSnapshotFile(uint64_t log_idx, const SnapshotFileInfoPtr & snapshot_file_info)
{
    auto [it, inserted] = existing_snapshots.try_emplace(log_idx, snapshot_file_info);
    if (inserted)
        return it->second;

    if (it->second->disk == snapshot_file_info->disk && it->second->path == snapshot_file_info->path)
        return it->second; /// In-place overwrite: keep the canonical entry the map already tracks.

    LOG_WARNING(
        log,
        "Snapshot with last log idx {} was already registered at {} (disk {}); replacing the registry entry "
        "with the just-written file at {} (disk {}) and retiring the old one",
        log_idx, it->second->path, it->second->disk->getName(),
        snapshot_file_info->path, snapshot_file_info->disk->getName());
    /// Different (disk, path), and the managed deleter unlinks only after the last pin releases.
    it->second->retired_for_removal.store(true, std::memory_order_release);
    it->second = snapshot_file_info;
    return it->second;
}

void KeeperSnapshotManager::removeOutdatedSnapshotsIfNeeded(uint64_t just_written_log_idx)
{
    /// Keep the `snapshots_to_keep` newest snapshots, plus the protected (mark-backing) entry,
    /// the pending-install entry, and the just-written entry. Worst-case: snapshots_to_keep + 3.
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
        removeSnapshot(to_remove->first);
    }
}

void KeeperSnapshotManager::moveSnapshotsIfNeeded()
{
    /// Move snapshots to their configured disks when no outside holder pins them.
    auto disk = getDisk();
    auto latest_snapshot_disk = getLatestSnapshotDisk();
    auto latest_snapshot_idx = getLatestSnapshotIndex();

    for (auto & [idx, file_info] : existing_snapshots)
    {
        DiskPtr target_disk = (idx == latest_snapshot_idx) ? latest_snapshot_disk : disk;

        if (file_info->disk == target_disk)
            continue;

        /// `use_count > 1` means a transfer, S3 upload, or caller still
        /// holds the file. Retry the move on the next snapshot-manager update.
        const int64_t count = file_info.use_count();
        if (count > 1)
        {
            LOG_DEBUG(log,
                "Deferring move of snapshot {} - has {} outside references",
                idx, count - 1);
            continue;
        }

        moveSnapshotBetweenDisks(file_info->disk, file_info->path, target_disk, file_info->path, keeper_context);
        file_info->disk = target_disk;
    }
}

void KeeperSnapshotManager::removeSnapshot(uint64_t log_idx)
{
    auto itr = existing_snapshots.find(log_idx);
    if (itr == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);

    /// Mark before erasing so the deleter unlinks after the last pin is released.
    itr->second->retired_for_removal.store(true, std::memory_order_release);
    existing_snapshots.erase(itr);
}

SnapshotFileInfoPtr KeeperSnapshotManager::serializeSnapshotToDisk(const KeeperStorageSnapshot & snapshot)
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
        if (compress_snapshots_zstd)
            compressed_writer = wrapWriteBufferWithCompressionMethod(std::move(writer), CompressionMethod::Zstd, 3);
        else
            compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer);

        const size_t bytes_before = compressed_writer->count();
        KeeperStorageSnapshot::serialize(snapshot, *compressed_writer, keeper_context);
        const size_t bytes_written = compressed_writer->count() - bytes_before;
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, bytes_written);

        compressed_writer->finalize();

        Stopwatch watch;
        compressed_writer->sync();
        ProfileEvents::increment(ProfileEvents::KeeperSnapshotFileSyncMicroseconds, watch.elapsedMicroseconds());

        compressed_writer.reset();
        writer.reset();
        disk->removeFile(tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(compressed_writer);
        cancelAndResetWriteBuffer(writer);
        removeFailedSnapshotArtifacts(disk, snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    auto snapshot_file_info = registerSnapshotFile(up_to_log_idx, makeManagedSnapshotFileInfo(snapshot_file_name, disk, up_to_log_idx));

    try
    {
        removeOutdatedSnapshotsIfNeeded(up_to_log_idx);
        moveSnapshotsIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup and/or move older snapshots");
    }

    return snapshot_file_info;
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
                chassert(acl_id_64 <= std::numeric_limits<ACLId>::max());
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
    chassert(out_path_size != 0);
    return true;
}

void KeeperSnapshotReader::Stream::readNodePathAndDataSize(char * out_path, size_t path_size, size_t & out_data_size)
{
    in->readStrict(out_path, path_size);
    readVarUInt(out_data_size, *in);
}

void KeeperSnapshotReader::Stream::readNodeDataAndStats(char * out_data, size_t data_size, KeeperNodeStats & out_stats)
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

        chassert(acl_id_64 <= std::numeric_limits<ACLId>::max());
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
    readBinary(out_stats.czxid, *in);
    readBinary(out_stats.mzxid, *in);
    readBinary(out_stats.ctime, *in);
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

    if (ephemeral_owner == 0)
        out_stats.setNumChildrenAndIsEphemeral(num_children, false);
    else
        out_stats.setEphemeralOwner(ephemeral_owner);

    readBinary(out_stats.pzxid, *in);

    if (version >= SnapshotVersion::V7)
    {
        int64_t seq_num = 0;
        readBinary(seq_num, *in);
        if (ephemeral_owner == 0)
            out_stats.setSeqNum(seq_num);
    }
    else
    {
        int32_t seq_num = 0;
        readBinary(seq_num, *in);
        if (ephemeral_owner == 0)
            out_stats.setSeqNum(seq_num);
    }

    if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
    {
        uint64_t size_bytes = 0;
        readBinary(size_bytes, *in);
    }
}

KeeperSnapshotReader::Stream::WhatToDoWithNode KeeperSnapshotReader::Stream::checkIfSystemNode(std::string_view path, const KeeperNodeStats & stats)
{
    using enum Coordination::PathMatchResult;
    auto match_result = Coordination::matchPath(path, keeper_system_path);

    const auto get_error_msg = [&]
    {
        return fmt::format("Cannot read node on path {} from a snapshot because it is used as a system node", path);
    };

    if (match_result == IS_CHILD)
    {
        if (parent.keeper_context->ignoreSystemPathOnStartup() || parent.keeper_context->getServerState() != KeeperContext::Phase::INIT)
        {
            LOG_ERROR(getLogger("KeeperSnapshotManager"), "{}. Ignoring it", get_error_msg());
            return WhatToDoWithNode::Skip;
        }
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{}. Ignoring it can lead to data loss. "
            "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true",
            get_error_msg());
    }
    if (match_result == EXACT)
    {
        if (stats.data_size != 0 || stats.mzxid != 0)
        {
            if (parent.keeper_context->ignoreSystemPathOnStartup() || parent.keeper_context->getServerState() != KeeperContext::Phase::INIT)
            {
                LOG_ERROR(getLogger("KeeperSnapshotManager"), "{}. Ignoring it", get_error_msg());
                return WhatToDoWithNode::Clear;
            }
            else
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "{}. Ignoring it can lead to data loss. "
                    "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true",
                    get_error_msg());
        }
    }

    return WhatToDoWithNode::ProcessNormally;
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
