#include <filesystem>
#include <memory>
#include <optional>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperChunkedSnapshot.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Core/Field.h>
#include <Common/thread_local_rng.h>
#include <Disks/IDisk.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ZstdDeflatingWriteBuffer.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <base/sort.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/SharedLockGuard.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event KeeperSnapshotWrittenBytes;
    extern const Event KeeperSnapshotFileSyncMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric KeeperSnapshotDeserThreads;
    extern const Metric KeeperSnapshotDeserThreadsActive;
    extern const Metric KeeperSnapshotDeserThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int KEEPER_EXCEPTION;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNKNOWN_SNAPSHOT;
    extern const int LOGICAL_ERROR;
}

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 rocksdb_load_batch_size;
    extern const CoordinationSettingsUInt64 snapshot_chunk_size;
    extern const CoordinationSettingsUInt64 snapshot_deser_threads;
}


namespace
{
    /// Create a ZSTD streaming reader for a single chunked snapshot ZSTD frame.
    /// Sets require_frame_complete=true to verify that ZSTD_decompressStream reaches ret==0
    /// (i.e. the full frame including the 4-byte content-checksum epilogue is consumed).
    /// This restores the integrity guarantee that the superseded one-shot ZSTD_decompress
    /// path provided: a corrupt or truncated frame is detected rather than accepted.
    std::unique_ptr<ReadBuffer> makeChunkReader(std::unique_ptr<ReadBufferFromMemory> read_buffer)
    {
        return std::make_unique<ZstdInflatingReadBuffer>(
            std::move(read_buffer),
            DBMS_DEFAULT_BUFFER_SIZE,
            /*existing_memory=*/nullptr,
            /*alignment=*/0,
            /*zstd_window_log_max=*/0,
            /*require_frame_complete=*/true);
    }

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

    template<typename Node>
    void writeNode(const Node & node, SnapshotVersion version, WriteBuffer & out)
    {
        writeBinary(node.getData(), out);

        /// Serialize ACL
        if (version >= SnapshotVersion::V7)
            writeBinary(node.acl_id, out);
        else
            writeBinary(static_cast<uint64_t>(node.acl_id), out);
        /// Write is_sequential for backwards compatibility
        if (version < SnapshotVersion::V6)
            writeBinary(false, out);

        /// Serialize stat
        writeBinary(node.stats.czxid, out);
        writeBinary(node.stats.mzxid, out);
        writeBinary(node.stats.ctime(), out);
        writeBinary(node.stats.mtime, out);
        writeBinary(node.stats.version, out);
        writeBinary(node.stats.cversion, out);
        writeBinary(node.stats.aversion, out);
        writeBinary(node.stats.ephemeralOwner(), out);
        if (version < SnapshotVersion::V6)
            writeBinary(static_cast<int32_t>(node.stats.data_size), out);
        writeBinary(node.numChildren(), out);
        writeBinary(node.stats.pzxid, out);

        if (version >= SnapshotVersion::V7)
            writeBinary(node.stats.seqNum(), out);
        else
        {
            auto seq_num = node.stats.seqNum();
            if (seq_num < std::numeric_limits<int32_t>::min() || seq_num > std::numeric_limits<int32_t>::max())
                throw Exception(ErrorCodes::KEEPER_EXCEPTION,
                    "Sequential node counter {} overflows int32, upgrade to snapshot version >= V7", seq_num);
            writeBinary(static_cast<int32_t>(seq_num), out);
        }

        if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
            writeBinary(node.sizeInBytes(), out);

        if (version >= SnapshotVersion::V8)
        {
            writeBinary(node.stats.isTTL(), out);
            if (node.stats.isTTL())
                writeBinary(node.stats.ttl(), out);
        }
    }

    /// Writes path+node to `out` if the path is not a system node child.
    /// Returns true if the node was written, false if it was skipped.
    template <typename Node>
    bool serializeSnapshotNode(std::string_view path, const Node & node, SnapshotVersion version, uint64_t zxid, WriteBuffer & out)
    {
        if (Coordination::matchPath(path, keeper_system_path) == Coordination::PathMatchResult::IS_CHILD)
            return false;

        if (static_cast<uint64_t>(node.stats.mzxid) > zxid)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Trying to serialize node with mzxid {}, but last snapshot index {}",
                node.stats.mzxid,
                zxid);

        writeBinary(path, out);
        writeNode(node, version, out);
        return true;
    }

    /// Validates a deserialized node against system-path constraints and prepares it for insertion.
    /// Returns false if the node must be skipped (IS_CHILD of /keeper), true otherwise.
    /// May reset `node` to empty if it is the /keeper root with data (EXACT match).
    template <typename Node>
    bool prepareDeserializedNode(
        Coordination::PathMatchResult match,
        std::string_view path,
        Node & node,
        KeeperContextPtr keeper_context)
    {
        using enum Coordination::PathMatchResult;

        if (match == IS_CHILD)
        {
            if (keeper_context->ignoreSystemPathOnStartup() || keeper_context->getServerState() != KeeperContext::Phase::INIT)
            {
                LOG_ERROR(getLogger("KeeperSnapshotManager"), "System-path child {} found in snapshot — skipping", path);
                return false;
            }
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "System-path child {} found in snapshot. "
                "Set keeper_server.ignore_system_path_on_startup=true to ignore.",
                path);
        }
        if (match == EXACT && !node.empty())
        {
            if (keeper_context->ignoreSystemPathOnStartup() || keeper_context->getServerState() != KeeperContext::Phase::INIT)
            {
                LOG_ERROR(getLogger("KeeperSnapshotManager"), "Non-empty keeper system node {} found in snapshot — clearing data", path);
                node = Node{};
            }
            else
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Non-empty keeper system node {} found in snapshot. "
                    "Set keeper_server.ignore_system_path_on_startup=true to ignore.",
                    path);
        }

        if constexpr (!std::is_same_v<Node, KeeperRocksNode>)
            if (!node.stats.isEphemeral() && node.numChildren() > 0)
                node.getChildren().reserve(node.numChildren());

        return true;
    }

    template<typename Node>
    void readNode(Node & node, ReadBuffer & in, SnapshotVersion version, ACLMap * acl_map, bool cleanup_acl)
    {
        readVarUInt(node.stats.data_size, in);
        if (node.stats.data_size != 0)
        {
            node.data = std::unique_ptr<char[]>(new char[node.stats.data_size]);
            in.readStrict(node.data.get(), node.stats.data_size);
        }

        bool add_usage = true;
        if (version >= SnapshotVersion::V7)
        {
            readBinary(node.acl_id, in);

            if (cleanup_acl)
                node.acl_id = 0;
        }
        else if (version >= SnapshotVersion::V1)
        {
            /// V1-V6 stored acl_id as uint64_t
            uint64_t acl_id_64 = 0;
            readBinary(acl_id_64, in);

            /// Some strange ACL ID during deserialization from ZooKeeper
            if (acl_id_64 == std::numeric_limits<uint64_t>::max())
                acl_id_64 = 0;

            chassert(acl_id_64 <= std::numeric_limits<ACLId>::max());
            node.acl_id = static_cast<ACLId>(acl_id_64);

            if (cleanup_acl)
                node.acl_id = 0;
        }
        else if (version == SnapshotVersion::V0)
        {
            /// Deserialize ACL
            size_t acls_size = 0;
            readBinary(acls_size, in);
            Coordination::ACLs acls;
            for (size_t i = 0; i < acls_size; ++i)
            {
                Coordination::ACL acl;
                readBinary(acl.permissions, in);
                readBinary(acl.scheme, in);
                readBinary(acl.id, in);
                acls.push_back(acl);
            }

            if (!cleanup_acl && acl_map)
            {
                node.acl_id = acl_map->convertACLs(acls);
                add_usage = false;
            }
        }

        if (add_usage && acl_map)
            acl_map->addUsage(node.acl_id);

        if (version < SnapshotVersion::V6)
        {
            bool is_sequential = false;
            readBinary(is_sequential, in);
        }

        /// Deserialize stat
        readBinary(node.stats.czxid, in);
        readBinary(node.stats.mzxid, in);
        int64_t ctime = 0;
        readBinary(ctime, in);
        node.stats.setCtime(ctime);
        readBinary(node.stats.mtime, in);
        readBinary(node.stats.version, in);
        readBinary(node.stats.cversion, in);
        readBinary(node.stats.aversion, in);
        int64_t ephemeral_owner = 0;
        readBinary(ephemeral_owner, in);
        if (ephemeral_owner != 0)
            node.stats.setEphemeralOwner(ephemeral_owner);

        if (version < SnapshotVersion::V6)
        {
            int32_t data_length = 0;
            readBinary(data_length, in);
        }
        int32_t num_children = 0;
        readBinary(num_children, in);
        if (num_children < 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Snapshot: negative num_children {} in node", num_children);
        node.setNumChildren(num_children);

        readBinary(node.stats.pzxid, in);

        if (version >= SnapshotVersion::V7)
        {
            int64_t seq_num = 0;
            readBinary(seq_num, in);
            if (ephemeral_owner == 0)
                node.stats.setSeqNum(seq_num);
        }
        else
        {
            int32_t seq_num = 0;
            readBinary(seq_num, in);
            if (ephemeral_owner == 0)
                node.stats.setSeqNum(seq_num);
        }

        if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
        {
            uint64_t size_bytes = 0;
            readBinary(size_bytes, in);
        }

        if (version >= SnapshotVersion::V8)
        {
            bool has_ttl = false;
            readBinary(has_ttl, in);
            if (has_ttl)
            {
                int64_t ttl_ms = 0;
                readBinary(ttl_ms, in);
                node.stats.setTTL(ttl_ms);
            }
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

    /// Deserialize sessions (with auth) and optional cluster config. Mirrors serializeSessionsAndConfig.
    template <typename Storage>
    void deserializeSessionsAndConfig(Storage & storage, ClusterConfigPtr & cluster_config, ReadBuffer & in, SnapshotVersion version)
    {
        size_t active_sessions_size = 0;
        readBinary(active_sessions_size, in);
        for (size_t i = 0; i < active_sessions_size; ++i)
        {
            int64_t active_session_id = 0;
            int64_t timeout = 0;
            readBinary(active_session_id, in);
            readBinary(timeout, in);
            storage.addSessionID(active_session_id, timeout);
            if (version >= SnapshotVersion::V1)
            {
                size_t session_auths_size = 0;
                readBinary(session_auths_size, in);
                typename Storage::AuthIDs ids;
                for (size_t j = 0; j < session_auths_size; ++j)
                {
                    String scheme;
                    String id;
                    readBinary(scheme, in);
                    readBinary(id, in);
                    ids.emplace_back(typename Storage::AuthID{scheme, id});
                }
                if (!ids.empty())
                    storage.committed_session_and_auth[active_session_id] = ids;
            }
        }

        if (in.eof())
            return;
        size_t data_size = 0;
        readVarUInt(data_size, in);
        auto buf = nuraft::buffer::alloc(data_size);
        in.readStrict(reinterpret_cast<char *>(buf->data_begin()), data_size);
        buf->pos(0);
        cluster_config = ClusterConfig::deserialize(*buf);
    }

    template <typename Storage>
    void deserializeStorageMetaFields(
        Storage & storage,
        const SnapshotMetadataPtr & snapshot_meta,
        ReadBuffer & in,
        SnapshotVersion version,
        bool & recalculate_digest,
        KeeperContextPtr keeper_context) TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        if (version >= SnapshotVersion::V5)
        {
            int64_t zxid = 0;
            readBinary(zxid, in);
            {
                std::lock_guard lock(storage.transaction_mutex);
                storage.zxid = zxid;
            }

            uint8_t digest_version = 0;
            readBinary(digest_version, in);
            if (digest_version != static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST))
            {
                uint64_t nodes_digest = 0;
                readBinary(nodes_digest, in);
                if (digest_version == static_cast<uint8_t>(KEEPER_CURRENT_DIGEST_VERSION))
                {
                    storage.nodes_digest = nodes_digest;
                    recalculate_digest = false;
                }
            }

            storage.old_snapshot_zxid = 0;
        }
        else
        {
            storage.zxid = snapshot_meta->get_last_log_idx();
            storage.old_snapshot_zxid = storage.zxid;
        }

        int64_t session_id = 0;
        readBinary(session_id, in);
        storage.session_id_counter = session_id;

        if (version >= SnapshotVersion::V1)
        {
            size_t acl_map_size = 0;
            readBinary(acl_map_size, in);
            for (size_t i = 0; i < acl_map_size; ++i)
            {
                ACLId acl_id = 0;
                if (version >= SnapshotVersion::V7)
                {
                    readBinary(acl_id, in);
                }
                else
                {
                    uint64_t acl_id_64 = 0;
                    readBinary(acl_id_64, in);
                    chassert(acl_id_64 <= std::numeric_limits<ACLId>::max());
                    acl_id = static_cast<ACLId>(acl_id_64);
                }
                size_t acls_size = 0;
                readBinary(acls_size, in);
                Coordination::ACLs acls;
                for (size_t j = 0; j < acls_size; ++j)
                {
                    Coordination::ACL acl;
                    readBinary(acl.permissions, in);
                    readBinary(acl.scheme, in);
                    readBinary(acl.id, in);
                    acls.push_back(acl);
                }
                if (!keeper_context->shouldBlockACL())
                    storage.acl_map.addMapping(acl_id, acls);
            }
        }
    }

    template <typename Storage>
    void serializeStorageMetaFields(
        const KeeperStorageSnapshot<Storage> & snapshot,
        WriteBuffer & out,
        SnapshotVersion version,
        KeeperContextPtr keeper_context)
    {
        if (version >= SnapshotVersion::V5)
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

        std::vector<std::pair<ACLId, Coordination::ACLs>> sorted_acl_map(snapshot.acl_map.begin(), snapshot.acl_map.end());
        ::sort(sorted_acl_map.begin(), sorted_acl_map.end());
        writeBinary(sorted_acl_map.size(), out);
        for (const auto & [acl_id, acls] : sorted_acl_map)
        {
            if (version >= SnapshotVersion::V7)
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
    }

    template <typename Storage>
    void serializeSessionsAndConfig(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & out)
    {
        std::vector<std::pair<int64_t, int64_t>> sorted_sessions(
            snapshot.session_and_timeout.begin(), snapshot.session_and_timeout.end());
        ::sort(sorted_sessions.begin(), sorted_sessions.end());
        writeBinary(sorted_sessions.size(), out);
        for (const auto & [session_id, timeout] : sorted_sessions)
        {
            writeBinary(session_id, out);
            writeBinary(timeout, out);
            KeeperStorageBase::AuthIDs ids;
            if (snapshot.session_and_auth.contains(session_id))
                ids = snapshot.session_and_auth.at(session_id);
            writeBinary(ids.size(), out);
            for (const auto & [scheme, id] : ids)
            {
                writeBinary(scheme, out);
                writeBinary(id, out);
            }
        }

        if (snapshot.cluster_config)
        {
            auto buf = snapshot.cluster_config->serialize();
            writeVarUInt(buf->size(), out);
            out.write(reinterpret_cast<const char *>(buf->data_begin()), buf->size());
        }
    }

    /// Serialize a chunked snapshot: FRONT HEADER | METADATA chunk | K×NODES chunks | FOOTER.
    template <typename Storage>
    std::vector<SnapshotChunkDescriptor>
    serializeChunkedSnapshot(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & raw_out, KeeperContextPtr keeper_context)
    {
        const size_t system_nodes = keeper_context->getSystemNodesWithData().size();
        const size_t total_nodes = snapshot.snapshot_container_size > system_nodes ? snapshot.snapshot_container_size - system_nodes : 0;

        const size_t chunk_size_limit = std::max(
            static_cast<size_t>(1),
            static_cast<size_t>(keeper_context->getCoordinationSettings()[CoordinationSetting::snapshot_chunk_size]));

        // At least 1 NODES chunk even for empty storage.
        const size_t nodes_chunk_count = total_nodes == 0 ? 1 : (total_nodes + chunk_size_limit - 1) / chunk_size_limit;
        const uint64_t total_chunk_count = 1 + nodes_chunk_count; // METADATA + N*NODES

        std::vector<SnapshotChunkDescriptor> chunks;
        chunks.reserve(total_chunk_count);

        packChunkedSnapshotHeader(total_chunk_count, raw_out);

        // ZSTD-compress write_body's output, append to raw_out, record the descriptor.
        auto write_chunk = [&](SnapshotChunkType type, auto && write_body)
        {
            const uint64_t offset = static_cast<uint64_t>(raw_out.count());
            WriteBufferFromOwnString chunk_buf;
            uint64_t node_count = 0;
            {
                ZstdDeflatingWriteBuffer zstd(&chunk_buf, /*compression_level=*/3);
                node_count = write_body(zstd);
                zstd.finalize();
            }
            const std::string & compressed = chunk_buf.str();
            raw_out.write(compressed.data(), compressed.size());
            chunks.push_back(SnapshotChunkDescriptor{type, offset, static_cast<uint64_t>(compressed.size()), node_count});
        };

        write_chunk(
            SnapshotChunkType::METADATA,
            [&](WriteBuffer & out) -> uint64_t
            {
                writeBinary(static_cast<uint8_t>(SnapshotVersion::V8), out);
                serializeSnapshotMetadata(snapshot.snapshot_meta, out);
                serializeStorageMetaFields(snapshot, out, SnapshotVersion::V8, keeper_context);
                serializeSessionsAndConfig(snapshot, out);
                return 0;
            });

        // node_count per chunk is stored in the descriptor, not the chunk body.
        {
            size_t remaining_nodes = total_nodes;
            auto it = snapshot.begin;
            size_t container_pos = 0;

            for (size_t chunk_idx = 0; chunk_idx < nodes_chunk_count; ++chunk_idx)
            {
                const size_t nodes_for_this_chunk = std::min(remaining_nodes, chunk_size_limit);

                write_chunk(
                    SnapshotChunkType::NODES,
                    [&](WriteBuffer & out) -> uint64_t
                    {
                        uint64_t actual_node_count = 0;

                        while (actual_node_count < nodes_for_this_chunk && container_pos < snapshot.snapshot_container_size)
                        {
                            const auto & path = it->key;
                            const auto & node = it->value;

                            if (serializeSnapshotNode(path, node, snapshot.version, snapshot.zxid, out))
                                ++actual_node_count;

                            const bool last = (container_pos + 1 >= snapshot.snapshot_container_size);
                            if (!last)
                                ++it;
                            ++container_pos;
                        }

                        return actual_node_count;
                    });

                remaining_nodes -= chunks.back().node_count;
            }
        }

        packChunkedSnapshotFooter(chunks, raw_out);

        return chunks;
    }
}

template<typename Storage>
void KeeperStorageSnapshot<Storage>::serialize(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context)
{
    if (snapshot.version < SnapshotVersion::V8)
    {
        SharedLockGuard storage_lock(snapshot.storage->storage_mutex);
        if (!snapshot.storage->ttl_paths.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot serialize snapshot with version {}: storage contains {} TTL node(s), which require snapshot "
                "version {} or higher. Bump write_snapshot_version after every replica has been upgraded.",
                static_cast<uint8_t>(snapshot.version),
                snapshot.storage->ttl_paths.size(),
                static_cast<uint8_t>(SnapshotVersion::V8));
    }
    if (snapshot.version >= SnapshotVersion::V9)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Chunked snapshots must be written via serializeChunkedSnapshot (which manages per-chunk compression "
            "and the front-header + footer index), not via the legacy streaming serialiser");

    writeBinary(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);

    serializeStorageMetaFields(snapshot, out, snapshot.version, keeper_context);

    /// Serialize data tree
    writeBinary(snapshot.snapshot_container_size - keeper_context->getSystemNodesWithData().size(), out);
    size_t counter = 0;
    for (auto it = snapshot.begin; counter < snapshot.snapshot_container_size; ++counter)
    {
        const auto & path = it->key;
        const auto & node = it->value;

        /// (This is guaranteed because KeeperStorageSnapshot constructor is called with nuraft's
        ///  commit_lock_ held, and therefore storage can't change between when we get storage->zxid
        ///  and when we call storage->enableSnapshotMode().)
        serializeSnapshotNode(path, node, snapshot.version, snapshot.zxid, out);

        /// Last iteration: exit here without iterator increment to avoid a false-positive
        /// race condition on list end.
        if (counter == snapshot.snapshot_container_size - 1)
            break;

        ++it;
    }

    serializeSessionsAndConfig(snapshot, out);
}

template <typename Storage>
void KeeperStorageSnapshot<Storage>::deserialize(
    SnapshotDeserializationResult<Storage> & deserialization_result,
    ReadBuffer & in,
    KeeperContextPtr keeper_context,
    bool load_full_storage) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    uint8_t version = 0;
    readBinary(version, in);
    SnapshotVersion current_version = static_cast<SnapshotVersion>(version);
    if (current_version > MAX_SUPPORTED_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);
    if (current_version >= SnapshotVersion::V9)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Snapshot version {} reached the legacy reader; V9+ snapshots must use the chunked "
            "front-header path, or the input is corrupt/unsupported", version);

    deserialization_result.snapshot_meta = deserializeSnapshotMetadata(in);
    Storage & storage = *deserialization_result.storage;

    bool recalculate_digest = keeper_context->digestEnabled();
    deserializeStorageMetaFields(storage, deserialization_result.snapshot_meta, in, current_version, recalculate_digest, keeper_context);

    size_t snapshot_container_size = 0;
    readBinary(snapshot_container_size, in);
    if constexpr (!use_rocksdb)
        storage.container.reserve(snapshot_container_size);

    if (recalculate_digest)
        storage.nodes_digest = 0;

    auto batch_load_size = keeper_context->getCoordinationSettings()[CoordinationSetting::rocksdb_load_batch_size];
    if constexpr (use_rocksdb)
        storage.container.startLoading(batch_load_size);

    for (size_t nodes_read = 0; nodes_read < snapshot_container_size; ++nodes_read)
    {
        size_t path_size = 0;
        readVarUInt(path_size, in);
        chassert(path_size != 0);
        auto path_data = storage.container.allocateKey(path_size);
        in.readStrict(path_data.get(), path_size);
        std::string_view path{path_data.get(), path_size};

        typename Storage::Node node{};

        if (!load_full_storage)
        {
            deserialization_result.paths.push_back(std::string{path});
            readNode(node, in, current_version, &storage.acl_map, keeper_context->shouldBlockACL());
            continue;
        }

        readNode(node, in, current_version, &storage.acl_map, keeper_context->shouldBlockACL());
        const auto match_result = Coordination::matchPath(path, keeper_system_path);

        if (!prepareDeserializedNode(match_result, path, node, keeper_context))
            continue;

        auto ephemeral_owner = node.stats.ephemeralOwner();
        if (ephemeral_owner != 0)
        {
            storage.committed_ephemerals[node.stats.ephemeralOwner()].insert(std::string{path});
            ++storage.committed_ephemeral_nodes;
        }

        if (recalculate_digest)
            storage.nodes_digest += node.getDigest(path);

        if (node.stats.isTTL())
        {
            storage.ttl_paths.insert(std::string{path});
            storage.committed_ttl_nodes.fetch_add(1);
        }

        storage.container.insertOrReplace(std::move(path_data), path_size, std::move(node));
    }

    /// The snapshot's ACL map may contain ACLs that are not referenced by any node, e.g. ACLs
    /// that were referenced only by uncommitted nodes.
    storage.acl_map.removeUnusedACLs();

    if constexpr (use_rocksdb)
    {
        LOG_TRACE(getLogger("KeeperSnapshotManager"), "Update node stats");
        storage.container.finishLoading();
    }

    if constexpr (!use_rocksdb)
    {
        LOG_TRACE(getLogger("KeeperSnapshotManager"), "Building structure for children nodes");

        for (const auto & itr : storage.container)
        {
            if (itr.key != "/")
            {
                auto parent_path = Coordination::parentNodePath(itr.key);
                storage.container.updateValue(
                    parent_path, [path = itr.key](typename Storage::Node & value) { value.addChild(Coordination::getBaseNodeName(path)); });
            }
        }

        for (const auto & itr : storage.container)
        {
            if (itr.key != "/")
            {
                if (itr.value.numChildren() != static_cast<int32_t>(itr.value.getChildren().size()))
                {
#ifdef NDEBUG
                    /// TODO (alesapin) remove this, it should be always CORRUPTED_DATA.
                    LOG_ERROR(
                        getLogger("KeeperSnapshotManager"),
                        "Children counter in stat.numChildren {}"
                        " is different from actual children size {} for node {}",
                        itr.value.numChildren(),
                        itr.value.getChildren().size(),
                        itr.key);
#else
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Children counter in stat.numChildren {}"
                        " is different from actual children size {} for node {}",
                        itr.value.numChildren(),
                        itr.value.getChildren().size(),
                        itr.key);
#endif
                }
            }
        }
    }

    deserializeSessionsAndConfig(storage, deserialization_result.cluster_config, in, current_version);

    storage.updateStats();
}

template<typename Storage>
KeeperStorageSnapshot<Storage>::KeeperStorageSnapshot(Storage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_)
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
    scope_guard snapshot_mode_guard([&] { storage->disableSnapshotMode(); });
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
    snapshot_mode_guard.release();
}

template<typename Storage>
KeeperStorageSnapshot<Storage>::KeeperStorageSnapshot(
    Storage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_, SnapshotVersion version_)
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
    scope_guard snapshot_mode_guard([&] { storage->disableSnapshotMode(); });
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
    snapshot_mode_guard.release();
}

template<typename Storage>
KeeperStorageSnapshot<Storage>::~KeeperStorageSnapshot()
{
    storage->disableSnapshotMode();
}

template<typename Storage>
SnapshotFileInfoPtr
KeeperSnapshotManager<Storage>::makeManagedSnapshotFileInfo(std::string path, DiskPtr disk, uint64_t log_idx) const
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

template<typename Storage>
KeeperSnapshotManager<Storage>::KeeperSnapshotManager(
    size_t snapshots_to_keep_,
    const KeeperContextPtr & keeper_context_,
    bool compress_snapshots_zstd_,
    const std::string & superdigest_,
    size_t storage_tick_time_)
    : snapshots_to_keep(snapshots_to_keep_)
    , compress_snapshots_zstd(compress_snapshots_zstd_)
    , superdigest(superdigest_)
    , storage_tick_time(storage_tick_time_)
    , keeper_context(keeper_context_)
{
    // 0 → all CPU cores; >1 → parallel deserialisation; 1 → serial. Memory storage only (RocksDB skips chunked snapshots).
    if constexpr (!use_rocksdb)
    {
        const auto & coordination_settings = keeper_context->getCoordinationSettings();
        const uint64_t raw = static_cast<uint64_t>(coordination_settings[CoordinationSetting::snapshot_deser_threads]);
        deser_threads = (raw == 0) ? getNumberOfCPUCoresToUse() : static_cast<size_t>(raw);

        if (deser_threads > 1)
        {
            // shutdown_on_exception=false: a worker exception must not permanently poison
            // the pool; wait() rethrows and clears it.
            deser_pool.emplace(
                CurrentMetrics::KeeperSnapshotDeserThreads,
                CurrentMetrics::KeeperSnapshotDeserThreadsActive,
                CurrentMetrics::KeeperSnapshotDeserThreadsScheduled,
                /*max_threads=*/deser_threads,
                /*max_free_threads=*/0,
                /*queue_size=*/0,
                /*shutdown_on_exception=*/false);
        }
    }

    std::unordered_set<DiskPtr> read_disks;

    struct DuplicateSnapshotFile
    {
        DiskPtr disk;
        std::string path;
        uint64_t up_to_log_idx = 0;
    };
    /// Same-index duplicates found during the scan; handled after all disks are scanned
    /// because the decision depends on the latest registered index.
    std::vector<DuplicateSnapshotFile> duplicate_snapshot_files;

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
            size_t snapshot_up_to = getLogIdxFromSnapshotPath(snapshot_file);
            if (existing_snapshots.contains(snapshot_up_to))
            {
                /// Equivalent snapshots for the same committed index (upgrade races, crashed loser
                /// cleanup, interrupted moves). First-scanned copy stays registered; the duplicate
                /// is handled after the scan.
                duplicate_snapshot_files.push_back(DuplicateSnapshotFile{disk, snapshot_file, snapshot_up_to});
                continue;
            }
            existing_snapshots.emplace(snapshot_up_to, makeManagedSnapshotFileInfo(snapshot_file, disk, snapshot_up_to));
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

    /// Duplicates outside the retained window are deleted. Duplicates within it are kept as
    /// redundant recovery points (operator can remove a broken copy and restart from another).
    const uint64_t latest_registered_idx = getLatestSnapshotIndex();
    std::optional<uint64_t> oldest_retained_idx;
    if (!existing_snapshots.empty() && snapshots_to_keep > 0)
    {
        const size_t purged_count
            = existing_snapshots.size() > snapshots_to_keep ? existing_snapshots.size() - snapshots_to_keep : 0;
        oldest_retained_idx = std::next(existing_snapshots.begin(), purged_count)->first;
    }
    for (auto & duplicate : duplicate_snapshot_files)
    {
        const auto & registered = existing_snapshots.at(duplicate.up_to_log_idx);
        if (!oldest_retained_idx || duplicate.up_to_log_idx < *oldest_retained_idx)
        {
            LOG_WARNING(
                log,
                "Found duplicate snapshot file {} on disk {} for log index {} which is outside the retained window; "
                "keeping {} on disk {} and removing the duplicate",
                duplicate.path,
                duplicate.disk->getName(),
                duplicate.up_to_log_idx,
                registered->path,
                registered->disk->getName());
            duplicate.disk->removeFileIfExists(duplicate.path);
            continue;
        }

        /// Same-named cross-disk duplicate (interrupted move): re-point registration to the copy
        /// already on the target disk so maintenance doesn't overwrite it via `copyFile`.
        const DiskPtr target_disk = (duplicate.up_to_log_idx == latest_registered_idx) ? getLatestSnapshotDisk() : getDisk();
        if (duplicate.path == registered->path && duplicate.disk != registered->disk && duplicate.disk == target_disk)
        {
            LOG_WARNING(
                log,
                "Re-pointing registered snapshot {} for retained log index {} from disk {} to its same-named copy on target disk {}; "
                "keeping both copies as redundant recovery points",
                registered->path,
                duplicate.up_to_log_idx,
                registered->disk->getName(),
                duplicate.disk->getName());
            /// Track the now-unreferenced original so retention reclaims it with this index.
            const DiskPtr orphaned_disk = registered->disk;
            retained_duplicate_snapshots[duplicate.up_to_log_idx].push_back(
                makeManagedSnapshotFileInfo(registered->path, orphaned_disk, duplicate.up_to_log_idx));
            registered->disk = duplicate.disk;
        }
        else
        {
            LOG_WARNING(
                log,
                "Found duplicate snapshot file {} on disk {} for retained log index {}; keeping it as a redundant recovery copy "
                "next to the registered {} on disk {} until the index leaves the retained window",
                duplicate.path,
                duplicate.disk->getName(),
                duplicate.up_to_log_idx,
                registered->path,
                registered->disk->getName());
            /// Track the kept duplicate so it ages out with its index (as the message promises).
            retained_duplicate_snapshots[duplicate.up_to_log_idx].push_back(
                makeManagedSnapshotFileInfo(std::move(duplicate.path), std::move(duplicate.disk), duplicate.up_to_log_idx));
        }
    }

    /// Runs before `init` sets the mark, so `protected_snapshot_log_idx == 0` here — nothing
    /// to pin yet. With `snapshots_to_keep == 0` retention keeps none at startup (pre-existing).
    runMaintenanceInline(/*just_written_log_idx=*/0);
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::writeSnapshotBufferToFile(nuraft::buffer & buffer, uint64_t up_to_log_idx)
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
        disk->removeFile(tmp_snapshot_file_name);
    }
    catch (...)
    {
        cancelAndResetWriteBuffer(plain_buf);
        removeFailedSnapshotArtifacts(disk, snapshot_file_name, tmp_snapshot_file_name, log);
        throw;
    }

    return makeManagedSnapshotFileInfo(snapshot_file_name, disk, up_to_log_idx);
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::tryReuseRegisteredSnapshot(uint64_t up_to_log_idx) const
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

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::publishAndRunMaintenance(uint64_t up_to_log_idx, SnapshotFileInfoPtr written)
{
    auto published_snapshot_file_info = publishSnapshotFile(up_to_log_idx, written);
    if (published_snapshot_file_info != written)
        retireUnpublishedSnapshotFile(written);
    runMaintenanceInline(up_to_log_idx);
    return published_snapshot_file_info;
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx)
{
    if (auto existing = tryReuseRegisteredSnapshot(up_to_log_idx))
        return existing;

    auto snapshot_file_info = writeSnapshotBufferToFile(buffer, up_to_log_idx);
    return publishAndRunMaintenance(up_to_log_idx, snapshot_file_info);
}

template<typename Storage>
std::unique_ptr<SnapshotReceiveCtx> KeeperSnapshotManager<Storage>::beginSnapshotReceiveToDisk(uint64_t up_to_log_idx)
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

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::finalizeSnapshotReceiveToDisk(SnapshotReceiveCtx & ctx)
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

    auto snapshot_file_info = makeManagedSnapshotFileInfo(ctx.snapshot_file_name, ctx.disk, ctx.log_idx);
    return publishAndRunMaintenance(ctx.log_idx, snapshot_file_info);
}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperSnapshotManager<Storage>::deserializeLatestSnapshotBufferFromDisk()
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

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperSnapshotManager<Storage>::deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const
{
    const auto & snapshot_info = *existing_snapshots.at(up_to_log_idx);
    return deserializeSnapshotBufferFromDisk(snapshot_info);
}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperSnapshotManager<Storage>::deserializeSnapshotBufferFromDisk(const SnapshotFileInfo & snapshot_info) const
{
    WriteBufferFromNuraftBuffer writer;
    auto reader = snapshot_info.disk->readFile(snapshot_info.path, getReadSettings());
    copyData(*reader, writer);
    return writer.getBuffer();
}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperSnapshotManager<Storage>::serializeSnapshotToBuffer(const KeeperStorageSnapshot<Storage> & snapshot) const
{
    if (snapshot.version >= SnapshotVersion::V9)
    {
        // Chunked format: append-only. serializeChunkedSnapshot writes a 13-byte front header,
        // per-chunk ZSTD frames, then the footer (descriptor table)
        auto writer = std::make_unique<WriteBufferFromNuraftBuffer>();
        auto * raw_ptr = writer.get();
        serializeChunkedSnapshot(snapshot, *writer, keeper_context);
        writer->finalize();
        return raw_ptr->getBuffer();
    }

    std::unique_ptr<WriteBufferFromNuraftBuffer> writer = std::make_unique<WriteBufferFromNuraftBuffer>();
    auto * buffer_raw_ptr = writer.get();
    std::unique_ptr<WriteBuffer> compressed_writer;
    if (compress_snapshots_zstd)
        compressed_writer = wrapWriteBufferWithCompressionMethod(std::move(writer), CompressionMethod::Zstd, 3);
    else
        compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer);

    KeeperStorageSnapshot<Storage>::serialize(snapshot, *compressed_writer, keeper_context);
    compressed_writer->finalize();
    return buffer_raw_ptr->getBuffer();
}

template <typename Storage>
SnapshotDeserializationResult<Storage>
KeeperSnapshotManager<Storage>::deserializeChunkedSnapshotFromBuffer(ReadBufferFromNuraftBuffer & buffer, bool load_full_storage) const
{
    if constexpr (use_rocksdb)
    {
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Chunked snapshot format is not supported with RocksDB storage");
    }
    else
    {
        auto chunks = parseAndValidateChunkedSnapshot(buffer);

        // Chunked snapshot storage: root "/" must come from the snapshot, not the constructor.
        SnapshotDeserializationResult<Storage> result;
        result.storage = std::make_unique<Storage>(
            storage_tick_time,
            superdigest,
            keeper_context,
            /* initialize_system_nodes = */ false);
        Storage & storage = *result.storage;

        bool recalculate_digest = keeper_context->digestEnabled();

        // Partition chunks by type; unknown future types are ignored.
        const SnapshotChunkDescriptor * metadata_chunk_info = nullptr;
        std::vector<const SnapshotChunkDescriptor *> nodes_chunks;
        for (const auto & chunk_descriptor : chunks)
        {
            if (chunk_descriptor.type == SnapshotChunkType::METADATA)
                metadata_chunk_info = &chunk_descriptor;
            else if (chunk_descriptor.type == SnapshotChunkType::NODES)
                nodes_chunks.push_back(&chunk_descriptor);
        }

        if (!metadata_chunk_info)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot: no METADATA chunk found");

        // ── METADATA chunk ────────────────────────────────────────────────────────────────────────────
        auto compressed_metadata
            = makeChunkReader(buffer.getView(metadata_chunk_info->compressed_offset, metadata_chunk_info->compressed_size));
        ReadBuffer & metadata_rbuf = *compressed_metadata;

        uint8_t version_byte = 0;
        readBinary(version_byte, metadata_rbuf);
        if (version_byte != static_cast<uint8_t>(SnapshotVersion::V8))
            throw Exception(
                ErrorCodes::UNKNOWN_FORMAT_VERSION,
                "Chunked snapshot: unexpected version byte {} in METADATA chunk (expected 8)",
                version_byte);
        const SnapshotVersion chunked_version = static_cast<SnapshotVersion>(version_byte);

        result.snapshot_meta = deserializeSnapshotMetadata(metadata_rbuf);
        deserializeStorageMetaFields(storage, result.snapshot_meta, metadata_rbuf, chunked_version, recalculate_digest, keeper_context);
        deserializeSessionsAndConfig(storage, result.cluster_config, metadata_rbuf, chunked_version);
        if (!metadata_rbuf.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot: trailing bytes after METADATA chunk content");

        if (recalculate_digest)
            storage.nodes_digest = 0;

        const bool cleanup_acl_global = keeper_context->shouldBlockACL();

        const size_t nodes_chunk_count = nodes_chunks.size();

        // Per-chunk parser — shared by serial and parallel paths.
        // Accepts an explicit handle reference so the serial path can reuse a single handle
        // across all chunks while the parallel path passes a dedicated per-chunk handle.
        auto process_nodes_chunk = [&](const SnapshotChunkDescriptor & nodes_chunk_descriptor, MemorySnapshotLoadHandle * handle)
        {
            auto compressed_nodes_chunk = makeChunkReader(
                buffer.getView(nodes_chunk_descriptor.compressed_offset, nodes_chunk_descriptor.compressed_size));
            ReadBuffer & rbuf = *compressed_nodes_chunk;

            // node_count is stored in the header descriptor, not the chunk body.
            const uint64_t node_count = nodes_chunk_descriptor.node_count;

            if (!handle)
            {
                // Analyzer (path-only) mode: handle == nullptr when !load_full_storage.
                for (uint64_t i = 0; i < node_count; ++i)
                {
                    String path;
                    readBinary(path, rbuf);
                    result.paths.push_back(path);
                    typename Storage::Node node{};
                    readNode(node, rbuf, chunked_version, nullptr, /*cleanup_acl=*/true);
                }
                if (!rbuf.eof())
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA, "Chunked snapshot: trailing bytes after NODES chunk content (analyzer mode)");
                return;
            }

            MemorySnapshotLoadHandle & current_handle = *handle;
            for (uint64_t i = 0; i < node_count; ++i)
            {
                String path_str;
                readBinary(path_str, rbuf);

                using enum Coordination::PathMatchResult;
                const auto match = Coordination::matchPath(path_str, keeper_system_path);

                typename Storage::Node node{};
                readNode(node, rbuf, chunked_version, nullptr, cleanup_acl_global);

                if (!prepareDeserializedNode(match, path_str, node, keeper_context))
                    continue;

                const auto ephemeral_owner = node.stats.ephemeralOwner();
                if (ephemeral_owner != 0)
                {
                    current_handle.local_ephemerals[ephemeral_owner].insert(path_str);
                    ++current_handle.local_ephemeral_nodes;
                }

                if (recalculate_digest)
                    current_handle.digest_sum += node.getDigest(path_str);

                if (!cleanup_acl_global && node.acl_id != 0)
                    ++current_handle.acl_usage[node.acl_id];

                const size_t path_size = path_str.size();
                auto key = current_handle.nodes.allocateKey(path_size);
                std::memcpy(key.get(), path_str.data(), path_size);
                current_handle.nodes.emplace(std::move(key), path_size, std::move(node));
            }

            if (!rbuf.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot: trailing bytes after NODES chunk content");
        };

        // Dispatch: parallel for full-load with >=2 threads and >=2 chunks; serial otherwise.
        if (load_full_storage && deser_pool.has_value() && nodes_chunk_count > 1)
        {
            // One handle per worker thread; threads self-schedule via fetch_add.
            const size_t num_workers = std::min(deser_threads, nodes_chunk_count);
            std::vector<MemorySnapshotLoadHandle> handles;
            handles.reserve(num_workers);
            for (size_t i = 0; i < num_workers; ++i)
                handles.push_back(beginMemorySnapshotLoad(storage));

            std::atomic<size_t> next_chunk{0};
            ThreadPoolCallbackRunnerLocal<void> runner(*deser_pool, ThreadName::KEEPER_SNAPSHOT_LOAD);
            for (size_t w = 0; w < num_workers; ++w)
            {
                runner.enqueueAndKeepTrack(
                    [&next_chunk, nodes_chunk_count, &nodes_chunks, process_nodes_chunk, &handle = handles[w]]()
                    {
                        size_t idx = 0;
                        while ((idx = next_chunk.fetch_add(1, std::memory_order_relaxed)) < nodes_chunk_count)
                            process_nodes_chunk(*nodes_chunks[idx], &handle);
                    });
            }
            runner.waitForAllToFinishAndRethrowFirstError();

            finalizeMemorySnapshotLoad(storage, handles, recalculate_digest);
        }
        else if (load_full_storage)
        {
            // Serial full-load: single handle reused across all chunks.
            MemorySnapshotLoadHandle handle = beginMemorySnapshotLoad(storage);
            for (size_t nodes_chunk_idx = 0; nodes_chunk_idx < nodes_chunk_count; ++nodes_chunk_idx)
                process_nodes_chunk(*nodes_chunks[nodes_chunk_idx], &handle);
            finalizeMemorySnapshotLoad(storage, {&handle, 1}, recalculate_digest);
        }
        else
        {
            // Analyzer (path-only) mode.
            for (size_t nodes_chunk_idx = 0; nodes_chunk_idx < nodes_chunk_count; ++nodes_chunk_idx)
                process_nodes_chunk(*nodes_chunks[nodes_chunk_idx], nullptr);
        }

        if (load_full_storage)
        {
            storage.initializeSystemNodes();
            storage.updateStats();
        }

        return result;

    }
}

template<typename Storage>
bool KeeperSnapshotManager<Storage>::isZstdCompressed(nuraft::ptr<nuraft::buffer> buffer)
{
    static constexpr unsigned char ZSTD_COMPRESSED_MAGIC[4] = {0x28, 0xB5, 0x2F, 0xFD};

    ReadBufferFromNuraftBuffer reader(buffer);
    unsigned char magic_from_buffer[4]{};
    reader.readStrict(reinterpret_cast<char *>(&magic_from_buffer), sizeof(magic_from_buffer));
    buffer->pos(0);
    return memcmp(magic_from_buffer, ZSTD_COMPRESSED_MAGIC, 4) == 0;
}

template <typename Storage>
SnapshotDeserializationResult<Storage>
KeeperSnapshotManager<Storage>::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer, bool load_full_storage) const
{
    buffer->pos(0);
    auto reader = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);

    // Format detection (chunked first):
    //   CKFS magic + valid chunked structure → chunked format (v8); version checked by parseAndValidateChunkedSnapshot
    //     (a legacy non-ZSTD CompressedWriteBuffer checksum can start with CKFS, so isChunkedSnapshot
    //      validates the whole descriptor table, not just the magic, before routing here)
    //   ZSTD magic (front)                    → legacy ZSTD (V3-V7) — mutually exclusive by magic
    //   else                                  → legacy LZ4 / CompressedReadBuffer (V0-V2)
    auto is_chunked_snapshot = isChunkedSnapshot(*reader);
    reader->seek(0, SEEK_SET);
    if (is_chunked_snapshot)
        return deserializeChunkedSnapshotFromBuffer(*reader, load_full_storage);

    bool is_zstd_compressed = isZstdCompressed(buffer);

    std::unique_ptr<ReadBuffer> compressed_reader;

    if (is_zstd_compressed)
        compressed_reader = wrapReadBufferWithCompressionMethod(std::move(reader), CompressionMethod::Zstd);
    else
        compressed_reader = std::make_unique<CompressedReadBuffer>(*reader);

    SnapshotDeserializationResult<Storage> result;
    result.storage = std::make_unique<Storage>(storage_tick_time, superdigest, keeper_context, /* initialize_system_nodes */ false);
    KeeperStorageSnapshot<Storage>::deserialize(result, *compressed_reader, keeper_context, load_full_storage);
    if (load_full_storage)
        result.storage->initializeSystemNodes();
    return result;
}

/// Decompress only the METADATA chunk of a chunked snapshot and return the snapshot metadata.
static SnapshotMetadataPtr deserializeChunkedSnapshotMetadataFromBuffer(ReadBufferFromNuraftBuffer & buffer)
{
    auto chunks = parseAndValidateChunkedSnapshot(buffer);

    const SnapshotChunkDescriptor * metadata_chunk_descriptor = nullptr;
    for (const auto & chunk : chunks)
    {
        if (chunk.type == SnapshotChunkType::METADATA)
        {
            metadata_chunk_descriptor = &chunk;
            break;
        }
    }

    if (!metadata_chunk_descriptor)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot: no METADATA chunk found");

    auto zbuf_meta
        = makeChunkReader(buffer.getView(metadata_chunk_descriptor->compressed_offset, metadata_chunk_descriptor->compressed_size));
    ReadBuffer & rbuf = *zbuf_meta;
    uint8_t version_byte = 0;
    readBinary(version_byte, rbuf);
    if (version_byte != static_cast<uint8_t>(SnapshotVersion::V8))
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Chunked snapshot: unexpected version byte {} in METADATA chunk (expected 8)",
            version_byte);

    return deserializeSnapshotMetadata(rbuf);
}

template<typename Storage>
SnapshotMetadataPtr KeeperSnapshotManager<Storage>::deserializeSnapshotMetadataFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const
{
    /// `nuraft::buffer::pos(0)` resets the cursor. This method must leave the
    /// buffer at offset `0` on success and on throw.
    SCOPE_EXIT({ buffer->pos(0); });

    std::unique_ptr<ReadBufferFromNuraftBuffer> reader = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);
    auto is_chunked_snapshot = isChunkedSnapshot(*reader);
    reader->seek(0, SEEK_SET);
    if (is_chunked_snapshot)
        return deserializeChunkedSnapshotMetadataFromBuffer(*reader);

    bool is_zstd_compressed = isZstdCompressed(buffer);

    std::unique_ptr<ReadBuffer> compressed_reader;

    if (is_zstd_compressed)
        compressed_reader = wrapReadBufferWithCompressionMethod(std::move(reader), CompressionMethod::Zstd);
    else
        compressed_reader = std::make_unique<CompressedReadBuffer>(*reader);

    uint8_t version = 0;
    readBinary(version, *compressed_reader);
    SnapshotVersion current_version = static_cast<SnapshotVersion>(version);
    if (current_version > MAX_SUPPORTED_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);
    if (current_version >= SnapshotVersion::V9)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Snapshot version {} reached the legacy reader; V9+ snapshots must use the chunked "
            "front-header path, or the input is corrupt/unsupported", version);

    return deserializeSnapshotMetadata(*compressed_reader);
}

template<typename Storage>
SnapshotDeserializationResult<Storage> KeeperSnapshotManager<Storage>::restoreFromLatestSnapshot()
{
    if (existing_snapshots.empty())
        return {};

    auto buffer = deserializeLatestSnapshotBufferFromDisk();
    if (!buffer)
        return {};
    return deserializeSnapshotFromBuffer(buffer);
}

template<typename Storage>
DiskPtr KeeperSnapshotManager<Storage>::getDisk() const
{
    return keeper_context->getSnapshotDisk();
}

template<typename Storage>
DiskPtr KeeperSnapshotManager<Storage>::getLatestSnapshotDisk() const
{
    return keeper_context->getLatestSnapshotDisk();
}

template<typename Storage>
void KeeperSnapshotManager<Storage>::setProtectedSnapshotIndex(uint64_t log_idx)
{
    protected_snapshot_log_idx = log_idx;
}

template<typename Storage>
void KeeperSnapshotManager<Storage>::setProtectedPendingSnapshotIndex(uint64_t log_idx)
{
    protected_pending_snapshot_log_idx = log_idx;
}

template <typename Storage>
std::vector<SnapshotFileInfoPtr>
KeeperSnapshotManager<Storage>::detachSnapshotForRemoval(std::map<uint64_t, SnapshotFileInfoPtr>::iterator itr)
{
    const uint64_t log_idx = itr->first;
    std::vector<SnapshotFileInfoPtr> retired;
    auto snapshot_file_info = itr->second;
    snapshot_file_info->retired_for_removal.store(true, std::memory_order_release);
    existing_snapshots.erase(itr);
    retired.push_back(std::move(snapshot_file_info));

    /// Retire same-index recovery copies; caller's pin drop unlinks them outside `snapshots_lock`.
    if (auto dup_it = retained_duplicate_snapshots.find(log_idx); dup_it != retained_duplicate_snapshots.end())
    {
        for (auto & duplicate : dup_it->second)
        {
            duplicate->retired_for_removal.store(true, std::memory_order_release);
            retired.push_back(std::move(duplicate));
        }
        retained_duplicate_snapshots.erase(dup_it);
    }

    return retired;
}

template<typename Storage>
std::vector<SnapshotFileInfoPtr> KeeperSnapshotManager<Storage>::detachOutdatedSnapshotsIfNeeded(uint64_t just_written_log_idx)
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

template<typename Storage>
std::vector<SnapshotMoveCandidate> KeeperSnapshotManager<Storage>::selectSnapshotsToMove()
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

template<typename Storage>
SnapshotMaintenanceTasks KeeperSnapshotManager<Storage>::prepareSnapshotMaintenanceTasks(uint64_t just_written_log_idx)
{
    SnapshotMaintenanceTasks tasks;
    tasks.retired_snapshots = detachOutdatedSnapshotsIfNeeded(just_written_log_idx);
    tasks.move_candidates = selectSnapshotsToMove();
    return tasks;
}

template<typename Storage>
void KeeperSnapshotManager<Storage>::removeSnapshot(uint64_t log_idx)
{
    /// Tests/tools only: the dropped pins unlink synchronously here; the server reclaims
    /// via the deferred Phase 4 path.
    auto it = existing_snapshots.find(log_idx);
    if (it == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);
    detachSnapshotForRemoval(it);
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::writeSnapshotFile(const KeeperStorageSnapshot<Storage> & snapshot)
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

        if (snapshot.version >= SnapshotVersion::V9)
        {
            // Chunked format: append-only (front header + per-chunk ZSTD frames + footer; no trailer).
            // No seek/backpatch, so a forward-only WriteBuffer is sufficient.
            auto * raw_writer = writer.get();
            serializeChunkedSnapshot(snapshot, *raw_writer, keeper_context);

            // count() now equals the full file size (header + chunks + footer).
            const size_t bytes_written = raw_writer->count();
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, bytes_written);

            raw_writer->finalize();

            Stopwatch watch;
            raw_writer->sync();
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotFileSyncMicroseconds, watch.elapsedMicroseconds());

            writer.reset();
        }
        else
        {
            if (compress_snapshots_zstd)
                compressed_writer = wrapWriteBufferWithCompressionMethod(std::move(writer), CompressionMethod::Zstd, 3);
            else
                compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer);

            const size_t bytes_before = compressed_writer->count();
            KeeperStorageSnapshot<Storage>::serialize(snapshot, *compressed_writer, keeper_context);
            const size_t bytes_written = compressed_writer->count() - bytes_before;
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, bytes_written);

            compressed_writer->finalize();

            Stopwatch watch;
            compressed_writer->sync();
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotFileSyncMicroseconds, watch.elapsedMicroseconds());

            compressed_writer.reset();
            writer.reset();
        }

        disk->removeFile(tmp_snapshot_file_name);
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

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::serializeSnapshotToDisk(const KeeperStorageSnapshot<Storage> & snapshot)
{
    auto up_to_log_idx = snapshot.snapshot_meta->get_last_log_idx();
    if (auto existing = tryReuseRegisteredSnapshot(up_to_log_idx))
        return existing;

    auto snapshot_file_info = writeSnapshotFile(snapshot);
    return publishAndRunMaintenance(up_to_log_idx, snapshot_file_info);
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::publishSnapshotFile(uint64_t up_to_log_idx, SnapshotFileInfoPtr file_info)
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

template<typename Storage>
void KeeperSnapshotManager<Storage>::retireUnpublishedSnapshotFile(const SnapshotFileInfoPtr & file_info) const
{
    if (file_info)
        file_info->retired_for_removal.store(true, std::memory_order_release);
}

template<typename Storage>
bool KeeperSnapshotManager<Storage>::publishMovedSnapshotIfValid(const SnapshotMoveCandidate & candidate)
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

template<typename Storage>
void KeeperSnapshotManager<Storage>::cleanupCopiedMoveTarget(const SnapshotMoveCandidate & candidate) const
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

template<typename Storage>
bool KeeperSnapshotManager<Storage>::moveSnapshotCandidate(
    const SnapshotMoveCandidate & candidate,
    const std::function<bool(const SnapshotMoveCandidate &)> & publish_moved_snapshot)
{
    bool metadata_published = false;
    try
    {
        moveFileBetweenDisks(
            candidate.source_disk,
            candidate.source_path,
            candidate.target_disk,
            candidate.target_path,
            /// Returning false keeps the source file; the caller owns target cleanup.
            [&] { metadata_published = publish_moved_snapshot(candidate); return metadata_published; },
            log,
            keeper_context);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to move snapshot {}", candidate.log_idx));
    }

    if (!metadata_published)
        cleanupCopiedMoveTarget(candidate); /// harmless if the copy never completed
    return metadata_published;
}

template<typename Storage>
void KeeperSnapshotManager<Storage>::runMaintenanceInline(uint64_t just_written_log_idx)
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

template<typename Storage>
size_t KeeperSnapshotManager<Storage>::getLatestSnapshotIndex() const
{
    if (!existing_snapshots.empty())
        return existing_snapshots.rbegin()->first;
    return 0;
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::getLatestSnapshotInfo() const
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

template<typename Storage>
std::map<uint64_t, SnapshotFileInfoPtr> KeeperSnapshotManager<Storage>::getExistingSnapshots(const std::lock_guard<std::mutex> & /*snapshots_lock*/) const
{
    return existing_snapshots;
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::getSnapshotPin(uint64_t log_idx) const
{
    auto it = existing_snapshots.find(log_idx);
    if (it == existing_snapshots.end())
        return nullptr;
    return it->second;
}

template struct KeeperStorageSnapshot<KeeperMemoryStorage>;
template class KeeperSnapshotManager<KeeperMemoryStorage>;
#if USE_ROCKSDB
template struct KeeperStorageSnapshot<KeeperRocksStorage>;
template class KeeperSnapshotManager<KeeperRocksStorage>;
#endif
}
