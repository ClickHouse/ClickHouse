#include <filesystem>
#include <memory>
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
#include <Disks/IDisk.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
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
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <zstd.h>

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
    extern const int CANNOT_COMPRESS;
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
    /// path (C3) provided: a corrupt or truncated frame is detected rather than accepted.
    static std::unique_ptr<ReadBuffer> makeChunkReader(const char * data, size_t size)
    {
        auto raw = std::make_unique<ReadBufferFromMemory>(data, size);
        return std::make_unique<ZstdInflatingReadBuffer>(
            std::move(raw),
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
    }

    template<typename Node>
    void readNode(Node & node, ReadBuffer & in, SnapshotVersion version, ACLMap & acl_map, bool cleanup_acl)
    {
        readVarUInt(node.stats.data_size, in);
        if (node.stats.data_size != 0)
        {
            node.data = std::unique_ptr<char[]>(new char[node.stats.data_size]);
            in.readStrict(node.data.get(), node.stats.data_size);
        }

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

            if (!cleanup_acl)
                node.acl_id = acl_map.convertACLs(acls);
        }

        acl_map.addUsage(node.acl_id);

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
    }

    /// Per-node deserializer for the chunked snapshot format. Always uses the V7 wire encoding for individual nodes.
    /// Does NOT call acl_map.addUsage — chunked snapshot load path batches ACL usage in MemorySnapshotLoadHandle.
    /// Validates num_children >= 0 (untrusted snapshot input; throws CORRUPTED_DATA on failure).
    template<typename Node>
    void readChunkedSnapshotNode(Node & node, ReadBuffer & in, bool cleanup_acl)
    {
        readVarUInt(node.stats.data_size, in);
        if (node.stats.data_size != 0)
        {
            node.data = std::unique_ptr<char[]>(new char[node.stats.data_size]);
            in.readStrict(node.data.get(), node.stats.data_size);
        }

        // V7+ ACL ID as uint32_t
        readBinary(node.acl_id, in);
        if (cleanup_acl)
            node.acl_id = 0;
        // NOTE: Do NOT call acl_map.addUsage here.

        // V7+ has no is_sequential field.

        // Stats (same order as writeNode V7)
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

        // V6+ has no data_length field.
        int32_t num_children = 0;
        readBinary(num_children, in);
        if (num_children < 0)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot: negative num_children {} in node", num_children);
        node.setNumChildren(num_children);

        readBinary(node.stats.pzxid, in);

        // V7+: seq_num as int64_t (V6 and earlier used int32_t)
        int64_t seq_num = 0;
        readBinary(seq_num, in);
        if (ephemeral_owner == 0)
            node.stats.setSeqNum(seq_num);

        // V4-V5 had size_bytes; V7/V8 do not.
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

    /// Serialize a chunked (independently-compressed ZSTD) snapshot into `raw_out`.
    ///
    /// The caller must NOT wrap `raw_out` with a compression layer — the chunked format
    /// manages its own per-chunk ZSTD compression. A placeholder header of
    /// chunkedSnapshotHeaderSize(chunk_count) zeroed bytes is written first; the caller
    /// backpatches it after this function returns the chunk descriptor table.
    ///
    /// Chunk layout written:
    ///   1 × METADATA  — version byte, snapshot_meta, zxid+digest, session_id, ACL map
    ///   K × NODES     — each: uint64_t node_count, then node_count V7-encoded path+node pairs
    ///   1 × SESSIONS  — sorted sessions + optional cluster config
    ///
    /// K = ceil(total_non_system_nodes / chunk_size_limit), minimum 1.
    template <typename Storage>
    std::vector<SnapshotChunkDescriptor> serializeChunkedSnapshot(
        const KeeperStorageSnapshot<Storage> & snapshot,
        WriteBuffer & raw_out,
        KeeperContextPtr keeper_context)
    {
        const size_t system_nodes = keeper_context->getSystemNodesWithData().size();
        const size_t total_nodes = snapshot.snapshot_container_size > system_nodes
            ? snapshot.snapshot_container_size - system_nodes
            : 0;

        const size_t chunk_size_limit = std::max(
            static_cast<size_t>(1),
            static_cast<size_t>(keeper_context->getCoordinationSettings()[CoordinationSetting::snapshot_chunk_size]));

        // At least 1 NODES chunk even for empty storage (root "/" always present in practice,
        // but guard against edge cases with min-1).
        const size_t nodes_chunk_count = total_nodes == 0
            ? 1
            : (total_nodes + chunk_size_limit - 1) / chunk_size_limit;
        const uint64_t total_chunk_count = 1 + nodes_chunk_count + 1; // METADATA + N*NODES + SESSIONS

        // Write zeroed header placeholder; backpatched by the caller after chunks are written.
        const size_t header_size = chunkedSnapshotHeaderSize(total_chunk_count);
        {
            static const char kZeros[64] = {};
            size_t remaining = header_size;
            while (remaining > 0)
            {
                const size_t batch = std::min(remaining, sizeof(kZeros));
                raw_out.write(kZeros, batch);
                remaining -= batch;
            }
        }

        // Reusable ZSTD compression context (level 3, content checksum for integrity).
        ZSTD_CCtx * cctx = ZSTD_createCCtx();
        if (!cctx)
            throw Exception(ErrorCodes::CANNOT_COMPRESS,
                "Chunked snapshot: failed to create ZSTD compression context");
        SCOPE_EXIT({ ZSTD_freeCCtx(cctx); });
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 3);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);

        std::vector<SnapshotChunkDescriptor> chunks;
        chunks.reserve(total_chunk_count);

        // Compress the finalised `scratch` buffer and write one ZSTD frame to `raw_out`.
        // Records the chunk descriptor (type, absolute offset, compressed size).
        auto compress_and_record = [&](SnapshotChunkType type, WriteBufferFromOwnString & scratch) -> void
        {
            const std::string & uncompressed = scratch.str(); // finalises if not already
            const size_t max_compressed = ZSTD_compressBound(uncompressed.size());
            std::string compressed(max_compressed, '\0');
            const size_t compressed_size = ZSTD_compress2(
                cctx,
                compressed.data(), max_compressed,
                uncompressed.data(), uncompressed.size());
            if (ZSTD_isError(compressed_size))
                throw Exception(ErrorCodes::CANNOT_COMPRESS,
                    "Chunked snapshot ZSTD compression failed: {}", ZSTD_getErrorName(compressed_size));
            const uint64_t offset = static_cast<uint64_t>(raw_out.count());
            raw_out.write(compressed.data(), compressed_size);
            chunks.push_back(SnapshotChunkDescriptor{type, offset, static_cast<uint64_t>(compressed_size)});
        };

        // ── METADATA chunk ────────────────────────────────────────────────────────
        {
            WriteBufferFromOwnString scratch;
            writeBinary(static_cast<uint8_t>(SnapshotVersion::V8), scratch);
            serializeSnapshotMetadata(snapshot.snapshot_meta, scratch);
            // Chunked snapshot >= V5: always write zxid + digest
            writeBinary(snapshot.zxid, scratch);
            if (keeper_context->digestEnabled())
            {
                writeBinary(static_cast<uint8_t>(KEEPER_CURRENT_DIGEST_VERSION), scratch);
                writeBinary(snapshot.nodes_digest, scratch);
            }
            else
            {
                writeBinary(static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST), scratch);
            }
            writeBinary(snapshot.session_id, scratch);

            // ACL map — sorted for determinism across replicas (same as legacy serialiser).
            std::vector<std::pair<ACLId, Coordination::ACLs>> sorted_acl_map(
                snapshot.acl_map.begin(), snapshot.acl_map.end());
            ::sort(sorted_acl_map.begin(), sorted_acl_map.end());
            writeBinary(sorted_acl_map.size(), scratch);
            for (const auto & [acl_id, acls] : sorted_acl_map)
            {
                writeBinary(acl_id, scratch); // Chunked snapshot always uses V7+ uint32_t ACLId
                writeBinary(acls.size(), scratch);
                for (const auto & acl : acls)
                {
                    writeBinary(acl.permissions, scratch);
                    writeBinary(acl.scheme, scratch);
                    writeBinary(acl.id, scratch);
                }
            }
            compress_and_record(SnapshotChunkType::METADATA, scratch);
        }

        // ── NODES chunks ──────────────────────────────────────────────────────────
        // Each chunk: uint64_t node_count | node_count × (path binary + V7-encoded node).
        // The node_count is written as a zero placeholder and backpatched after the loop.
        {
            size_t remaining_nodes = total_nodes;
            auto it = snapshot.begin;
            size_t container_pos = 0;

            for (size_t chunk_idx = 0; chunk_idx < nodes_chunk_count; ++chunk_idx)
            {
                const size_t nodes_for_this_chunk = std::min(remaining_nodes, chunk_size_limit);

                WriteBufferFromOwnString scratch;
                // Reserve 8 bytes for node_count (backpatched after iteration).
                static const char kZero8[8] = {};
                scratch.write(kZero8, sizeof(kZero8));

                uint64_t actual_node_count = 0;

                while (actual_node_count < nodes_for_this_chunk
                    && container_pos < snapshot.snapshot_container_size)
                {
                    const auto & path = it->key;

                    // Skip system node children of /keeper (they are not persisted).
                    if (Coordination::matchPath(path, keeper_system_path)
                        == Coordination::PathMatchResult::IS_CHILD)
                    {
                        const bool last = (container_pos + 1 >= snapshot.snapshot_container_size);
                        if (!last)
                            ++it;
                        ++container_pos;
                        continue;
                    }

                    const auto & node = it->value;
                    if (node.stats.mzxid > snapshot.zxid)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Chunked snapshot serialize: node mzxid {} > snapshot zxid {}",
                            node.stats.mzxid, snapshot.zxid);

                    writeBinary(path, scratch);
                    // The chunked format reuses V7 per-node encoding (METADATA version byte = 8 is a
                    // chunk tag, not a node-encoding selector). The reader must decode
                    // each node with readNode(…, SnapshotVersion::V7, …).
                    writeNode(node, SnapshotVersion::V7, scratch);
                    ++actual_node_count;

                    const bool last = (container_pos + 1 >= snapshot.snapshot_container_size);
                    if (!last)
                        ++it;
                    ++container_pos;
                }

                remaining_nodes -= actual_node_count;

                // Backpatch node_count into the first 8 bytes of the scratch buffer.
                std::string & s = scratch.str(); // finalises
                memcpy(s.data(), &actual_node_count, sizeof(actual_node_count));
                compress_and_record(SnapshotChunkType::NODES, scratch);
            }
        }

        // ── SESSIONS chunk ────────────────────────────────────────────────────────
        {
            WriteBufferFromOwnString scratch;

            // Sorted sessions for determinism across replicas (same as legacy serialiser).
            std::vector<std::pair<int64_t, int64_t>> sorted_sessions(
                snapshot.session_and_timeout.begin(), snapshot.session_and_timeout.end());
            ::sort(sorted_sessions.begin(), sorted_sessions.end());
            writeBinary(sorted_sessions.size(), scratch);
            for (const auto & [session_id, timeout] : sorted_sessions)
            {
                writeBinary(session_id, scratch);
                writeBinary(timeout, scratch);
                KeeperStorageBase::AuthIDs ids;
                if (snapshot.session_and_auth.contains(session_id))
                    ids = snapshot.session_and_auth.at(session_id);
                writeBinary(ids.size(), scratch);
                for (const auto & [scheme, id] : ids)
                {
                    writeBinary(scheme, scratch);
                    writeBinary(id, scratch);
                }
            }

            // Optional cluster config: absent config is represented by EOF, exactly as in
            // the legacy serialiser (V1-V7). The SESSIONS chunk reader must use
            // `if (!in.eof())` here — never substitute a length-prefix reader for this
            // field, as alloc(0) would feed an empty buffer to ClusterConfig::deserialize.
            if (snapshot.cluster_config)
            {
                auto cluster_buf = snapshot.cluster_config->serialize();
                writeVarUInt(cluster_buf->size(), scratch);
                scratch.write(reinterpret_cast<const char *>(cluster_buf->data_begin()), cluster_buf->size());
            }

            compress_and_record(SnapshotChunkType::SESSIONS, scratch);
        }

        return chunks;
    }
}

template<typename Storage>
void KeeperStorageSnapshot<Storage>::serialize(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context)
{
    if (snapshot.version >= SnapshotVersion::V8)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Chunked snapshots must be written via serializeChunkedSnapshot (which manages per-chunk compression "
            "and header backpatch), not via the legacy streaming serialiser");

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

    /// Serialize cluster config
    if (snapshot.cluster_config)
    {
        auto buffer = snapshot.cluster_config->serialize();
        writeVarUInt(buffer->size(), out);
        out.write(reinterpret_cast<const char *>(buffer->data_begin()), buffer->size());
    }
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

    deserialization_result.snapshot_meta = deserializeSnapshotMetadata(in);
    Storage & storage = *deserialization_result.storage;

    bool recalculate_digest = keeper_context->digestEnabled();
    if (version >= SnapshotVersion::V5)
    {
        readBinary(storage.zxid, in);
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
        storage.zxid = deserialization_result.snapshot_meta->get_last_log_idx();
        storage.old_snapshot_zxid = storage.zxid;
    }

    int64_t session_id = 0;
    readBinary(session_id, in);

    storage.session_id_counter = session_id;

    /// Before V1 we serialized ACL without acl_map
    if (current_version >= SnapshotVersion::V1)
    {
        size_t acls_map_size = 0;

        readBinary(acls_map_size, in);
        size_t current_map_size = 0;
        while (current_map_size < acls_map_size)
        {
            ACLId acl_id = 0;
            if (current_version >= SnapshotVersion::V7)
            {
                readBinary(acl_id, in);
            }
            else
            {
                /// V1-V6 stored acl_id as uint64_t (8 bytes)
                uint64_t acl_id_64 = 0;
                readBinary(acl_id_64, in);
                chassert(acl_id_64 <= std::numeric_limits<ACLId>::max());
                acl_id = static_cast<ACLId>(acl_id_64);
            }

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

            if (!keeper_context->shouldBlockACL())
                storage.acl_map.addMapping(acl_id, acls);
            current_map_size++;
        }
    }

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
        readNode(node, in, current_version, storage.acl_map, keeper_context->shouldBlockACL());

        if (!load_full_storage)
        {
            deserialization_result.paths.push_back(std::string{path});
            continue;
        }

        using enum Coordination::PathMatchResult;
        auto match_result = Coordination::matchPath(path, keeper_system_path);

        const auto get_error_msg = [&]
        {
            return fmt::format("Cannot read node on path {} from a snapshot because it is used as a system node", path);
        };

        if (match_result == IS_CHILD)
        {
            if (keeper_context->ignoreSystemPathOnStartup() || keeper_context->getServerState() != KeeperContext::Phase::INIT)
            {
                LOG_ERROR(getLogger("KeeperSnapshotManager"), "{}. Ignoring it", get_error_msg());
                continue;
            }
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{}. Ignoring it can lead to data loss. "
                "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true",
                get_error_msg());
        }
        if (match_result == EXACT)
        {
            if (!node.empty())
            {
                if (keeper_context->ignoreSystemPathOnStartup() || keeper_context->getServerState() != KeeperContext::Phase::INIT)
                {
                    LOG_ERROR(getLogger("KeeperSnapshotManager"), "{}. Ignoring it", get_error_msg());
                    node = typename Storage::Node{};
                }
                else
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "{}. Ignoring it can lead to data loss. "
                        "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true",
                        get_error_msg());
            }
        }

        auto ephemeral_owner = node.stats.ephemeralOwner();
        if constexpr (!use_rocksdb)
            if (!node.stats.isEphemeral() && node.numChildren() > 0)
                node.getChildren().reserve(node.numChildren());

        if (ephemeral_owner != 0)
        {
            storage.committed_ephemerals[node.stats.ephemeralOwner()].insert(std::string{path});
            ++storage.committed_ephemeral_nodes;
        }

        if (recalculate_digest)
            storage.nodes_digest += node.getDigest(path);

        storage.container.insertOrReplace(std::move(path_data), path_size, std::move(node));
    }

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

    size_t active_sessions_size = 0;
    readBinary(active_sessions_size, in);

    size_t current_session_size = 0;
    while (current_session_size < active_sessions_size)
    {
        int64_t active_session_id = 0;
        int64_t timeout = 0;
        readBinary(active_session_id, in);
        readBinary(timeout, in);
        storage.addSessionID(active_session_id, timeout);

        if (current_version >= SnapshotVersion::V1)
        {
            size_t session_auths_size = 0;
            readBinary(session_auths_size, in);

            typename Storage::AuthIDs ids;
            size_t session_auth_counter = 0;
            while (session_auth_counter < session_auths_size)
            {
                String scheme;
                String id;
                readBinary(scheme, in);
                readBinary(id, in);
                ids.emplace_back(typename Storage::AuthID{scheme, id});

                session_auth_counter++;
            }
            if (!ids.empty())
                storage.committed_session_and_auth[active_session_id] = ids;
        }
        current_session_size++;
    }

    /// Optional cluster config
    ClusterConfigPtr cluster_config = nullptr;
    if (!in.eof())
    {
        size_t data_size = 0;
        readVarUInt(data_size, in);
        auto buffer = nuraft::buffer::alloc(data_size);
        in.readStrict(reinterpret_cast<char *>(buffer->data_begin()), data_size);
        buffer->pos(0);
        deserialization_result.cluster_config = ClusterConfig::deserialize(*buffer);
    }

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
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
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
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
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
    // ── Deserialisation thread pool (C4) ──────────────────────────────────────────────────────
    // Compute effective thread count.  0 → auto (clamp(cores/2, 1, 4)); explicit values clamped to [1, 8].
    // Never created under snapshots_lock — pool construction is safe here.
    // RocksDB managers never read chunked snapshots, so the pool is memory-storage-only.
    if constexpr (!use_rocksdb)
    {
        const auto & cs = keeper_context->getCoordinationSettings();
        const uint64_t raw = static_cast<uint64_t>(cs[CoordinationSetting::snapshot_deser_threads]);
        if (raw == 0)
            deser_threads = std::clamp<size_t>(getNumberOfCPUCoresToUse() / 2, 1, 4);
        else
            deser_threads = std::clamp<size_t>(static_cast<size_t>(raw), 1, 8);

        if (deser_threads > 1)
        {
            // shutdown_on_exception=false: a worker exception must not permanently poison
            // the pool (verified §7.2 / §10 of the design).  wait() rethrows + clears.
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

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx)
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

template<typename Storage>
std::unique_ptr<SnapshotReceiveCtx> KeeperSnapshotManager<Storage>::beginSnapshotReceiveToDisk(uint64_t up_to_log_idx)
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
    if (snapshot.version >= SnapshotVersion::V8)
    {
        // Chunked format: write directly to the nuraft buffer without a compression wrapper.
        // serializeChunkedSnapshot writes a zero-filled header placeholder followed by
        // independently-compressed ZSTD chunks, and returns the chunk descriptor table.
        // We backpatch the real header via memcpy into the contiguous nuraft::buffer.
        auto writer = std::make_unique<WriteBufferFromNuraftBuffer>();
        auto * raw_ptr = writer.get();
        auto chunks = serializeChunkedSnapshot(snapshot, *writer, keeper_context);
        writer->finalize();
        auto buf = raw_ptr->getBuffer();
        const size_t hdr_size = chunkedSnapshotHeaderSize(chunks.size());
        std::vector<char> header_buf(hdr_size);
        packChunkedSnapshotHeader(chunks, header_buf.data());
        std::memcpy(buf->data_begin(), header_buf.data(), hdr_size);
        return buf;
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

template<typename Storage>
SnapshotDeserializationResult<Storage>
KeeperSnapshotManager<Storage>::deserializeChunkedSnapshotFromBuffer(
    nuraft::ptr<nuraft::buffer> buffer, bool load_full_storage) const
{
    if constexpr (use_rocksdb)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Chunked snapshot format is not supported with RocksDB storage");
    }
    else
    {

    const char * buf_data = reinterpret_cast<const char *>(buffer->data_begin());
    const size_t buf_size = buffer->size();

    auto chunks = parseAndValidateChunkedSnapshotHeader(buf_data, buf_size);

    // Chunked snapshot storage: root "/" must come from the snapshot, not the constructor.
    SnapshotDeserializationResult<Storage> result;
    result.storage = std::make_unique<Storage>(
        storage_tick_time, superdigest, keeper_context,
        /* initialize_system_nodes = */ false,
        /* insert_initial_root = */ false);
    Storage & storage = *result.storage;

    bool recalculate_digest = keeper_context->digestEnabled();

    // ── METADATA chunk (always chunks[0], validated by parseAndValidateChunkedSnapshotHeader) ──────────────
    // Streaming decompression (F3): each chunk gets its own ZSTD_DCtx via makeChunkReader,
    // which also verifies the full frame (including the 4-byte content-checksum epilogue) is
    // consumed — matching the integrity guarantee of the superseded one-shot ZSTD_decompress.
    {
        const SnapshotChunkDescriptor & mfd = chunks[0];
        auto zbuf_meta = makeChunkReader(
            buf_data + mfd.compressed_offset,
            static_cast<size_t>(mfd.compressed_size));
        ReadBuffer & rbuf = *zbuf_meta;

        uint8_t version_byte = 0;
        readBinary(version_byte, rbuf);
        if (version_byte != static_cast<uint8_t>(SnapshotVersion::V8))
            throw Exception(
                ErrorCodes::UNKNOWN_FORMAT_VERSION,
                "Chunked snapshot: unexpected version byte {} in METADATA chunk (expected 8)", version_byte);

        result.snapshot_meta = deserializeSnapshotMetadata(rbuf);

        // zxid (always present in chunked snapshot; inherits V5+ semantics)
        {
            int64_t zxid = 0;
            readBinary(zxid, rbuf);
            std::lock_guard lock(storage.transaction_mutex);
            storage.zxid = zxid;
        }
        storage.old_snapshot_zxid = 0;

        // Digest
        uint8_t digest_version = 0;
        readBinary(digest_version, rbuf);
        if (digest_version != static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST))
        {
            uint64_t nodes_digest = 0;
            readBinary(nodes_digest, rbuf);
            if (digest_version == static_cast<uint8_t>(KEEPER_CURRENT_DIGEST_VERSION))
            {
                storage.nodes_digest = nodes_digest;
                recalculate_digest = false;
            }
        }

        // session_id_counter
        int64_t session_id = 0;
        readBinary(session_id, rbuf);
        storage.session_id_counter = session_id;

        // ACL map (chunked snapshot always uses V7+ uint32_t ACLId)
        size_t acl_map_size = 0;
        readBinary(acl_map_size, rbuf);
        for (size_t i = 0; i < acl_map_size; ++i)
        {
            ACLId acl_id = 0;
            readBinary(acl_id, rbuf);
            size_t acls_size = 0;
            readBinary(acls_size, rbuf);
            Coordination::ACLs acls;
            for (size_t j = 0; j < acls_size; ++j)
            {
                Coordination::ACL acl;
                readBinary(acl.permissions, rbuf);
                readBinary(acl.scheme, rbuf);
                readBinary(acl.id, rbuf);
                acls.push_back(acl);
            }
            if (!keeper_context->shouldBlockACL())
                storage.acl_map.addMapping(acl_id, acls);
        }
        // Drain: the METADATA chunk must be fully consumed; trailing bytes indicate format drift.
        if (!rbuf.eof())
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot: trailing bytes after METADATA chunk content");
    }

    if (recalculate_digest)
        storage.nodes_digest = 0;

    const bool cleanup_acl_global = keeper_context->shouldBlockACL();

    // ── NODES chunks ─────────────────────────────────────────────────────────────────────────────
    // Collect chunk descriptors in header order (preserves deterministic splice order).
    std::vector<const SnapshotChunkDescriptor *> nodes_chunks;
    for (const auto & cd : chunks)
        if (cd.type == SnapshotChunkType::NODES)
            nodes_chunks.push_back(&cd);

    const size_t K = nodes_chunks.size();

    // Pre-initialise handles SERIALLY before scheduling any tasks (U1 lifecycle):
    // beginMemorySnapshotLoad touches storage; workers must never touch storage directly.
    std::vector<MemorySnapshotLoadHandle> handles;
    if (load_full_storage)
    {
        handles.reserve(K);
        for (size_t i = 0; i < K; ++i)
            handles.push_back(beginMemorySnapshotLoad(storage));
    }

    // Per-chunk parser — shared by serial and parallel paths.
    // In the parallel path, each worker writes exclusively to handles[f] (different index per task).
    // !load_full_storage is never true in the parallel path (guaranteed by dispatch condition below).
    auto process_nodes_chunk = [&](size_t f, const SnapshotChunkDescriptor & cd)
    {
        auto zbuf = makeChunkReader(
            buf_data + cd.compressed_offset,
            static_cast<size_t>(cd.compressed_size));
        ReadBuffer & rbuf = *zbuf;

        uint64_t node_count = 0;
        readBinary(node_count, rbuf);

        if (!load_full_storage)
        {
            // Analyzer (path-only) mode: not reached from the parallel path.
            for (uint64_t i = 0; i < node_count; ++i)
            {
                String path;
                readBinary(path, rbuf);
                result.paths.push_back(path);
                typename Storage::Node node{};
                readChunkedSnapshotNode(node, rbuf, /*cleanup_acl=*/true);
            }
            if (!rbuf.eof())
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Chunked snapshot: trailing bytes after NODES chunk content (analyzer mode)");
            return;
        }

        // Full-load mode: fill the pre-initialised handle for this frame index.
        MemorySnapshotLoadHandle & h = handles[f];
        for (uint64_t i = 0; i < node_count; ++i)
        {
            String path_str;
            readBinary(path_str, rbuf);

            using enum Coordination::PathMatchResult;
            const auto match = Coordination::matchPath(path_str, keeper_system_path);

            typename Storage::Node node{};
            readChunkedSnapshotNode(node, rbuf, cleanup_acl_global);

            if (match == IS_CHILD)
            {
                // Chunked snapshot serializer skips system children; presence here means corrupt snapshot.
                if (keeper_context->ignoreSystemPathOnStartup()
                    || keeper_context->getServerState() != KeeperContext::Phase::INIT)
                {
                    LOG_ERROR(
                        getLogger("KeeperSnapshotManager"),
                        "System-path child {} found in chunked snapshot — skipping", path_str);
                    continue;
                }
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "System-path child {} found in chunked snapshot. "
                    "Set keeper_server.ignore_system_path_on_startup=true to ignore.", path_str);
            }
            if (match == EXACT && !node.empty())
            {
                if (keeper_context->ignoreSystemPathOnStartup()
                    || keeper_context->getServerState() != KeeperContext::Phase::INIT)
                {
                    LOG_ERROR(
                        getLogger("KeeperSnapshotManager"),
                        "Non-empty keeper system node {} found in chunked snapshot — clearing data", path_str);
                    node = typename Storage::Node{};
                }
                else
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Non-empty keeper system node {} found in chunked snapshot. "
                        "Set keeper_server.ignore_system_path_on_startup=true to ignore.", path_str);
            }

            // Pre-reserve child set for non-ephemeral nodes (avoids per-addChild re-alloc).
            if (!node.stats.isEphemeral() && node.numChildren() > 0)
                node.getChildren().reserve(node.numChildren());

            const auto ephemeral_owner = node.stats.ephemeralOwner();
            if (ephemeral_owner != 0)
            {
                h.local_ephemerals[ephemeral_owner].insert(path_str);
                ++h.local_ephemeral_nodes;
            }

            if (recalculate_digest)
                h.digest_sum += node.getDigest(path_str);

            if (!cleanup_acl_global && node.acl_id != 0)
                ++h.acl_usage[node.acl_id];

            // Allocate the path key in the shared arena and enqueue in the batch.
            // LocalInsertBatch::allocateKey uses new char[] — thread-safe (GlobalArena comment).
            const size_t path_size = path_str.size();
            auto key = h.nodes.allocateKey(path_size);
            std::memcpy(key.get(), path_str.data(), path_size);
            h.nodes.emplace(std::move(key), path_size, std::move(node));
        }

        if (!rbuf.eof())
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot: trailing bytes after NODES chunk content");
    };

    // Dispatch: parallel for full-load with >=2 threads and >=2 chunks; serial otherwise.
    if (load_full_storage && deser_pool.has_value() && K > 1)
    {
        // Capture the calling thread's group so workers can inherit it for metrics and naming.
        auto tg = getCurrentThreadGroup();
        try
        {
            for (size_t f = 0; f < K; ++f)
            {
                // Copy the chunk descriptor for lambda safety (lives on the calling stack).
                const SnapshotChunkDescriptor cd_copy = *nodes_chunks[f];
                deser_pool->scheduleOrThrowOnError(
                    [f, cd_copy, &process_nodes_chunk, tg]()
                    {
                        ThreadGroupSwitcher switcher(tg, ThreadName::KEEPER_SNAPSHOT_LOAD);
                        process_nodes_chunk(f, cd_copy);
                    });
            }
            deser_pool->wait();
        }
        catch (...)
        {
            // Drain pool before handles / process_nodes_chunk lambda go out of scope.
            deser_pool->wait();
            throw;
        }
    }
    else
    {
        // Serial path — also handles !load_full_storage (analyzer mode, not performance-critical).
        for (size_t f = 0; f < K; ++f)
            process_nodes_chunk(f, *nodes_chunks[f]);
    }

    if (load_full_storage)
    {
        finalizeMemorySnapshotLoad(storage, {handles.data(), handles.size()}, recalculate_digest);
        storage.initializeSystemNodes();
    }

    // ── SESSIONS chunk ───────────────────────────────────────────────────────────────────────────
    for (const auto & cd : chunks)
    {
        if (cd.type != SnapshotChunkType::SESSIONS)
            continue;

        auto zbuf_sess = makeChunkReader(
            buf_data + cd.compressed_offset,
            static_cast<size_t>(cd.compressed_size));
        ReadBuffer & rbuf = *zbuf_sess;

        size_t active_sessions_size = 0;
        readBinary(active_sessions_size, rbuf);
        for (size_t i = 0; i < active_sessions_size; ++i)
        {
            int64_t active_session_id = 0;
            int64_t timeout = 0;
            readBinary(active_session_id, rbuf);
            readBinary(timeout, rbuf);
            storage.addSessionID(active_session_id, timeout);

            // Chunked snapshot always includes session auth (inherits V1+ semantics).
            size_t session_auths_size = 0;
            readBinary(session_auths_size, rbuf);
            typename Storage::AuthIDs ids;
            for (size_t j = 0; j < session_auths_size; ++j)
            {
                String scheme;
                String id;
                readBinary(scheme, rbuf);
                readBinary(id, rbuf);
                ids.emplace_back(typename Storage::AuthID{scheme, id});
            }
            if (!ids.empty())
                storage.committed_session_and_auth[active_session_id] = ids;
        }

        // Optional cluster config: same encoding as legacy (EOF = absent, length-prefix = present).
        if (!rbuf.eof())
        {
            size_t data_size = 0;
            readVarUInt(data_size, rbuf);
            auto cluster_buf = nuraft::buffer::alloc(data_size);
            rbuf.readStrict(reinterpret_cast<char *>(cluster_buf->data_begin()), data_size);
            cluster_buf->pos(0);
            result.cluster_config = ClusterConfig::deserialize(*cluster_buf);
        }
        // Drain: any bytes after the cluster-config section indicate format corruption.
        if (!rbuf.eof())
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot: trailing bytes after SESSIONS chunk content");
        break; // Exactly one SESSIONS chunk.
    }

    if (load_full_storage)
        storage.updateStats();

    return result;

    } // end else (!use_rocksdb)
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

/// Returns true iff `buffer` starts with "CKFS" (chunked snapshot magic).
static bool isChunkedSnapshot(nuraft::ptr<nuraft::buffer> buffer)
{
    if (buffer->size() < 4)
        return false;
    const char * p = reinterpret_cast<const char *>(buffer->data_begin());
    return std::memcmp(p, KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4) == 0;
}

template<typename Storage>
SnapshotDeserializationResult<Storage> KeeperSnapshotManager<Storage>::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer, bool load_full_storage) const
{
    buffer->pos(0);

    // 3-way detection:
    //   "CKFS" → chunked ZSTD format (version 8)
    //   ZSTD magic → legacy ZSTD (V3-V7)
    //   else    → legacy LZ4 (V0-V2)
    if (isChunkedSnapshot(buffer))
        return deserializeChunkedSnapshotFromBuffer(buffer, load_full_storage);

    bool is_zstd_compressed = isZstdCompressed(buffer);

    std::unique_ptr<ReadBufferFromNuraftBuffer> reader = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);
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

/// Read and discard the METADATA tail that follows SnapshotMetadata in a chunked snapshot METADATA chunk:
/// zxid, digest fields, session_id_counter, and the ACL map.  Called from the metadata-only
/// fast path to validate full chunk layout without populating storage.
///
/// IMPORTANT: field order here must stay in sync with the METADATA parsing block inside
/// deserializeChunkedSnapshotFromBuffer (search for "// zxid (always present in chunked snapshot").
static void drainChunkedSnapshotMetadataChunkTail(ReadBuffer & rbuf)
{
    // zxid
    {
        int64_t zxid = 0;
        readBinary(zxid, rbuf);
    }

    // Digest
    {
        uint8_t digest_version = 0;
        readBinary(digest_version, rbuf);
        if (digest_version != static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST))
        {
            uint64_t nodes_digest = 0;
            readBinary(nodes_digest, rbuf);
        }
    }

    // session_id_counter
    {
        int64_t session_id = 0;
        readBinary(session_id, rbuf);
    }

    // ACL map
    {
        size_t acl_map_size = 0;
        readBinary(acl_map_size, rbuf);
        for (size_t i = 0; i < acl_map_size; ++i)
        {
            ACLId acl_id = 0;
            readBinary(acl_id, rbuf);
            size_t acls_size = 0;
            readBinary(acls_size, rbuf);
            for (size_t j = 0; j < acls_size; ++j)
            {
                int32_t permissions = 0;
                String scheme;
                String id;
                readBinary(permissions, rbuf);
                readBinary(scheme, rbuf);
                readBinary(id, rbuf);
            }
        }
    }

    // Trailing bytes after the ACL map indicate format drift or corruption.
    if (!rbuf.eof())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Chunked snapshot: trailing bytes after METADATA chunk content");
}

/// Decompress ONLY the METADATA chunk (chunks[0]) of a chunked snapshot and return the snapshot
/// metadata. Never touches NODES or SESSIONS chunks — O(1) in node count.
/// Called from deserializeSnapshotMetadataFromBuffer after "CKFS" magic is detected.
static SnapshotMetadataPtr deserializeChunkedSnapshotMetadataFromBuffer(nuraft::ptr<nuraft::buffer> buffer)
{
    const char * buf_data = reinterpret_cast<const char *>(buffer->data_begin());
    const size_t buf_size = buffer->size();

    auto chunks = parseAndValidateChunkedSnapshotHeader(buf_data, buf_size);

    // Decompress ONLY chunks[0] (METADATA) — intentionally never touch NODES or SESSIONS.
    const SnapshotChunkDescriptor & meta_cd = chunks[0];
    auto zbuf_meta = makeChunkReader(
        buf_data + meta_cd.compressed_offset,
        static_cast<size_t>(meta_cd.compressed_size));
    ReadBuffer & rbuf = *zbuf_meta;
    uint8_t version_byte = 0;
    readBinary(version_byte, rbuf);
    if (version_byte != static_cast<uint8_t>(SnapshotVersion::V8))
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Chunked snapshot: unexpected version byte {} in METADATA chunk (expected 8)", version_byte);

    auto snapshot_meta = deserializeSnapshotMetadata(rbuf);
    drainChunkedSnapshotMetadataChunkTail(rbuf); // Validate full chunk layout; values discarded.
    return snapshot_meta;
}

template<typename Storage>
SnapshotMetadataPtr KeeperSnapshotManager<Storage>::deserializeSnapshotMetadataFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const
{
    /// `nuraft::buffer::pos(0)` resets the cursor. This method must leave the
    /// buffer at offset `0` on success and on throw.
    SCOPE_EXIT({ buffer->pos(0); });

    // Chunked snapshot: snapshot_meta lives in the METADATA chunk only.
    // Use the dedicated fast path that decompresses only chunks[0] — O(1) in node count.
    // The full deserializer (deserializeChunkedSnapshotFromBuffer) would process all NODES and SESSIONS
    // chunks, which at 56M-node scale allocates gigabytes of temporary data while the
    // old storage is still live — violating the three-phase peak-memory design.
    if (isChunkedSnapshot(buffer))
        return deserializeChunkedSnapshotMetadataFromBuffer(buffer);

    bool is_zstd_compressed = isZstdCompressed(buffer);

    std::unique_ptr<ReadBufferFromNuraftBuffer> reader = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);
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

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::registerSnapshotFile(uint64_t log_idx, const SnapshotFileInfoPtr & snapshot_file_info)
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

template<typename Storage>
void KeeperSnapshotManager<Storage>::removeOutdatedSnapshotsIfNeeded(uint64_t just_written_log_idx)
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

template<typename Storage>
void KeeperSnapshotManager<Storage>::moveSnapshotsIfNeeded()
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

template<typename Storage>
void KeeperSnapshotManager<Storage>::removeSnapshot(uint64_t log_idx)
{
    auto itr = existing_snapshots.find(log_idx);
    if (itr == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);

    /// Mark before erasing so the deleter unlinks after the last pin is released.
    itr->second->retired_for_removal.store(true, std::memory_order_release);
    existing_snapshots.erase(itr);
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::serializeSnapshotToDisk(const KeeperStorageSnapshot<Storage> & snapshot)
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

        if (snapshot.version >= SnapshotVersion::V8)
        {
            // Chunked format: write directly without a compression wrapper; serializeChunkedSnapshot
            // manages its own per-chunk ZSTD compression. After all chunks are written we
            // capture the total byte count (U3: before seeking back so the metric
            // reflects the real file size, not the post-seek position), then seek to
            // position 0 and overwrite the placeholder header.
            auto * raw_writer = writer.get();
            auto chunks = serializeChunkedSnapshot(snapshot, *raw_writer, keeper_context);

            // U3: capture total bytes BEFORE seeking back to avoid recording the
            // seek-back position (0 + header_size) instead of the real file size.
            const size_t bytes_written = raw_writer->count();
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotWrittenBytes, bytes_written);

            raw_writer->next(); // flush internal buffer to OS before seeking

            auto * seekable = dynamic_cast<WriteBufferFromFileDescriptor *>(raw_writer);
            if (!seekable)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Chunked snapshot disk writer does not support seek — cannot backpatch header");
            seekable->seek(0, SEEK_SET);

            const size_t hdr_size = chunkedSnapshotHeaderSize(chunks.size());
            std::vector<char> header_buf(hdr_size);
            packChunkedSnapshotHeader(chunks, header_buf.data());
            raw_writer->write(header_buf.data(), hdr_size);
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
