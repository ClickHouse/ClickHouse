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
#include <base/scope_guard.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
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

    template<typename Node>
    void readNode(Node & node, ReadBuffer & in, SnapshotVersion version, ACLMap & acl_map, bool cleanup_acl)
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

            if (!cleanup_acl)
            {
                node.acl_id = acl_map.convertACLs(acls);
                add_usage = false;
            }
        }

        if (add_usage)
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
}

void KeeperStorageSnapshot::serialize(const KeeperStorageSnapshot & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context)
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

void KeeperStorageSnapshot::deserialize(
    SnapshotDeserializationResult & deserialization_result,
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
    KeeperStorage & storage = *deserialization_result.storage;

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
    storage.container.reserve(snapshot_container_size);

    if (recalculate_digest)
        storage.nodes_digest = 0;

    for (size_t nodes_read = 0; nodes_read < snapshot_container_size; ++nodes_read)
    {
        size_t path_size = 0;
        readVarUInt(path_size, in);
        chassert(path_size != 0);
        auto path_data = storage.container.allocateKey(path_size);
        in.readStrict(path_data.get(), path_size);
        std::string_view path{path_data.get(), path_size};

        typename KeeperStorage::Node node{};
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
                    node = typename KeeperStorage::Node{};
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
        if (!node.stats.isEphemeral() && node.numChildren() > 0)
            node.getChildren().reserve(node.numChildren());

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

    {
        LOG_TRACE(getLogger("KeeperSnapshotManager"), "Building structure for children nodes");

        for (const auto & itr : storage.container)
        {
            if (itr.key != "/")
            {
                auto parent_path = Coordination::parentNodePath(itr.key);
                storage.container.updateValue(
                    parent_path, [path = itr.key](typename KeeperStorage::Node & value) { value.addChild(Coordination::getBaseNodeName(path)); });
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

            typename KeeperStorage::AuthIDs ids;
            size_t session_auth_counter = 0;
            while (session_auth_counter < session_auths_size)
            {
                String scheme;
                String id;
                readBinary(scheme, in);
                readBinary(id, in);
                ids.emplace_back(typename KeeperStorage::AuthID{scheme, id});

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
    scope_guard snapshot_mode_guard([&] { storage->disableSnapshotMode(); });
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
    snapshot_mode_guard.release();
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
    scope_guard snapshot_mode_guard([&] { storage->disableSnapshotMode(); });
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
    acl_map = storage->acl_map.getMapping();
    session_and_auth = storage->committed_session_and_auth;
    snapshot_mode_guard.release();
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

KeeperSnapshotManager::KeeperSnapshotManager(
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

SnapshotDeserializationResult KeeperSnapshotManager::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer, bool load_full_storage) const
{
    bool is_zstd_compressed = isZstdCompressed(buffer);

    std::unique_ptr<ReadBufferFromNuraftBuffer> reader = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);
    std::unique_ptr<ReadBuffer> compressed_reader;

    if (is_zstd_compressed)
        compressed_reader = wrapReadBufferWithCompressionMethod(std::move(reader), CompressionMethod::Zstd);
    else
        compressed_reader = std::make_unique<CompressedReadBuffer>(*reader);

    SnapshotDeserializationResult result;
    result.storage = std::make_unique<KeeperStorage>(storage_tick_time, superdigest, keeper_context, /* initialize_system_nodes */ false);
    KeeperStorageSnapshot::deserialize(result, *compressed_reader, keeper_context, load_full_storage);
    if (load_full_storage)
        result.storage->initializeSystemNodes();
    return result;
}

SnapshotMetadataPtr KeeperSnapshotManager::deserializeSnapshotMetadataFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const
{
    /// `nuraft::buffer::pos(0)` resets the cursor. This method must leave the
    /// buffer at offset `0` on success and on throw.
    SCOPE_EXIT({ buffer->pos(0); });

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

SnapshotDeserializationResult KeeperSnapshotManager::restoreFromLatestSnapshot()
{
    if (existing_snapshots.empty())
        return {};

    auto buffer = deserializeLatestSnapshotBufferFromDisk();
    if (!buffer)
        return {};
    return deserializeSnapshotFromBuffer(buffer);
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

}
