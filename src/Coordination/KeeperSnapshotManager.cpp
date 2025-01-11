#include <filesystem>
#include <memory>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Core/Field.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
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
        Strings name_parts;
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

    template<typename Node>
    void writeNode(const Node & node, SnapshotVersion version, WriteBuffer & out)
    {
        writeBinary(node.getData(), out);

        /// Serialize ACL
        writeBinary(node.acl_id, out);
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
        writeBinary(node.stats.numChildren(), out);
        writeBinary(node.stats.pzxid, out);

        writeBinary(node.stats.seqNum(), out);

        if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
            writeBinary(node.sizeInBytes(), out);
    }

    template<typename Node>
    void readNode(Node & node, ReadBuffer & in, SnapshotVersion version, ACLMap & acl_map)
    {
        readVarUInt(node.stats.data_size, in);
        if (node.stats.data_size != 0)
        {
            node.data = std::unique_ptr<char[]>(new char[node.stats.data_size]);
            in.readStrict(node.data.get(), node.stats.data_size);
        }

        if (version >= SnapshotVersion::V1)
        {
            readBinary(node.acl_id, in);
        }
        else if (version == SnapshotVersion::V0)
        {
            /// Deserialize ACL
            size_t acls_size;
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
            node.acl_id = acl_map.convertACLs(acls);
        }

        /// Some strange ACLID during deserialization from ZooKeeper
        if (node.acl_id == std::numeric_limits<uint64_t>::max())
            node.acl_id = 0;

        acl_map.addUsage(node.acl_id);

        if (version < SnapshotVersion::V6)
        {
            bool is_sequential = false;
            readBinary(is_sequential, in);
        }

        /// Deserialize stat
        readBinary(node.stats.czxid, in);
        readBinary(node.stats.mzxid, in);
        int64_t ctime;
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
        if (ephemeral_owner == 0)
            node.stats.setNumChildren(num_children);

        readBinary(node.stats.pzxid, in);

        int32_t seq_num = 0;
        readBinary(seq_num, in);
        if (ephemeral_owner == 0)
            node.stats.setSeqNum(seq_num);

        if (version >= SnapshotVersion::V4 && version <= SnapshotVersion::V5)
        {
            uint64_t size_bytes = 0;
            readBinary(size_bytes, in);
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
        size_t data_size;
        readVarUInt(data_size, in);
        auto buffer = nuraft::buffer::alloc(data_size);
        in.readStrict(reinterpret_cast<char *>(buffer->data_begin()), data_size);
        buffer->pos(0);
        return SnapshotMetadata::deserialize(*buffer);
    }
}

template<typename Storage>
void KeeperStorageSnapshot<Storage>::serialize(const KeeperStorageSnapshot<Storage> & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context)
{
    writeBinary(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);

    if (snapshot.version >= SnapshotVersion::V5)
    {
        writeBinary(snapshot.zxid, out);
        if (keeper_context->digestEnabled())
        {
            writeBinary(static_cast<uint8_t>(Storage::CURRENT_DIGEST_VERSION), out);
            writeBinary(snapshot.nodes_digest, out);
        }
        else
            writeBinary(static_cast<uint8_t>(Storage::NO_DIGEST), out);
    }

    writeBinary(snapshot.session_id, out);

    /// Better to sort before serialization, otherwise snapshots can be different on different replicas
    std::vector<std::pair<int64_t, Coordination::ACLs>> sorted_acl_map(snapshot.acl_map.begin(), snapshot.acl_map.end());
    ::sort(sorted_acl_map.begin(), sorted_acl_map.end());
    /// Serialize ACLs map
    writeBinary(sorted_acl_map.size(), out);
    for (const auto & [acl_id, acls] : sorted_acl_map)
    {
        writeBinary(acl_id, out);
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
        if (Coordination::matchPath(path.toView(), keeper_system_path) == Coordination::PathMatchResult::IS_CHILD)
        {
            if (counter == snapshot.snapshot_container_size - 1)
                break;

            ++it;
            continue;
        }

        const auto & node = it->value;

        /// Benign race condition possible while taking snapshot: NuRaft decide to create snapshot at some log id
        /// and only after some time we lock storage and enable snapshot mode. So snapshot_container_size can be
        /// slightly bigger than required.
        if (node.stats.mzxid > snapshot.zxid)
            break;
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

template<typename Storage>
void KeeperStorageSnapshot<Storage>::deserialize(SnapshotDeserializationResult<Storage> & deserialization_result, ReadBuffer & in, KeeperContextPtr keeper_context) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    uint8_t version;
    readBinary(version, in);
    SnapshotVersion current_version = static_cast<SnapshotVersion>(version);
    if (current_version > CURRENT_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

    deserialization_result.snapshot_meta = deserializeSnapshotMetadata(in);
    Storage & storage = *deserialization_result.storage;

    bool recalculate_digest = keeper_context->digestEnabled();
    if (version >= SnapshotVersion::V5)
    {
        readBinary(storage.zxid, in);
        uint8_t digest_version;
        readBinary(digest_version, in);
        if (digest_version != Storage::DigestVersion::NO_DIGEST)
        {
            uint64_t nodes_digest;
            readBinary(nodes_digest, in);
            if (digest_version == Storage::CURRENT_DIGEST_VERSION)
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

    int64_t session_id;
    readBinary(session_id, in);

    storage.session_id_counter = session_id;

    /// Before V1 we serialized ACL without acl_map
    if (current_version >= SnapshotVersion::V1)
    {
        size_t acls_map_size;

        readBinary(acls_map_size, in);
        size_t current_map_size = 0;
        while (current_map_size < acls_map_size)
        {
            uint64_t acl_id;
            readBinary(acl_id, in);

            size_t acls_size;
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
            storage.acl_map.addMapping(acl_id, acls);
            current_map_size++;
        }
    }

    size_t snapshot_container_size;
    readBinary(snapshot_container_size, in);
    if constexpr (!use_rocksdb)
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

        typename Storage::Node node{};
        readNode(node, in, current_version, storage.acl_map);

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
            if (!node.stats.isEphemeral() && node.stats.numChildren() > 0)
                node.getChildren().reserve(node.stats.numChildren());

        if (ephemeral_owner != 0)
            storage.committed_ephemerals[node.stats.ephemeralOwner()].insert(std::string{path});

        if (recalculate_digest)
            storage.nodes_digest += node.getDigest(path);

        storage.container.insertOrReplace(std::move(path_data), path_size, std::move(node));
    }

    LOG_TRACE(getLogger("KeeperSnapshotManager"), "Building structure for children nodes");

    if constexpr (!use_rocksdb)
    {
        for (const auto & itr : storage.container)
        {
            if (itr.key != "/")
            {
                auto parent_path = parentNodePath(itr.key);
                storage.container.updateValue(
                    parent_path, [path = itr.key](typename Storage::Node & value) { value.addChild(getBaseNodeName(path)); });
            }
        }

        for (const auto & itr : storage.container)
        {
            if (itr.key != "/")
            {
                if (itr.value.stats.numChildren() != static_cast<int32_t>(itr.value.getChildren().size()))
                {
#ifdef NDEBUG
                    /// TODO (alesapin) remove this, it should be always CORRUPTED_DATA.
                    LOG_ERROR(
                        getLogger("KeeperSnapshotManager"),
                        "Children counter in stat.numChildren {}"
                        " is different from actual children size {} for node {}",
                        itr.value.stats.numChildren(),
                        itr.value.getChildren().size(),
                        itr.key);
#else
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Children counter in stat.numChildren {}"
                        " is different from actual children size {} for node {}",
                        itr.value.stats.numChildren(),
                        itr.value.getChildren().size(),
                        itr.key);
#endif
                }
            }
        }
    }

    size_t active_sessions_size;
    readBinary(active_sessions_size, in);

    size_t current_session_size = 0;
    while (current_session_size < active_sessions_size)
    {
        int64_t active_session_id, timeout;
        readBinary(active_session_id, in);
        readBinary(timeout, in);
        storage.addSessionID(active_session_id, timeout);

        if (current_version >= SnapshotVersion::V1)
        {
            size_t session_auths_size;
            readBinary(session_auths_size, in);

            typename Storage::AuthIDs ids;
            size_t session_auth_counter = 0;
            while (session_auth_counter < session_auths_size)
            {
                String scheme, id;
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
        size_t data_size;
        readVarUInt(data_size, in);
        auto buffer = nuraft::buffer::alloc(data_size);
        in.readStrict(reinterpret_cast<char *>(buffer->data_begin()), data_size);
        buffer->pos(0);
        deserialization_result.cluster_config = ClusterConfig::deserialize(*buffer);
    }

    storage.updateStats();
}

template<typename Storage>
KeeperStorageSnapshot<Storage>::KeeperStorageSnapshot(Storage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_)
    : storage(storage_)
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
    Storage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_)
    : storage(storage_)
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
            auto [_, inserted] = existing_snapshots.insert_or_assign(snapshot_up_to, std::make_shared<SnapshotFileInfo>(snapshot_file, disk));

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

    removeOutdatedSnapshotsIfNeeded();
    moveSnapshotsIfNeeded();
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx)
{
    ReadBufferFromNuraftBuffer reader(buffer);

    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);
    auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;

    auto disk = getLatestSnapshotDisk();

    {
        auto buf = disk->writeFile(tmp_snapshot_file_name);
        buf->finalize();
    }

    auto plain_buf = disk->writeFile(snapshot_file_name);
    copyData(reader, *plain_buf);
    plain_buf->sync();
    plain_buf->finalize();

    disk->removeFile(tmp_snapshot_file_name);

    auto snapshot_file_info = std::make_shared<SnapshotFileInfo>(snapshot_file_name, disk);
    existing_snapshots.emplace(up_to_log_idx, snapshot_file_info);
    removeOutdatedSnapshotsIfNeeded();
    moveSnapshotsIfNeeded();

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
            const auto & [path, disk, size] = *latest_itr->second;
            disk->removeFile(path);
            existing_snapshots.erase(latest_itr->first);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    return nullptr;
}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperSnapshotManager<Storage>::deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const
{
    const auto & [snapshot_path, snapshot_disk, size] = *existing_snapshots.at(up_to_log_idx);
    WriteBufferFromNuraftBuffer writer;
    auto reader = snapshot_disk->readFile(snapshot_path, getReadSettings());
    copyData(*reader, writer);
    return writer.getBuffer();
}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperSnapshotManager<Storage>::serializeSnapshotToBuffer(const KeeperStorageSnapshot<Storage> & snapshot) const
{
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
bool KeeperSnapshotManager<Storage>::isZstdCompressed(nuraft::ptr<nuraft::buffer> buffer)
{
    static constexpr unsigned char ZSTD_COMPRESSED_MAGIC[4] = {0x28, 0xB5, 0x2F, 0xFD};

    ReadBufferFromNuraftBuffer reader(buffer);
    unsigned char magic_from_buffer[4]{};
    reader.readStrict(reinterpret_cast<char *>(&magic_from_buffer), sizeof(magic_from_buffer));
    buffer->pos(0);
    return memcmp(magic_from_buffer, ZSTD_COMPRESSED_MAGIC, 4) == 0;
}

template<typename Storage>
SnapshotDeserializationResult<Storage> KeeperSnapshotManager<Storage>::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const
{
    bool is_zstd_compressed = isZstdCompressed(buffer);

    std::unique_ptr<ReadBufferFromNuraftBuffer> reader = std::make_unique<ReadBufferFromNuraftBuffer>(buffer);
    std::unique_ptr<ReadBuffer> compressed_reader;

    if (is_zstd_compressed)
        compressed_reader = wrapReadBufferWithCompressionMethod(std::move(reader), CompressionMethod::Zstd);
    else
        compressed_reader = std::make_unique<CompressedReadBuffer>(*reader);

    SnapshotDeserializationResult<Storage> result;
    result.storage = std::make_unique<Storage>(storage_tick_time, superdigest, keeper_context, /* initialize_system_nodes */ false);
    KeeperStorageSnapshot<Storage>::deserialize(result, *compressed_reader, keeper_context);
    result.storage->initializeSystemNodes();
    return result;
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
void KeeperSnapshotManager<Storage>::removeOutdatedSnapshotsIfNeeded()
{
    while (existing_snapshots.size() > snapshots_to_keep)
        removeSnapshot(existing_snapshots.begin()->first);
}

template<typename Storage>
void KeeperSnapshotManager<Storage>::moveSnapshotsIfNeeded()
{
    /// move snapshots to correct disks

    auto disk = getDisk();
    auto latest_snapshot_disk = getLatestSnapshotDisk();
    auto latest_snapshot_idx = getLatestSnapshotIndex();

    for (auto & [idx, file_info] : existing_snapshots)
    {
        if (idx == latest_snapshot_idx)
        {
            if (file_info->disk != latest_snapshot_disk)
            {
                moveSnapshotBetweenDisks(file_info->disk, file_info->path, latest_snapshot_disk, file_info->path, keeper_context);
                file_info->disk = latest_snapshot_disk;
            }
        }
        else
        {
            if (file_info->disk != disk)
            {
                moveSnapshotBetweenDisks(file_info->disk, file_info->path, disk, file_info->path, keeper_context);
                file_info->disk = disk;
            }
        }
    }

}

template<typename Storage>
void KeeperSnapshotManager<Storage>::removeSnapshot(uint64_t log_idx)
{
    auto itr = existing_snapshots.find(log_idx);
    if (itr == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);
    const auto & [path, disk, size] = *itr->second;
    disk->removeFileIfExists(path);
    existing_snapshots.erase(itr);
}

template<typename Storage>
SnapshotFileInfoPtr KeeperSnapshotManager<Storage>::serializeSnapshotToDisk(const KeeperStorageSnapshot<Storage> & snapshot)
{
    auto up_to_log_idx = snapshot.snapshot_meta->get_last_log_idx();
    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx, compress_snapshots_zstd);
    auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;

    auto disk = getLatestSnapshotDisk();
    {
        auto buf = disk->writeFile(tmp_snapshot_file_name);
        buf->finalize();
    }

    auto writer = disk->writeFile(snapshot_file_name);
    std::unique_ptr<WriteBuffer> compressed_writer;
    if (compress_snapshots_zstd)
        compressed_writer = wrapWriteBufferWithCompressionMethod(std::move(writer), CompressionMethod::Zstd, 3);
    else
        compressed_writer = std::make_unique<CompressedWriteBuffer>(*writer);

    KeeperStorageSnapshot<Storage>::serialize(snapshot, *compressed_writer, keeper_context);
    compressed_writer->finalize();
    compressed_writer->sync();

    disk->removeFile(tmp_snapshot_file_name);

    auto snapshot_file_info = std::make_shared<SnapshotFileInfo>(snapshot_file_name, disk);
    existing_snapshots.emplace(up_to_log_idx, snapshot_file_info);

    try
    {
        removeOutdatedSnapshotsIfNeeded();
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
    if (!existing_snapshots.empty())
    {
        const auto & [path, disk, size] = *existing_snapshots.at(getLatestSnapshotIndex());

        try
        {
            if (disk->existsFile(path))
                return std::make_shared<SnapshotFileInfo>(path, disk);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
    return nullptr;
}

template struct KeeperStorageSnapshot<KeeperMemoryStorage>;
template class KeeperSnapshotManager<KeeperMemoryStorage>;
#if USE_ROCKSDB
template struct KeeperStorageSnapshot<KeeperRocksStorage>;
template class KeeperSnapshotManager<KeeperRocksStorage>;
#endif
}
