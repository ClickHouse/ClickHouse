#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <filesystem>
#include <memory>
#include <Common/logger_useful.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/pathUtils.h>
#include <Coordination/KeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include "Core/Field.h"
#include <Disks/DiskLocal.h>


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
    constexpr std::string_view tmp_prefix = "tmp_";

    void moveFileBetweenDisks(DiskPtr disk_from, const std::string & path_from, DiskPtr disk_to, const std::string & path_to)
    {
        /// we use empty file with prefix tmp_ to detect incomplete copies
        /// if a copy is complete we don't care from which disk we use the same file
        /// so it's okay if a failure happens after removing of tmp file but before we remove
        /// the snapshot from the source disk
        auto from_path = fs::path(path_from);
        auto tmp_snapshot_name = from_path.parent_path() / (std::string{tmp_prefix} + from_path.filename().string());
        {
            auto buf = disk_to->writeFile(tmp_snapshot_name);
            buf->finalize();
        }
        disk_from->copyFile(from_path, *disk_to, path_to, {});
        disk_to->removeFile(tmp_snapshot_name);
        disk_from->removeFile(path_from);
    }

    uint64_t getSnapshotPathUpToLogIdx(const String & snapshot_path)
    {
        std::filesystem::path path(snapshot_path);
        std::string filename = path.stem();
        Strings name_parts;
        splitInto<'_'>(name_parts, filename);
        return parse<uint64_t>(name_parts[1]);
    }

    std::string getSnapshotFileName(uint64_t up_to_log_idx, bool compress_zstd)
    {
        auto base = fmt::format("snapshot_{}.bin", up_to_log_idx);
        if (compress_zstd)
            base += ".zstd";
        return base;
    }

    void writeNode(const KeeperStorage::Node & node, SnapshotVersion version, WriteBuffer & out)
    {
        writeBinary(node.getData(), out);

        /// Serialize ACL
        writeBinary(node.acl_id, out);
        writeBinary(node.is_sequental, out);
        /// Serialize stat
        writeBinary(node.stat.czxid, out);
        writeBinary(node.stat.mzxid, out);
        writeBinary(node.stat.ctime, out);
        writeBinary(node.stat.mtime, out);
        writeBinary(node.stat.version, out);
        writeBinary(node.stat.cversion, out);
        writeBinary(node.stat.aversion, out);
        writeBinary(node.stat.ephemeralOwner, out);
        writeBinary(node.stat.dataLength, out);
        writeBinary(node.stat.numChildren, out);
        writeBinary(node.stat.pzxid, out);

        writeBinary(node.seq_num, out);

        if (version >= SnapshotVersion::V4)
        {
            writeBinary(node.size_bytes, out);
        }
    }

    void readNode(KeeperStorage::Node & node, ReadBuffer & in, SnapshotVersion version, ACLMap & acl_map)
    {
        String new_data;
        readBinary(new_data, in);
        node.setData(std::move(new_data));

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

        readBinary(node.is_sequental, in);

        /// Deserialize stat
        readBinary(node.stat.czxid, in);
        readBinary(node.stat.mzxid, in);
        readBinary(node.stat.ctime, in);
        readBinary(node.stat.mtime, in);
        readBinary(node.stat.version, in);
        readBinary(node.stat.cversion, in);
        readBinary(node.stat.aversion, in);
        readBinary(node.stat.ephemeralOwner, in);
        readBinary(node.stat.dataLength, in);
        readBinary(node.stat.numChildren, in);
        readBinary(node.stat.pzxid, in);
        readBinary(node.seq_num, in);

        if (version >= SnapshotVersion::V4)
        {
            readBinary(node.size_bytes, in);
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

void KeeperStorageSnapshot::serialize(const KeeperStorageSnapshot & snapshot, WriteBuffer & out, KeeperContextPtr keeper_context)
{
    writeBinary(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);

    if (snapshot.version >= SnapshotVersion::V5)
    {
        writeBinary(snapshot.zxid, out);
        if (keeper_context->digestEnabled())
        {
            writeBinary(static_cast<uint8_t>(KeeperStorage::CURRENT_DIGEST_VERSION), out);
            writeBinary(snapshot.nodes_digest, out);
        }
        else
            writeBinary(static_cast<uint8_t>(KeeperStorage::NO_DIGEST), out);
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
        if (node.stat.mzxid > snapshot.zxid)
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

void KeeperStorageSnapshot::deserialize(SnapshotDeserializationResult & deserialization_result, ReadBuffer & in, KeeperContextPtr keeper_context)
{
    uint8_t version;
    readBinary(version, in);
    SnapshotVersion current_version = static_cast<SnapshotVersion>(version);
    if (current_version > CURRENT_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

    deserialization_result.snapshot_meta = deserializeSnapshotMetadata(in);
    KeeperStorage & storage = *deserialization_result.storage;

    bool recalculate_digest = keeper_context->digestEnabled();
    if (version >= SnapshotVersion::V5)
    {
        readBinary(storage.zxid, in);
        uint8_t digest_version;
        readBinary(digest_version, in);
        if (digest_version != KeeperStorage::DigestVersion::NO_DIGEST)
        {
            uint64_t nodes_digest;
            readBinary(nodes_digest, in);
            if (digest_version == KeeperStorage::CURRENT_DIGEST_VERSION)
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

    if (recalculate_digest)
        storage.nodes_digest = 0;

    const auto is_node_empty = [](const auto & node)
    {
        return node.getData().empty() && node.stat == Coordination::Stat{};
    };

    for (size_t nodes_read = 0; nodes_read < snapshot_container_size; ++nodes_read)
    {
        std::string path;
        readBinary(path, in);
        KeeperStorage::Node node{};
        readNode(node, in, current_version, storage.acl_map);

        using enum Coordination::PathMatchResult;
        auto match_result = Coordination::matchPath(path, keeper_system_path);

        const std::string error_msg = fmt::format("Cannot read node on path {} from a snapshot because it is used as a system node", path);
        if (match_result == IS_CHILD)
        {
            if (keeper_context->ignoreSystemPathOnStartup() || keeper_context->getServerState() != KeeperContext::Phase::INIT)
            {
                LOG_ERROR(&Poco::Logger::get("KeeperSnapshotManager"), "{}. Ignoring it", error_msg);
                continue;
            }
            else
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "{}. Ignoring it can lead to data loss. "
                    "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true",
                    error_msg);
        }
        else if (match_result == EXACT)
        {
            if (!is_node_empty(node))
            {
                if (keeper_context->ignoreSystemPathOnStartup() || keeper_context->getServerState() != KeeperContext::Phase::INIT)
                {
                    LOG_ERROR(&Poco::Logger::get("KeeperSnapshotManager"), "{}. Ignoring it", error_msg);
                    node = KeeperStorage::Node{};
                }
                else
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "{}. Ignoring it can lead to data loss. "
                        "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true",
                        error_msg);
            }

            // we always ignore the written size for this node
            node.recalculateSize();
        }

        storage.container.insertOrReplace(path, node);
        if (node.stat.ephemeralOwner != 0)
            storage.ephemerals[node.stat.ephemeralOwner].insert(path);

        if (recalculate_digest)
            storage.nodes_digest += node.getDigest(path);
    }

    for (const auto & itr : storage.container)
    {
        if (itr.key != "/")
        {
            auto parent_path = parentNodePath(itr.key);
            storage.container.updateValue(
                parent_path, [version, path = itr.key](KeeperStorage::Node & value) { value.addChild(getBaseNodeName(path), /*update_size*/ version < SnapshotVersion::V4); });
        }
    }

    for (const auto & itr : storage.container)
    {
        if (itr.key != "/")
        {
            if (itr.value.stat.numChildren != static_cast<int32_t>(itr.value.getChildren().size()))
            {
#ifdef NDEBUG
                /// TODO (alesapin) remove this, it should be always CORRUPTED_DATA.
                LOG_ERROR(&Poco::Logger::get("KeeperSnapshotManager"), "Children counter in stat.numChildren {}"
                            " is different from actual children size {} for node {}", itr.value.stat.numChildren, itr.value.getChildren().size(), itr.key);
#else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Children counter in stat.numChildren {}"
                                " is different from actual children size {} for node {}",
                                itr.value.stat.numChildren, itr.value.getChildren().size(), itr.key);
#endif
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

            KeeperStorage::AuthIDs ids;
            size_t session_auth_counter = 0;
            while (session_auth_counter < session_auths_size)
            {
                String scheme, id;
                readBinary(scheme, in);
                readBinary(id, in);
                ids.emplace_back(KeeperStorage::AuthID{scheme, id});

                session_auth_counter++;
            }
            if (!ids.empty())
                storage.session_and_auth[active_session_id] = ids;
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
}

KeeperStorageSnapshot::KeeperStorageSnapshot(KeeperStorage * storage_, uint64_t up_to_log_idx_, const ClusterConfigPtr & cluster_config_)
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
    session_and_auth = storage->session_and_auth;
}

KeeperStorageSnapshot::KeeperStorageSnapshot(
    KeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_, const ClusterConfigPtr & cluster_config_)
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
    session_and_auth = storage->session_and_auth;
}

KeeperStorageSnapshot::~KeeperStorageSnapshot()
{
    storage->disableSnapshotMode();
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
    const auto load_snapshot_from_disk = [&](const auto & disk)
    {
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
            if (it->name().starts_with(tmp_prefix))
            {
                incomplete_files.emplace(it->name().substr(tmp_prefix.size()), it->path());
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
            auto [_, inserted] = existing_snapshots.insert_or_assign(snapshot_up_to, SnapshotFileInfo{snapshot_file, disk});

            if (!inserted)
                LOG_WARNING(
                    &Poco::Logger::get("KeeperSnapshotManager"),
                    "Found another snapshots with last log idx {}, will use snapshot from disk {}",
                    snapshot_up_to,
                    disk->getName());
        }

        for (const auto & [name, path] : incomplete_files)
            disk->removeFile(path);
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

SnapshotFileInfo KeeperSnapshotManager::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, uint64_t up_to_log_idx)
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

    existing_snapshots.emplace(up_to_log_idx, SnapshotFileInfo{snapshot_file_name, disk});
    removeOutdatedSnapshotsIfNeeded();
    moveSnapshotsIfNeeded();

    return {snapshot_file_name, disk};
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
            const auto & [path, disk] = latest_itr->second;
            disk->removeFile(path);
            existing_snapshots.erase(latest_itr->first);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    return nullptr;
}

nuraft::ptr<nuraft::buffer> KeeperSnapshotManager::deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const
{
    const auto & [snapshot_path, snapshot_disk] = existing_snapshots.at(up_to_log_idx);
    WriteBufferFromNuraftBuffer writer;
    auto reader = snapshot_disk->readFile(snapshot_path);
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

SnapshotDeserializationResult KeeperSnapshotManager::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer) const
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
    KeeperStorageSnapshot::deserialize(result, *compressed_reader, keeper_context);
    result.storage->initializeSystemNodes();
    return result;
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

void KeeperSnapshotManager::removeOutdatedSnapshotsIfNeeded()
{
    while (existing_snapshots.size() > snapshots_to_keep)
        removeSnapshot(existing_snapshots.begin()->first);
}

void KeeperSnapshotManager::moveSnapshotsIfNeeded()
{
    /// move snapshots to correct disks

    auto disk = getDisk();
    auto latest_snapshot_disk = getLatestSnapshotDisk();
    auto latest_snapshot_idx = getLatestSnapshotIndex();

    for (auto & [idx, file_info] : existing_snapshots)
    {
        if (idx == latest_snapshot_idx)
        {
            if (file_info.disk != latest_snapshot_disk)
            {
                moveFileBetweenDisks(file_info.disk, file_info.path, latest_snapshot_disk, file_info.path);
                file_info.disk = latest_snapshot_disk;
            }
        }
        else
        {
            if (file_info.disk != disk)
            {
                moveFileBetweenDisks(file_info.disk, file_info.path, disk, file_info.path);
                file_info.disk = disk;
            }
        }
    }

}

void KeeperSnapshotManager::removeSnapshot(uint64_t log_idx)
{
    auto itr = existing_snapshots.find(log_idx);
    if (itr == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);
    const auto & [path, disk] = itr->second;
    disk->removeFile(path);
    existing_snapshots.erase(itr);
}

SnapshotFileInfo KeeperSnapshotManager::serializeSnapshotToDisk(const KeeperStorageSnapshot & snapshot)
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

    KeeperStorageSnapshot::serialize(snapshot, *compressed_writer, keeper_context);
    compressed_writer->finalize();
    compressed_writer->sync();

    disk->removeFile(tmp_snapshot_file_name);

    existing_snapshots.emplace(up_to_log_idx, SnapshotFileInfo{snapshot_file_name, disk});
    removeOutdatedSnapshotsIfNeeded();
    moveSnapshotsIfNeeded();

    return {snapshot_file_name, disk};
}

}
