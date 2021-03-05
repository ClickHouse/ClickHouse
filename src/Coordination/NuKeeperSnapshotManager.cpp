#include <Coordination/NuKeeperSnapshotManager.h>
#include <IO/WriteHelpers.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/copyData.h>
#include <filesystem>

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
    size_t getSnapshotPathUpToLogIdx(const String & snapshot_path)
    {
        std::filesystem::path path(snapshot_path);
        std::string filename = path.stem();
        Strings name_parts;
        splitInto<'_'>(name_parts, filename);
        return parse<size_t>(name_parts[1]);
    }

    std::string getSnapshotFileName(size_t up_to_log_idx)
    {
        return std::string{"snapshot_"} + std::to_string(up_to_log_idx) + ".bin";
    }

    std::string getBaseName(const String & path)
    {
        size_t basename_start = path.rfind('/');
        return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
    }

    String parentPath(const String & path)
    {
        auto rslash_pos = path.rfind('/');
        if (rslash_pos > 0)
            return path.substr(0, rslash_pos);
        return "/";
    }

    void writeNode(const NuKeeperStorage::Node & node, WriteBuffer & out)
    {
        writeBinary(node.data, out);

        /// Serialize ACL
        writeBinary(node.acls.size(), out);
        for (const auto & acl : node.acls)
        {
            writeBinary(acl.permissions, out);
            writeBinary(acl.scheme, out);
            writeBinary(acl.id, out);
        }

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
    }

    void readNode(NuKeeperStorage::Node & node, ReadBuffer & in)
    {
        readBinary(node.data, in);

        /// Deserialize ACL
        size_t acls_size;
        readBinary(acls_size, in);
        for (size_t i = 0; i < acls_size; ++i)
        {
            Coordination::ACL acl;
            readBinary(acl.permissions, in);
            readBinary(acl.scheme, in);
            readBinary(acl.id, in);
            node.acls.push_back(acl);
        }
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


void NuKeeperStorageSnapshot::serialize(const NuKeeperStorageSnapshot & snapshot, WriteBuffer & out)
{
    writeBinary(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);
    writeBinary(snapshot.session_id, out);
    writeBinary(snapshot.snapshot_container_size, out);
    size_t counter = 0;
    for (auto it = snapshot.begin; counter < snapshot.snapshot_container_size; ++it, ++counter)
    {
        const auto & path = it->key;
        const auto & node = it->value;
        if (static_cast<size_t>(node.stat.mzxid) > snapshot.snapshot_meta->get_last_log_idx())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to serialize node with mzxid {}, but last snapshot index {}", node.stat.mzxid, snapshot.snapshot_meta->get_last_log_idx());

        writeBinary(path, out);
        writeNode(node, out);
    }

    size_t size = snapshot.session_and_timeout.size();
    writeBinary(size, out);
    for (const auto & [session_id, timeout] : snapshot.session_and_timeout)
    {
        writeBinary(session_id, out);
        writeBinary(timeout, out);
    }
}

SnapshotMetadataPtr NuKeeperStorageSnapshot::deserialize(NuKeeperStorage & storage, ReadBuffer & in)
{
    uint8_t version;
    readBinary(version, in);
    if (static_cast<SnapshotVersion>(version) > SnapshotVersion::V0)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

    SnapshotMetadataPtr result = deserializeSnapshotMetadata(in);
    int64_t session_id;
    readBinary(session_id, in);
    storage.zxid = result->get_last_log_idx();
    storage.session_id_counter = session_id;

    size_t snapshot_container_size;
    readBinary(snapshot_container_size, in);

    size_t current_size = 0;
    while (current_size < snapshot_container_size)
    {
        std::string path;
        readBinary(path, in);
        NuKeeperStorage::Node node;
        readNode(node, in);
        storage.container.insertOrReplace(path, node);
        if (node.stat.ephemeralOwner != 0)
            storage.ephemerals[node.stat.ephemeralOwner].insert(path);

        current_size++;
    }

    for (const auto & itr : storage.container)
    {
        if (itr.key != "/")
        {
            auto parent_path = parentPath(itr.key);
            storage.container.updateValue(parent_path, [&path = itr.key] (NuKeeperStorage::Node & value) { value.children.insert(getBaseName(path)); });
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
        current_session_size++;
    }

    return result;
}

NuKeeperStorageSnapshot::NuKeeperStorageSnapshot(NuKeeperStorage * storage_, size_t up_to_log_idx_)
    : storage(storage_)
    , snapshot_meta(std::make_shared<SnapshotMetadata>(up_to_log_idx_, 0, std::make_shared<nuraft::cluster_config>()))
    , session_id(storage->session_id_counter)
{
    storage->enableSnapshotMode();
    snapshot_container_size = storage->container.snapshotSize();
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
}

NuKeeperStorageSnapshot::NuKeeperStorageSnapshot(NuKeeperStorage * storage_, const SnapshotMetadataPtr & snapshot_meta_)
    : storage(storage_)
    , snapshot_meta(snapshot_meta_)
    , session_id(storage->session_id_counter)
{
    storage->enableSnapshotMode();
    snapshot_container_size = storage->container.snapshotSize();
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
}

NuKeeperStorageSnapshot::~NuKeeperStorageSnapshot()
{
    storage->disableSnapshotMode();
}

NuKeeperSnapshotManager::NuKeeperSnapshotManager(const std::string & snapshots_path_, size_t snapshots_to_keep_)
    : snapshots_path(snapshots_path_)
    , snapshots_to_keep(snapshots_to_keep_)
{
    namespace fs = std::filesystem;

    if (!fs::exists(snapshots_path))
        fs::create_directories(snapshots_path);

    for (const auto & p : fs::directory_iterator(snapshots_path))
    {
        if (startsWith(p.path(), "tmp_")) /// Unfinished tmp files
        {
            std::filesystem::remove(p);
            continue;
        }
        size_t snapshot_up_to = getSnapshotPathUpToLogIdx(p.path());
        existing_snapshots[snapshot_up_to] = p.path();
    }

    removeOutdatedSnapshotsIfNeeded();
}


std::string NuKeeperSnapshotManager::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, size_t up_to_log_idx)
{
    ReadBufferFromNuraftBuffer reader(buffer);

    auto snapshot_file_name = getSnapshotFileName(up_to_log_idx);
    auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;
    std::string tmp_snapshot_path = std::filesystem::path{snapshots_path} / tmp_snapshot_file_name;
    std::string new_snapshot_path = std::filesystem::path{snapshots_path} / snapshot_file_name;

    WriteBufferFromFile plain_buf(tmp_snapshot_path);
    copyData(reader, plain_buf);
    plain_buf.sync();

    std::filesystem::rename(tmp_snapshot_path, new_snapshot_path);

    existing_snapshots.emplace(up_to_log_idx, new_snapshot_path);
    removeOutdatedSnapshotsIfNeeded();

    return new_snapshot_path;
}

nuraft::ptr<nuraft::buffer> NuKeeperSnapshotManager::deserializeLatestSnapshotBufferFromDisk()
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
            std::filesystem::remove(latest_itr->second);
            existing_snapshots.erase(latest_itr->first);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    return nullptr;
}

nuraft::ptr<nuraft::buffer> NuKeeperSnapshotManager::deserializeSnapshotBufferFromDisk(size_t up_to_log_idx) const
{
    const std::string & snapshot_path = existing_snapshots.at(up_to_log_idx);
    WriteBufferFromNuraftBuffer writer;
    ReadBufferFromFile reader(snapshot_path);
    copyData(reader, writer);
    return writer.getBuffer();
}

nuraft::ptr<nuraft::buffer> NuKeeperSnapshotManager::serializeSnapshotToBuffer(const NuKeeperStorageSnapshot & snapshot)
{
    WriteBufferFromNuraftBuffer writer;
    CompressedWriteBuffer compressed_writer(writer);

    NuKeeperStorageSnapshot::serialize(snapshot, compressed_writer);
    compressed_writer.finalize();
    return writer.getBuffer();
}

SnapshotMetadataPtr NuKeeperSnapshotManager::deserializeSnapshotFromBuffer(NuKeeperStorage * storage, nuraft::ptr<nuraft::buffer> buffer)
{
    ReadBufferFromNuraftBuffer reader(buffer);
    CompressedReadBuffer compressed_reader(reader);
    return NuKeeperStorageSnapshot::deserialize(*storage, compressed_reader);
}

SnapshotMetadataPtr NuKeeperSnapshotManager::restoreFromLatestSnapshot(NuKeeperStorage * storage)
{
    if (existing_snapshots.empty())
        return nullptr;

    auto buffer = deserializeLatestSnapshotBufferFromDisk();
    if (!buffer)
        return nullptr;
    return deserializeSnapshotFromBuffer(storage, buffer);
}

void NuKeeperSnapshotManager::removeOutdatedSnapshotsIfNeeded()
{
    while (existing_snapshots.size() > snapshots_to_keep)
        removeSnapshot(existing_snapshots.begin()->first);
}

void NuKeeperSnapshotManager::removeSnapshot(size_t log_idx)
{
    auto itr = existing_snapshots.find(log_idx);
    if (itr == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);
    std::filesystem::remove(itr->second);
    existing_snapshots.erase(itr);

}


}
