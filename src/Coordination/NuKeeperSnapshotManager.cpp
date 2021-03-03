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

    String parentPath(const String & path)
    {
        auto rslash_pos = path.rfind('/');
        if (rslash_pos > 0)
            return path.substr(0, rslash_pos);
        return "/";
    }

    void writeNode(const NuKeeperStorage::Node & node, WriteBuffer & out)
    {
        Coordination::write(node.data, out);
        Coordination::write(node.acls, out);
        Coordination::write(node.ephemeral_owner, out);
        Coordination::write(node.is_sequental, out);
        Coordination::write(node.stat, out);
        Coordination::write(node.seq_num, out);
    }

    void readNode(NuKeeperStorage::Node & node, ReadBuffer & in)
    {
        Coordination::read(node.data, in);
        Coordination::read(node.acls, in);
        Coordination::read(node.ephemeral_owner, in);
        Coordination::read(node.is_sequental, in);
        Coordination::read(node.stat, in);
        Coordination::read(node.seq_num, in);
    }

    void serializeSnapshotMetadata(const SnapshotMetadataPtr & snapshot_meta, WriteBuffer & out)
    {
        auto buffer = snapshot_meta->serialize();
        Coordination::write(reinterpret_cast<const char *>(buffer->data_begin()), buffer->size(), out);
    }

    SnapshotMetadataPtr deserializeSnapshotMetadata(ReadBuffer & in)
    {
        /// FIXME (alesap)
        std::string data;
        Coordination::read(data, in);
        auto buffer = nuraft::buffer::alloc(data.size());
        buffer->put_raw(reinterpret_cast<const nuraft::byte *>(data.c_str()), data.size());
        buffer->pos(0);
        return SnapshotMetadata::deserialize(*buffer);
    }
}


void NuKeeperStorageSnapshot::serialize(const NuKeeperStorageSnapshot & snapshot, WriteBuffer & out)
{
    Coordination::write(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);
    Coordination::write(snapshot.zxid, out);
    Coordination::write(snapshot.session_id, out);
    Coordination::write(snapshot.snapshot_container_size, out);
    size_t counter = 0;
    for (auto it = snapshot.begin; counter < snapshot.snapshot_container_size; ++it, ++counter)
    {
        const auto & path = it->key;
        const auto & node = it->value;
        Coordination::write(path, out);
        writeNode(node, out);
    }

    size_t size = snapshot.session_and_timeout.size();
    Coordination::write(size, out);
    for (const auto & [session_id, timeout] : snapshot.session_and_timeout)
    {
        Coordination::write(session_id, out);
        Coordination::write(timeout, out);
    }
}

SnapshotMetadataPtr NuKeeperStorageSnapshot::deserialize(NuKeeperStorage & storage, ReadBuffer & in)
{
    uint8_t version;
    Coordination::read(version, in);
    if (static_cast<SnapshotVersion>(version) > SnapshotVersion::V0)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

    SnapshotMetadataPtr result = deserializeSnapshotMetadata(in);
    int64_t session_id, zxid;
    Coordination::read(zxid, in);
    Coordination::read(session_id, in);
    storage.zxid = zxid;
    storage.session_id_counter = session_id;

    size_t snapshot_container_size;
    Coordination::read(snapshot_container_size, in);

    size_t current_size = 0;
    while (current_size < snapshot_container_size)
    {
        std::string path;
        Coordination::read(path, in);
        NuKeeperStorage::Node node;
        readNode(node, in);
        storage.container.insertOrReplace(path, node);
        if (node.ephemeral_owner != 0)
            storage.ephemerals[node.ephemeral_owner].insert(path);

        current_size++;
    }
    for (const auto & itr : storage.container)
    {
        if (itr.key != "/")
        {
            auto parent_path = parentPath(itr.key);
            storage.container.updateValue(parent_path, [&path = itr.key] (NuKeeperStorage::Node & value) { value.children.insert(path); });
        }
    }

    size_t active_sessions_size;
    Coordination::read(active_sessions_size, in);

    size_t current_session_size = 0;
    while (current_session_size < active_sessions_size)
    {
        int64_t active_session_id, timeout;
        Coordination::read(active_session_id, in);
        Coordination::read(timeout, in);
        storage.addSessionID(active_session_id, timeout);
        current_session_size++;
    }

    return result;
}

NuKeeperStorageSnapshot::NuKeeperStorageSnapshot(NuKeeperStorage * storage_, size_t up_to_log_idx_)
    : storage(storage_)
    , snapshot_meta(std::make_shared<SnapshotMetadata>(up_to_log_idx_, 0, std::make_shared<nuraft::cluster_config>()))
    , zxid(storage->getZXID())
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
    , zxid(storage->getZXID())
    , session_id(storage->session_id_counter)
{
    storage->enableSnapshotMode();
    snapshot_container_size = storage->container.snapshotSize();
    begin = storage->getSnapshotIteratorBegin();
    session_and_timeout = storage->getActiveSessions();
}

NuKeeperStorageSnapshot::~NuKeeperStorageSnapshot()
{
    storage->clearGarbageAfterSnapshot();
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
