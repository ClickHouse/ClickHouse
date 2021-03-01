#include <Coordination/NuKeeperStorageSerializer.h>
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
}


void NuKeeperStorageSnapshot::serialize(const NuKeeperStorageSnapshot & snapshot, WriteBuffer & out)
{
    Coordination::write(static_cast<uint8_t>(snapshot.version), out);
    Coordination::write(snapshot.zxid, out);
    Coordination::write(snapshot.session_id, out);
    Coordination::write(snapshot.snapshot_container_size, out);
    for (auto it = snapshot.begin; it != snapshot.end; ++it)
    {
        const auto & path = it->key;
        const auto & node = it->value;
        Coordination::write(path, out);
        writeNode(node, out);
    }

    Coordination::write(snapshot.session_and_timeout.size(), out);
    for (const auto & [session_id, timeout] : snapshot.session_and_timeout)
    {
        Coordination::write(session_id, out);
        Coordination::write(timeout, out);
    }
}

void NuKeeperStorageSnapshot::deserialize(NuKeeperStorage & storage, ReadBuffer & in)
{
    uint8_t version;
    Coordination::read(version, in);
    if (static_cast<SnapshotVersion>(version) > SnapshotVersion::V0)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

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
        if (path != "/")
        {
            auto parent_path = parentPath(path);
            storage.container.updateValue(parent_path, [&path] (NuKeeperStorage::Node & value) { value.children.insert(path); });
        }

        if (node.ephemeral_owner != 0)
            storage.ephemerals[node.ephemeral_owner].insert(path);

        current_size++;
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
}

NuKeeperStorageSnapshot::NuKeeperStorageSnapshot(NuKeeperStorage * storage_, size_t up_to_log_idx_)
    : storage(storage_)
    , up_to_log_idx(up_to_log_idx_)
    , zxid(storage->getZXID())
    , session_id(storage->session_id_counter)
{
    storage->enableSnapshotMode();
    snapshot_container_size = storage->container.snapshotSize();
    begin = storage->getSnapshotIteratorBegin();
    end = storage->getSnapshotIteratorEnd();
    session_and_timeout = storage->getActiveSessions();
}

NuKeeperStorageSnapshot::~NuKeeperStorageSnapshot()
{
    storage->clearGarbageAfterSnapshot();
    storage->disableSnapshotMode();
}

NuKeeperSnapshotManager::NuKeeperSnapshotManager(const std::string & snapshots_path_)
    : snapshots_path(snapshots_path_)
{
    namespace fs = std::filesystem;

    if (!fs::exists(snapshots_path))
        fs::create_directories(snapshots_path);

    for (const auto & p : fs::directory_iterator(snapshots_path))
    {
        size_t snapshot_up_to = getSnapshotPathUpToLogIdx(p.path());
        existing_snapshots[snapshot_up_to] = p.path();
    }
}


std::string NuKeeperSnapshotManager::serializeSnapshotBufferToDisk(nuraft::buffer & buffer, size_t up_to_log_idx)
{
    ReadBufferFromNuraftBuffer reader(buffer);

    std::string new_snapshot_path = std::filesystem::path{snapshots_path} / getSnapshotFileName(up_to_log_idx);

    WriteBufferFromFile plain_buf(new_snapshot_path);
    copyData(reader, plain_buf);
    plain_buf.sync();
    existing_snapshots.emplace(up_to_log_idx, new_snapshot_path);
    return new_snapshot_path;
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


void NuKeeperSnapshotManager::deserializeSnapshotFromBuffer(NuKeeperStorage * storage, nuraft::ptr<nuraft::buffer> buffer)
{
    ReadBufferFromNuraftBuffer reader(buffer);
    CompressedReadBuffer compressed_reader(reader);
    NuKeeperStorageSnapshot::deserialize(*storage, compressed_reader);
}

size_t NuKeeperSnapshotManager::restoreFromLatestSnapshot(NuKeeperStorage * storage) const
{
    if (existing_snapshots.empty())
        return 0 ;

    auto log_id = existing_snapshots.rbegin()->first;
    auto buffer = deserializeSnapshotBufferFromDisk(log_id);
    deserializeSnapshotFromBuffer(storage, buffer);
    return log_id;
}



}
