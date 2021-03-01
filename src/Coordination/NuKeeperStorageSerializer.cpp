#include <Coordination/NuKeeperStorageSerializer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

namespace
{
    void writeNode(const NuKeeperStorage::Node & node, WriteBuffer & out)
    {
        Coordination::write(node.data, out);
        Coordination::write(node.acls, out);
        Coordination::write(node.is_ephemeral, out);
        Coordination::write(node.is_sequental, out);
        Coordination::write(node.stat, out);
        Coordination::write(node.seq_num, out);
    }

    void readNode(NuKeeperStorage::Node & node, ReadBuffer & in)
    {
        Coordination::read(node.data, in);
        Coordination::read(node.acls, in);
        Coordination::read(node.is_ephemeral, in);
        Coordination::read(node.is_sequental, in);
        Coordination::read(node.stat, in);
        Coordination::read(node.seq_num, in);
    }
}

void NuKeeperStorageSerializer::serialize(const NuKeeperStorage & storage, WriteBuffer & out)
{
    Coordination::write(storage.zxid, out);
    Coordination::write(storage.session_id_counter, out);
    Coordination::write(storage.container.size(), out);
    for (const auto & [path, node] : storage.container)
    {
        Coordination::write(path, out);
        writeNode(node, out);
    }
    Coordination::write(storage.ephemerals.size(), out);
    for (const auto & [session_id, paths] : storage.ephemerals)
    {
        Coordination::write(session_id, out);
        Coordination::write(paths.size(), out);
        for (const auto & path : paths)
            Coordination::write(path, out);
    }
}

void NuKeeperStorageSerializer::deserialize(NuKeeperStorage & storage, ReadBuffer & in)
{
    int64_t session_id_counter, zxid;
    Coordination::read(zxid, in);
    Coordination::read(session_id_counter, in);
    storage.zxid = zxid;
    storage.session_id_counter = session_id_counter;

    size_t container_size;
    Coordination::read(container_size, in);

    size_t current_size = 0;
    while (current_size < container_size)
    {
        std::string path;
        Coordination::read(path, in);
        NuKeeperStorage::Node node;
        readNode(node, in);
        storage.container[path] = node;
        current_size++;
    }
    size_t ephemerals_size;
    Coordination::read(ephemerals_size, in);
    while (storage.ephemerals.size() < ephemerals_size)
    {
        int64_t session_id;
        size_t ephemerals_for_session;
        Coordination::read(session_id, in);
        Coordination::read(ephemerals_for_session, in);
        while (storage.ephemerals[session_id].size() < ephemerals_for_session)
        {
            std::string ephemeral_path;
            Coordination::read(ephemeral_path, in);
            storage.ephemerals[session_id].emplace(ephemeral_path);
        }
    }
}

}
