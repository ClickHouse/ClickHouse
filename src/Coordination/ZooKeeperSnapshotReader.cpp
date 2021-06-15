#include <Coordination/ZooKeeperSnapshotReader.h>
#include <filesystem>
#include <cstdlib>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <IO/ReadBufferFromFile.h>
#include <string>


namespace DB
{

static String parentPath(const String & path)
{
    auto rslash_pos = path.rfind('/');
    if (rslash_pos > 0)
        return path.substr(0, rslash_pos);
    return "/";
}

static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
}

int64_t getZxidFromName(const std::string & filename)
{
    std::filesystem::path path(filename);
    std::string extension = path.extension();
    //std::cerr << "Extension:" << extension << std::endl;
    char * end;
    int64_t zxid = std::strtoul(extension.data() + 1, &end, 16);
    return zxid;
}

void deserializeMagic(ReadBuffer & in)
{
    int32_t magic_header, version;
    int64_t dbid;
    Coordination::read(magic_header, in);
    Coordination::read(version, in);
    Coordination::read(dbid, in);
    //const char * data = "ZKSN";
    //std::cerr << "Expected Hedader:" << *reinterpret_cast<const int32_t *>(data) << std::endl;
    //std::cerr << "MAGIC HEADER:" << magic_header << std::endl;
    //std::cerr << "VERSION:" << version << std::endl;
    //std::cerr << "DBID:" << dbid << std::endl;
}

int64_t deserializeSessionAndTimeout(KeeperStorage & storage, ReadBuffer & in)
{
    int32_t count;
    Coordination::read(count, in);
    //std::cerr << "Total session and timeout:" << count << std::endl;
    int64_t max_session_id = 0;
    while (count > 0)
    {
        int64_t session_id;
        int32_t timeout;

        Coordination::read(session_id, in);
        Coordination::read(timeout, in);
        //std::cerr << "Session id:" << session_id << std::endl;
        //std::cerr << "Timeout:" << timeout << std::endl;
        storage.addSessionID(session_id, timeout);
        max_session_id = std::max(session_id, max_session_id);
        count--;
    }
    std::cerr << "Done deserializing sessions\n";
    return max_session_id;
}

void deserializeACLMap(KeeperStorage & storage, ReadBuffer & in)
{
    int32_t count;
    Coordination::read(count, in);
    //std::cerr << "ACLs Count:" << count << "\n";
    while (count > 0)
    {
        int64_t map_index;
        Coordination::read(map_index, in);
        //std::cerr << "Map index:" << map_index << "\n";

        Coordination::ACLs acls;
        int32_t acls_len;
        Coordination::read(acls_len, in);

        //std::cerr << "ACLs len:" << acls_len << "\n";
        while (acls_len > 0)
        {
            Coordination::ACL acl;
            Coordination::read(acl.permissions, in);
            Coordination::read(acl.scheme, in);
            Coordination::read(acl.id, in);
            //std::cerr << "ACL perms:" << acl.permissions << "\n";
            //std::cerr << "ACL scheme:" << acl.scheme << "\n";
            //std::cerr << "ACL id:" << acl.id << "\n";
            acls.push_back(acl);
            acls_len--;
        }
        storage.acl_map.addMapping(map_index, acls);

        count--;
    }
    std::cerr << "Done deserializing ACLs Total" << count << "\n";
}

int64_t deserializeStorageData(KeeperStorage & storage, ReadBuffer & in)
{
    int64_t max_zxid = 0;
    std::string path;
    Coordination::read(path, in);
    //std::cerr << "Read path FIRST length:" << path.length() << std::endl;
    //std::cerr << "Read path FIRST data:" << path << std::endl;
    size_t count = 0;
    while (path != "/")
    {
        KeeperStorage::Node node{};
        Coordination::read(node.data, in);
        Coordination::read(node.acl_id, in);

        /// Deserialize stat
        Coordination::read(node.stat.czxid, in);
        Coordination::read(node.stat.mzxid, in);
        /// For some reason ZXID specified in filename can be smaller
        /// then actual zxid from nodes.
        max_zxid = std::max(max_zxid, node.stat.mzxid);

        Coordination::read(node.stat.ctime, in);
        Coordination::read(node.stat.mtime, in);
        Coordination::read(node.stat.version, in);
        Coordination::read(node.stat.cversion, in);
        Coordination::read(node.stat.aversion, in);
        Coordination::read(node.stat.ephemeralOwner, in);
        Coordination::read(node.stat.pzxid, in);
        if (!path.empty())
        {
            node.stat.dataLength = node.data.length();
            node.seq_num = node.stat.cversion;
            storage.container.insertOrReplace(path, node);

            if (node.stat.ephemeralOwner != 0)
                storage.ephemerals[node.stat.ephemeralOwner].insert(path);

            storage.acl_map.addUsage(node.acl_id);
        }
        Coordination::read(path, in);
        count++;
        if (count % 1000 == 0)
            std::cerr << "Deserialized nodes:"  << count << std::endl;
    }

    for (const auto & itr : storage.container)
    {
        if (itr.key != "/")
        {
            auto parent_path = parentPath(itr.key);
            storage.container.updateValue(parent_path, [&path = itr.key] (KeeperStorage::Node & value) { value.children.insert(getBaseName(path)); value.stat.numChildren++; });
        }
    }

    return max_zxid;
}

void deserializeKeeperStorage(KeeperStorage & storage, const std::string & path)
{
    int64_t zxid = getZxidFromName(path);
    //std::cerr << "Got ZXID:" << zxid << std::endl;

    ReadBufferFromFile reader(path);

    deserializeMagic(reader);
    auto max_session_id = deserializeSessionAndTimeout(storage, reader);

    storage.session_id_counter = max_session_id;
    deserializeACLMap(storage, reader);

    int64_t zxid_from_nodes = deserializeStorageData(storage, reader);
    storage.zxid = std::max(zxid, zxid_from_nodes);
}

}
