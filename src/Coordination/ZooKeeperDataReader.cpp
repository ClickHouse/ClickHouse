#include <Coordination/ZooKeeperDataReader.h>
#include <filesystem>
#include <cstdlib>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <IO/ReadBufferFromFile.h>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CORRUPTED_DATA;
}

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
    char * end;
    int64_t zxid = std::strtoul(extension.data() + 1, &end, 16);
    return zxid;
}

void deserializeSnapshotMagic(ReadBuffer & in)
{
    int32_t magic_header, version;
    int64_t dbid;
    Coordination::read(magic_header, in);
    Coordination::read(version, in);
    if (version != 2)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot deserialize ZooKeeper data other than version 2, got version {}", version);
    Coordination::read(dbid, in);
    static constexpr int32_t SNP_HEADER = 1514885966; /// "ZKSN"
    if (magic_header != SNP_HEADER)
        throw Exception(ErrorCodes::CORRUPTED_DATA ,"Incorrect magic header in file, expected {}, got {}", SNP_HEADER, magic_header);
}

int64_t deserializeSessionAndTimeout(KeeperStorage & storage, ReadBuffer & in)
{
    int32_t count;
    Coordination::read(count, in);
    int64_t max_session_id = 0;
    while (count > 0)
    {
        int64_t session_id;
        int32_t timeout;

        Coordination::read(session_id, in);
        Coordination::read(timeout, in);
        storage.addSessionID(session_id, timeout);
        max_session_id = std::max(session_id, max_session_id);
        count--;
    }
    return max_session_id;
}

void deserializeACLMap(KeeperStorage & storage, ReadBuffer & in)
{
    int32_t count;
    Coordination::read(count, in);
    while (count > 0)
    {
        int64_t map_index;
        Coordination::read(map_index, in);

        Coordination::ACLs acls;
        int32_t acls_len;
        Coordination::read(acls_len, in);

        while (acls_len > 0)
        {
            Coordination::ACL acl;
            Coordination::read(acl.permissions, in);
            Coordination::read(acl.scheme, in);
            Coordination::read(acl.id, in);
            acls.push_back(acl);
            acls_len--;
        }
        storage.acl_map.addMapping(map_index, acls);

        count--;
    }
}

int64_t deserializeStorageData(KeeperStorage & storage, ReadBuffer & in, Poco::Logger * log)
{
    int64_t max_zxid = 0;
    std::string path;
    Coordination::read(path, in);
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
        /// then actual zxid from nodes. In this case we will use zxid from nodes.
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
            LOG_INFO(log, "Deserialized nodes from snapshot: {}", count);
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

void deserializeKeeperStorageFromSnapshot(KeeperStorage & storage, const std::string & snapshot_path, Poco::Logger * log)
{
    LOG_INFO(log, "Deserializing storage snapshot {}", snapshot_path);
    int64_t zxid = getZxidFromName(snapshot_path);

    ReadBufferFromFile reader(snapshot_path);

    deserializeSnapshotMagic(reader);

    LOG_INFO(log, "Magic deserialized, looks OK");
    auto max_session_id = deserializeSessionAndTimeout(storage, reader);
    LOG_INFO(log, "Sessions and timeouts deserialized");

    storage.session_id_counter = max_session_id;
    deserializeACLMap(storage, reader);
    LOG_INFO(log, "ACLs deserialized");

    LOG_INFO(log, "Deserializing data from snapshot");
    int64_t zxid_from_nodes = deserializeStorageData(storage, reader, log);
    /// In ZooKeeper Snapshots can contain inconsistent state of storage. They call
    /// this inconsistent state "fuzzy". So it's guaranteed that snapshot contain all
    /// records up to zxid from snapshot name and also some records for future.
    /// But it doesn't mean that we have just some state of storage from future (like zxid + 100 log records).
    /// We have incorrect state of storage where some random log entries from future were applied....
    ///
    /// In ZooKeeper they say that their transactions log is idempotent and can be applied to "fuzzy" state as is.
    /// It's true but there is no any general invariant which produces this property. They just have ad-hoc "if's" which detects
    /// "fuzzy" state inconsistencies and apply log records in special way. Several examples:
    /// https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java#L453-L463
    /// https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java#L476-L480
    /// https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java#L547-L549
    if (zxid_from_nodes > zxid)
        LOG_WARNING(log, "ZooKeeper snapshot was in inconsistent (fuzzy) state. Will try to apply log. ZooKeeper create non fuzzy snapshot with restart. You can just restart ZooKeeper server and get consistent version.");

    storage.zxid = zxid;

    LOG_INFO(log, "Finished, snapshot ZXID {}", storage.zxid);
}

void deserializeKeeperStorageFromSnapshotsDir(KeeperStorage & storage, const std::string & path, Poco::Logger * log)
{
    namespace fs = std::filesystem;
    std::map<int64_t, std::string> existing_snapshots;
    for (const auto & p : fs::directory_iterator(path))
    {
        const auto & log_path = p.path();
        if (!log_path.has_filename() || !startsWith(log_path.filename(), "snapshot."))
            continue;
        int64_t zxid = getZxidFromName(log_path);
        existing_snapshots[zxid] = p.path();
    }

    LOG_INFO(log, "Totally have {} snapshots, will use latest", existing_snapshots.size());
    /// deserialize only from latest snapshot
    if (!existing_snapshots.empty())
        deserializeKeeperStorageFromSnapshot(storage, existing_snapshots.rbegin()->second, log);
    else
        throw Exception(ErrorCodes::CORRUPTED_DATA, "No snapshots found on path {}. At least one snapshot must exist.", path);
}

void deserializeLogMagic(ReadBuffer & in)
{
    int32_t magic_header, version;
    int64_t dbid;
    Coordination::read(magic_header, in);
    Coordination::read(version, in);
    Coordination::read(dbid, in);

    static constexpr int32_t LOG_HEADER = 1514884167; /// "ZKLG"
    if (magic_header != LOG_HEADER)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Incorrect magic header in file, expected {}, got {}", LOG_HEADER, magic_header);

    if (version != 2)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot deserialize ZooKeeper data other than version 2, got version {}", version);
}


/// ZooKeeper transactions log differs from requests. The main reason: to store records in log
/// in some "finalized" state (for example with concrete versions).
///
/// Example:
/// class CreateTxn {
///      ustring path;
///      buffer data;
///      vector<org.apache.zookeeper.data.ACL> acl;
///      boolean ephemeral;
///      int parentCVersion;
///  }
///  But Create Request:
///  class CreateRequest {
///      ustring path;
///      buffer data;
///      vector<org.apache.zookeeper.data.ACL> acl;
///      int flags;
///  }
///
/// However type is the same OpNum...
///
/// Also there is a comment in ZooKeeper's code base about log structure, but
/// it's almost completely incorrect. Actual ZooKeeper log structure starting from version 3.6+:
///
/// Magic Header: "ZKLG" + 4 byte version + 8 byte dbid.
/// After that goes serialized transactions, in the following format:
///     8 byte checksum
///     4 byte transaction length
///     8 byte session_id (author of the transaction)
///     4 byte user XID
///     8 byte ZXID
///     8 byte transaction time
///     4 byte transaction type (OpNum)
///     [Transaction body depending on transaction type]
///     12 bytes tail (starting from 3.6+): 4 byte version + 8 byte checksum of data tree
///     1 byte -- 0x42
///
/// Transaction body is quite simple for all kinds of transactions except
/// Multitransactions. Their structure is following:
///     4 byte sub transactions count
///     4 byte sub transaction length
///     [Transaction body depending on transaction type]
///     and so on
///
/// Gotchas:
///
///     1) For some reason ZooKeeper store ErrorTxn's in log. It's
///     reasonable for Multitransactions, but why they store standalone errors
///     is not clear.
///
///     2) For some reason there is no 12 bytes tail (version + checksum of
///     tree) after standalone ErrorTxn.
///
///     3) The most strange thing: In one of our production logs (about 1.2GB
///     size) we have found Multitransaction with two sub transactions: Error1
///     and Error2, both -1 OpCode. Normal Error transaction has 4 bytes length
///     (for error code), but the Error1 has 550 bytes length. What is more
///     strange, that this 550 bytes obviously was a part of Create transaction,
///     but the operation code was -1. We have added debug prints to original
///     zookeeper (3.6.3) and found that it just reads 550 bytes of this "Error"
///     transaction, tooks the first 4 bytes as an error code (it was 79, non
///     existing code) and skip all remaining 546 bytes. NOTE: it looks like a bug
///     in ZooKeeper.
///
namespace
{

Coordination::ZooKeeperRequestPtr deserializeCreateTxn(ReadBuffer & in)
{
    std::shared_ptr<Coordination::ZooKeeperCreateRequest> result = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    Coordination::read(result->path, in);
    Coordination::read(result->data, in);
    Coordination::read(result->acls, in);
    Coordination::read(result->is_ephemeral, in);
    Coordination::read(result->parent_cversion, in);

    result->restored_from_zookeeper_log = true;
    return result;
}

Coordination::ZooKeeperRequestPtr deserializeDeleteTxn(ReadBuffer & in)
{
    std::shared_ptr<Coordination::ZooKeeperRemoveRequest> result = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
    Coordination::read(result->path, in);
    result->restored_from_zookeeper_log = true;
    return result;
}

Coordination::ZooKeeperRequestPtr deserializeSetTxn(ReadBuffer & in)
{
    std::shared_ptr<Coordination::ZooKeeperSetRequest> result = std::make_shared<Coordination::ZooKeeperSetRequest>();
    Coordination::read(result->path, in);
    Coordination::read(result->data, in);
    Coordination::read(result->version, in);
    result->restored_from_zookeeper_log = true;
    /// It stores version + 1 (which should be, not for request)
    result->version -= 1;

    return result;
}

Coordination::ZooKeeperRequestPtr deserializeCheckVersionTxn(ReadBuffer & in)
{
    std::shared_ptr<Coordination::ZooKeeperCheckRequest> result = std::make_shared<Coordination::ZooKeeperCheckRequest>();
    Coordination::read(result->path, in);
    Coordination::read(result->version, in);
    result->restored_from_zookeeper_log = true;
    /// It stores version + 1 (which should be, not for request)
    result->version -= 1;

    return result;
}

Coordination::ZooKeeperRequestPtr deserializeCreateSession(ReadBuffer & in)
{
    std::shared_ptr<Coordination::ZooKeeperSessionIDRequest> result = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    int32_t timeout;
    Coordination::read(timeout, in);
    result->session_timeout_ms = timeout;
    result->restored_from_zookeeper_log = true;
    return result;
}

Coordination::ZooKeeperRequestPtr deserializeCloseSession(ReadBuffer & in, bool empty)
{
    std::shared_ptr<Coordination::ZooKeeperCloseRequest> result = std::make_shared<Coordination::ZooKeeperCloseRequest>();
    if (!empty)
    {
        std::vector<std::string> data;
        Coordination::read(data, in);
    }
    result->restored_from_zookeeper_log = true;
    return result;
}

Coordination::ZooKeeperRequestPtr deserializeErrorTxn(ReadBuffer & in)
{
    int32_t error;
    Coordination::read(error, in);
    return nullptr;
}

Coordination::ZooKeeperRequestPtr deserializeSetACLTxn(ReadBuffer & in)
{
    std::shared_ptr<Coordination::ZooKeeperSetACLRequest> result = std::make_shared<Coordination::ZooKeeperSetACLRequest>();

    Coordination::read(result->path, in);
    Coordination::read(result->acls, in);
    Coordination::read(result->version, in);
    /// It stores version + 1 (which should be, not for request)
    result->version -= 1;
    result->restored_from_zookeeper_log = true;

    return result;
}

Coordination::ZooKeeperRequestPtr deserializeMultiTxn(ReadBuffer & in);

Coordination::ZooKeeperRequestPtr deserializeTxnImpl(ReadBuffer & in, bool subtxn, int64_t txn_length = 0)
{
    int32_t type;
    Coordination::read(type, in);
    Coordination::ZooKeeperRequestPtr result = nullptr;
    int32_t sub_txn_length = 0;
    if (subtxn)
        Coordination::read(sub_txn_length, in);

    bool empty_txn = !subtxn && txn_length == 32; /// Possible for old-style CloseTxn's

    if (empty_txn && type != -11)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Empty non close-session transaction found");

    int64_t in_count_before = in.count();

    switch (type)
    {
        case 1:
            result = deserializeCreateTxn(in);
            break;
        case 2:
            result = deserializeDeleteTxn(in);
            break;
        case 5:
            result = deserializeSetTxn(in);
            break;
        case 7:
            result = deserializeSetACLTxn(in);
            break;
        case 13:
            result = deserializeCheckVersionTxn(in);
            break;
        case 14:
            result = deserializeMultiTxn(in);
            break;
        case -10:
            result = deserializeCreateSession(in);
            break;
        case -11:
            result = deserializeCloseSession(in, empty_txn);
            break;
        case -1:
            result = deserializeErrorTxn(in);
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented operation {}", type);
    }

    if (subtxn)
    {
        int64_t bytes_read = in.count() - in_count_before;
        if (bytes_read < sub_txn_length)
            in.ignore(sub_txn_length - bytes_read);
    }

    return result;
}

Coordination::ZooKeeperRequestPtr deserializeMultiTxn(ReadBuffer & in)
{
    int32_t length;
    Coordination::read(length, in);

    std::shared_ptr<Coordination::ZooKeeperMultiRequest> result = std::make_shared<Coordination::ZooKeeperMultiRequest>();
    while (length > 0)
    {
        auto subrequest = deserializeTxnImpl(in, true);
        result->requests.push_back(subrequest);
        length--;
    }
    return result;
}

bool isErrorRequest(Coordination::ZooKeeperRequestPtr request)
{
    return request == nullptr;
}

bool hasErrorsInMultiRequest(Coordination::ZooKeeperRequestPtr request)
{
    if (request == nullptr)
        return true;

    for (const auto & subrequest : dynamic_cast<Coordination::ZooKeeperMultiRequest *>(request.get())->requests) // -V522
        if (subrequest == nullptr)
            return true;
    return false;
}

}

bool deserializeTxn(KeeperStorage & storage, ReadBuffer & in, Poco::Logger * /*log*/)
{
    int64_t checksum;
    Coordination::read(checksum, in);
    /// Zero padding is possible until file end
    if (checksum == 0)
        return false;

    int32_t txn_len;
    Coordination::read(txn_len, in);
    int64_t count_before = in.count();
    int64_t session_id;
    Coordination::read(session_id, in);
    int32_t xid;
    Coordination::read(xid, in);
    int64_t zxid;
    Coordination::read(zxid, in);
    int64_t time;
    Coordination::read(time, in);

    Coordination::ZooKeeperRequestPtr request = deserializeTxnImpl(in, false, txn_len);

    /// Skip all other bytes
    int64_t bytes_read = in.count() - count_before;
    if (bytes_read < txn_len)
        in.ignore(txn_len - bytes_read);

    /// We don't need to apply error requests
    if (isErrorRequest(request))
        return true;

    request->xid = xid;

    if (zxid > storage.zxid)
    {
        /// Separate processing of session id requests
        if (request->getOpNum() == Coordination::OpNum::SessionID)
        {
            const Coordination::ZooKeeperSessionIDRequest & session_id_request = dynamic_cast<const Coordination::ZooKeeperSessionIDRequest &>(*request);
            storage.getSessionID(session_id_request.session_timeout_ms);
        }
        else
        {
            /// Skip failed multirequests
            if (request->getOpNum() == Coordination::OpNum::Multi && hasErrorsInMultiRequest(request))
                return true;

            storage.processRequest(request, session_id, zxid, /* check_acl = */ false);
        }
    }

    return true;
}

void deserializeLogAndApplyToStorage(KeeperStorage & storage, const std::string & log_path, Poco::Logger * log)
{
    ReadBufferFromFile reader(log_path);

    LOG_INFO(log, "Deserializing log {}", log_path);
    deserializeLogMagic(reader);
    LOG_INFO(log, "Header looks OK");

    size_t counter = 0;
    while (!reader.eof() && deserializeTxn(storage, reader, log))
    {
        counter++;
        if (counter % 1000 == 0)
            LOG_INFO(log, "Deserialized txns log: {}", counter);

        int8_t forty_two;
        Coordination::read(forty_two, reader);
        if (forty_two != 0x42)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Forty two check byte ({}) is not equal 0x42", forty_two);
    }

    LOG_INFO(log, "Finished {} deserialization, totally read {} records", log_path, counter);
}

void deserializeLogsAndApplyToStorage(KeeperStorage & storage, const std::string & path, Poco::Logger * log)
{
    namespace fs = std::filesystem;
    std::map<int64_t, std::string> existing_logs;
    for (const auto & p : fs::directory_iterator(path))
    {
        const auto & log_path = p.path();
        if (!log_path.has_filename() || !startsWith(log_path.filename(), "log."))
            continue;
        int64_t zxid = getZxidFromName(log_path);
        existing_logs[zxid] = p.path();
    }

    LOG_INFO(log, "Totally have {} logs", existing_logs.size());

    for (auto [zxid, log_path] : existing_logs)
    {
        if (zxid > storage.zxid)
            deserializeLogAndApplyToStorage(storage, log_path, log);
        else
            LOG_INFO(log, "Skipping log {}, it's ZXID {} is smaller than storages ZXID {}", log_path, zxid, storage.zxid);
    }
}

}
