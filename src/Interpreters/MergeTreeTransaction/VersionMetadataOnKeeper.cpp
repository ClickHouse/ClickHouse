#include <Interpreters/MergeTreeTransaction/VersionMetadataOnKeeper.h>

#include <optional>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int METADATA_MISMATCH;
extern const int NOT_FOUND_NODE;
}

VersionMetadataOnKeeper::VersionMetadataOnKeeper(
    IMergeTreeDataPart * merge_tree_data_part_, GetZooKeeperFunc get_zk_func_, String metadata_path_, String lock_path_)
    : VersionMetadata(merge_tree_data_part_)
    , get_zk_func(get_zk_func_)
    , metadata_path(std::move(metadata_path_))
    , lock_path(std::move(lock_path_))
{
    log = ::getLogger("VersionMetadataOnKeeper");
    LOG_DEBUG(log, "Object {}, metadata_path {}, lock_path {}", getObjectName(), metadata_path, lock_path);
    auto zookeeper = get_zk_func();
    if (zookeeper->exists(metadata_path))
        loadAndVerifyMetadata(log);
}

VersionMetadataOnKeeper::VersionMetadataOnKeeper(IMergeTreeDataPart * merge_tree_data_part_, GetZooKeeperFunc get_zk_func_)
    : VersionMetadata(merge_tree_data_part_)
    , get_zk_func(get_zk_func_)
{
    log = ::getLogger("VersionMetadataOnKeeper");
    auto & data_part_storage = merge_tree_data_part->getDataPartStorage();

    LOG_DEBUG(
        log,
        "Object {}, TXN_VERSION_METADATA_FILE_NAME {}, exist: {}",
        getObjectName(),
        TXN_VERSION_METADATA_FILE_NAME,
        data_part_storage.existsFile(TXN_VERSION_METADATA_FILE_NAME));
    if (data_part_storage.existsFile(TXN_VERSION_METADATA_FILE_NAME))
    {
        auto buf = data_part_storage.readFile(
            TXN_VERSION_METADATA_FILE_NAME, Context::getGlobalContextInstance()->getReadSettings(), std::nullopt);
        readStringUntilEOF(txn_keeper_node, *buf);
    }
    else
    {
        txn_keeper_node = getRandomASCIIString(64);
    }

    metadata_path = fmt::format("/clickhouse/txn/version/{}/txn_metadata", txn_keeper_node);
    lock_path = fmt::format("/clickhouse/txn/version/{}/lock", txn_keeper_node);

    auto zookeeper = Context::getGlobalContextInstance()->getZooKeeper();
    String version_path = fmt::format("/clickhouse/txn/version/{}", txn_keeper_node);
    zookeeper->createAncestors("/clickhouse/txn/version");
    zookeeper->createIfNotExists("/clickhouse/txn/version", "");
    zookeeper->createIfNotExists(version_path, "");

    LOG_DEBUG(log, "Object {}, metadata_path {}, lock_path {}", getObjectName(), metadata_path, lock_path);
    if (zookeeper->exists(metadata_path))
        loadAndVerifyMetadata(log);
}

void VersionMetadataOnKeeper::loadMetadata()
{
    auto zookeeper = get_zk_func();
    String content;
    Coordination::Stat stat;
    if (zookeeper->tryGet(metadata_path, content, &stat))
    {
        LOG_DEBUG(log, "Object {}, metadata_version {}, load metadata content\n{}", getObjectName(), stat.version, content);
        ReadBufferFromString buf(content);
        readFromBuffer(buf);
        metadata_version = stat.version;
        return;
    }
    LOG_TEST(log, "Object {}, no metadata", getObjectName());
    metadata_version = std::nullopt;
    setCreationTID(Tx::PrehistoricTID, nullptr);
    setCreationCSN(Tx::PrehistoricCSN);
}

void VersionMetadataOnKeeper::storeMetadata(bool) const
{
    if (!txn_keeper_node.empty() && !merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME))
    {
        LOG_TEST(log, "Object {}, store {}, keeper_node {}", getObjectName(), TXN_VERSION_METADATA_FILE_NAME, txn_keeper_node);
        auto out_metadata = merge_tree_data_part->getDataPartStorage().writeFile(
            TXN_VERSION_METADATA_FILE_NAME, 4096, Context::getGlobalContextInstance()->getWriteSettings());

        writeText(txn_keeper_node, *out_metadata);
        out_metadata->finalize();
        out_metadata->sync();
    }
    auto zookeeper = get_zk_func();

    String content;
    WriteBufferFromString buf(content);
    writeToBuffer(buf);
    buf.finalize();

    if (!metadata_version.has_value())
    {
        LOG_TEST(log, "Object {}, metadata_version {}, create metadata content,", getObjectName(), metadata_path);
        zookeeper->create(metadata_path, content, zkutil::CreateMode::Persistent);
        metadata_version = 0;
    }
    else
    {
        Coordination::Stat stat;
        zookeeper->set(metadata_path, content, *metadata_version, &stat);
        metadata_version = stat.version;
    }
    LOG_DEBUG(log, "Object {}, metadata_version {}, store metadata content\n{},", getObjectName(), *metadata_version, content);
}


bool VersionMetadataOnKeeper::tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id)
{
    LOG_DEBUG(
        log, "Table {}, part {}, lock removal TID {}, {}", context.table.getNameForLogs(), context.part_name, tid.getHash(), lock_path);

    chassert(!tid.isEmpty());
    chassert(!creation_tid.isEmpty());
    TIDHash removal_lock_value = tid.getHash();

    auto zookeeper = get_zk_func();

    String content;
    Coordination::Stat stat;
    if (!zookeeper->tryGet(lock_path, content, &stat))
    {
        zookeeper->create(lock_path, std::to_string(removal_lock_value), zkutil::CreateMode::Persistent);
        tryWriteEventToSystemLog(log, TransactionsInfoLogElement::LOCK_PART, tid, context);
        return true;
    }

    if (content.empty())
    {
        zookeeper->set(lock_path, std::to_string(removal_lock_value), stat.version);
        tryWriteEventToSystemLog(log, TransactionsInfoLogElement::LOCK_PART, tid, context);
        return true;
    }

    auto locked_by = DB::parse<TIDHash>(content);
    if (locked_by == removal_lock_value)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Tried to lock object {}, lock_path {} for removal second time by {}",
            getObjectName(),
            lock_path,
            tid);

    if (locked_by_id)
        *locked_by_id = locked_by;
    return false;
}


void VersionMetadataOnKeeper::unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(
        log,
        "Unlocking removal_tid by {}, table: {}, part: {}, lock_path {},",
        tid,
        context.table.getNameForLogs(),
        context.part_name,
        lock_path);
    chassert(!tid.isEmpty());
    TIDHash removal_lock_value = tid.getHash();

    auto zookeeper = get_zk_func();

    String content;
    Coordination::Stat stat;

    if (!zookeeper->tryGet(lock_path, content, &stat))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot unlock removal_tid, it's a bug. Current: {} {}, actual: unlocked", removal_lock_value, tid);

    auto locked_by = DB::parse<TIDHash>(content);
    auto throw_cannot_unlock = [&]()
    {
        auto locked_by_txn = TransactionLog::instance().tryGetRunningTransaction(locked_by);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unlock removal_tid, it's a bug. Current: {} {}, actual: {} {}",
            removal_lock_value,
            tid,
            locked_by,
            locked_by_txn ? locked_by_txn->tid : Tx::EmptyTID);
    };


    if (locked_by != removal_lock_value)
        throw_cannot_unlock();

    zookeeper->set(lock_path, "", stat.version);
    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::UNLOCK_PART, tid, context);
}


bool VersionMetadataOnKeeper::isRemovalTIDLocked() const
{
    auto zookeeper = get_zk_func();
    String content;
    return zookeeper->tryGet(lock_path, content) && !content.empty();
}

TIDHash VersionMetadataOnKeeper::getRemovalTIDLock() const
{
    String content;
    auto zookeeper = get_zk_func();
    if (!zookeeper->tryGet(lock_path, content) || content.empty())
        return 0;

    return DB::parse<TIDHash>(content);
}

bool VersionMetadataOnKeeper::hasStoredMetadata() const
{
    auto zookeeper = get_zk_func();
    return zookeeper->exists(metadata_path);
}

void VersionMetadataOnKeeper::setRemovalTIDLock(TIDHash removal_tid_lock_hash)
{
    auto zookeeper = get_zk_func();
    if (removal_tid_lock_hash)
    {
        if (!metadata_version.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to lock removal_tid before loading metadata");

        // For lock of removal TID, it should not be updated as it is stored permanently on Keeper.
        auto locked_by = getRemovalTIDLock();
        if (removal_tid_lock_hash != locked_by)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Removal TID lock is mismatched, expected {}, actual {}", removal_tid_lock_hash, locked_by);
        return;
    }

    if (!zookeeper->exists(lock_path))
        return;

    Coordination::Requests ops;
    if (metadata_version.has_value())
        ops.emplace_back(zkutil::makeCheckRequest(metadata_path, *metadata_version));
    ops.emplace_back(zkutil::makeRemoveRequest(lock_path, -1));

    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    zkutil::KeeperMultiException::check(code, ops, responses);
}

void VersionMetadataOnKeeper::storeCreationCSNToStoredMetadataImpl()
{
    LOG_TEST(merge_tree_data_part->storage.log, "Object {}, store creation_csn {}", getObjectName(), getCreationCSN());

    if (!metadata_version.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Appending creation_csn before loading metadata");

    auto zookeeper = get_zk_func();

    String content;
    Coordination::Stat stat;
    zookeeper->tryGet(metadata_path, content, &stat);

    if (stat.version != *metadata_version)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Object {}, etadata mismatched, current {}, holding {}",
            getObjectName(),
            stat.version,
            *metadata_version);

    WriteBufferFromString buf(content, AppendModeTag{});
    writeCreationCSNToBuffer(buf);
    buf.finalize();

    Coordination::Stat new_stat;
    zookeeper->set(metadata_path, content, stat.version, &new_stat);
    metadata_version = new_stat.version;
}

void VersionMetadataOnKeeper::storeRemovalCSNToStoredMetadataImpl()
{
    LOG_TEST(merge_tree_data_part->storage.log, "Object {}, store removal_csn {}", getObjectName(), getRemovalCSN());

    if (!metadata_version.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storing removal_csn before loading metadata");

    auto zookeeper = get_zk_func();

    String content;
    Coordination::Stat stat;
    zookeeper->tryGet(metadata_path, content, &stat);

    if (stat.version != *metadata_version)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Object {}, metadata mismatched, current {}, holding {}",
            getObjectName(),
            stat.version,
            *metadata_version);

    WriteBufferFromString buf(content, AppendModeTag{});
    writeRemovalCSNToBuffer(buf);
    buf.finalize();

    Coordination::Stat new_stat;
    zookeeper->set(metadata_path, content, stat.version, &new_stat);
    metadata_version = new_stat.version;
}


void VersionMetadataOnKeeper::storeRemovalTIDToStoredMetadataImpl()
{
    LOG_TEST(
        merge_tree_data_part->storage.log,
        "Storing removal TID for {} (creation: {}, removal {})",
        getObjectName(),
        creation_tid,
        removal_tid);

    if (!metadata_version.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Appending removal TID before loading metadata");

    auto zookeeper = get_zk_func();

    String content;
    Coordination::Stat stat;
    zookeeper->tryGet(metadata_path, content, &stat);

    if (stat.version != *metadata_version)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Object {}, etadata mismatched, current {}, holding {}",
            getObjectName(),
            stat.version,
            *metadata_version);

    WriteBufferFromString buf(content, AppendModeTag{});
    writeRemovalTIDToBuffer(buf, removal_tid);
    buf.finalize();

    Coordination::Stat new_stat;
    zookeeper->set(metadata_path, content, stat.version, &new_stat);
    metadata_version = new_stat.version;
}


VersionMetadata::Info VersionMetadataOnKeeper::readStoredMetadata(String & content) const
{
    auto zookeeper = get_zk_func();
    if (!zookeeper->tryGet(metadata_path, content))
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "No node {}", metadata_path);

    ReadBufferFromString str_buf{content};
    return readFromBufferHelper(str_buf);
}
}
