#include <Interpreters/TransactionLog.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <Interpreters/Context.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/ServerUUID.h>
#include <base/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void tryWriteEventToSystemLog(Poco::Logger * log, ContextPtr context,
                                     TransactionsInfoLogElement::Type type, const TransactionID & tid, CSN csn = Tx::UnknownCSN)
try
{
    auto system_log = context->getTransactionsInfoLog();
    if (!system_log)
        return;

    TransactionsInfoLogElement elem;
    elem.type = type;
    elem.tid = tid;
    elem.csn = csn;
    elem.fillCommonFields(nullptr);
    system_log->add(elem);
}
catch (...)
{
    tryLogCurrentException(log);
}


TransactionLog::TransactionLog()
    : log(&Poco::Logger::get("TransactionLog"))
{
    global_context = Context::getGlobalContextInstance();
    global_context->checkTransactionsAreAllowed();

    zookeeper_path = "/test/clickhouse/txn_log";

    loadLogFromZooKeeper();

    updating_thread = ThreadFromGlobalPool(&TransactionLog::runUpdatingThread, this);
}

TransactionLog::~TransactionLog()
{
    shutdown();
}

void TransactionLog::shutdown()
{
    if (stop_flag.exchange(true))
        return;
    log_updated_event->set();
    latest_snapshot.notify_all();
    updating_thread.join();

    std::lock_guard lock{mutex};
    /// This is required to... you'll never guess - avoid race condition inside Poco::Logger (Coordination::ZooKeeper::log)
    zookeeper.reset();
}

ZooKeeperPtr TransactionLog::getZooKeeper() const
{
    std::lock_guard lock{mutex};
    return zookeeper;
}

UInt64 TransactionLog::parseCSN(const String & csn_node_name)
{
    ReadBufferFromString buf{csn_node_name};
    assertString("csn-", buf);
    UInt64 res;
    readText(res, buf);
    assertEOF(buf);
    return res;
}

TransactionID TransactionLog::parseTID(const String & csn_node_content)
{
    TransactionID tid = Tx::EmptyTID;
    if (csn_node_content.empty())
        return tid;

    ReadBufferFromString buf{csn_node_content};
    tid = TransactionID::read(buf);
    assertEOF(buf);
    return tid;
}

String TransactionLog::writeTID(const TransactionID & tid)
{
    WriteBufferFromOwnString buf;
    TransactionID::write(tid, buf);
    return buf.str();
}


void TransactionLog::loadEntries(Strings::const_iterator beg, Strings::const_iterator end)
{
    std::vector<std::future<Coordination::GetResponse>> futures;
    size_t entries_count = std::distance(beg, end);
    if (!entries_count)
        return;

    String last_entry = *std::prev(end);
    LOG_TRACE(log, "Loading {} entries from {}: {}..{}", entries_count, zookeeper_path, *beg, last_entry);
    futures.reserve(entries_count);
    for (auto it = beg; it != end; ++it)
        futures.emplace_back(zookeeper->asyncGet(fs::path(zookeeper_path) / *it));

    std::vector<std::pair<TIDHash, CSN>> loaded;
    loaded.reserve(entries_count);
    auto it = beg;
    for (size_t i = 0; i < entries_count; ++i, ++it)
    {
        auto res = futures[i].get();
        CSN csn = parseCSN(*it);
        TransactionID tid = parseTID(res.data);
        loaded.emplace_back(tid.getHash(), csn);
        LOG_TEST(log, "Got entry {} -> {}", tid, csn);
    }
    futures.clear();

    /// Use noexcept here to exit on unexpected exceptions (SIGABRT is better that broken state in memory)
    auto insert = [&]() noexcept
    {
        for (const auto & entry : loaded)
            if (entry.first != Tx::EmptyTID.getHash())
            tid_to_csn.emplace(entry.first, entry.second);
        last_loaded_entry = last_entry;
        latest_snapshot = loaded.back().second;
        local_tid_counter = Tx::MaxReservedLocalTID;
    };

    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    std::lock_guard lock{mutex};
    insert();
}

void TransactionLog::loadLogFromZooKeeper()
{
    assert(!zookeeper);
    assert(tid_to_csn.empty());
    assert(last_loaded_entry.empty());
    zookeeper = global_context->getZooKeeper();

    /// We do not write local_tid_counter to disk or zk and maintain it only in memory.
    /// Create empty entry to allocate new CSN to safely start counting from the beginning and avoid TID duplication.
    /// TODO It's possible to skip this step in come cases (especially for multi-host configuration).
    Coordination::Error code = zookeeper->tryCreate(zookeeper_path + "/csn-", "", zkutil::CreateMode::PersistentSequential);
    if (code != Coordination::Error::ZOK)
    {
        assert(code == Coordination::Error::ZNONODE);
        zookeeper->createAncestors(zookeeper_path);
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
        for (size_t i = 0; i <= Tx::MaxReservedCSN; ++i)
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/csn-", "", zkutil::CreateMode::PersistentSequential));
        Coordination::Responses res;
        code = zookeeper->tryMulti(ops, res);
        if (code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, res);
    }

    /// TODO Split log into "subdirectories" to:
    /// 1. fetch it more optimal way (avoid listing all CSNs on further incremental updates)
    /// 2. simplify log rotation
    /// 3. support 64-bit CSNs on top of Apache ZooKeeper (it uses Int32 for sequential numbers)
    Strings entries_list = zookeeper->getChildren(zookeeper_path, nullptr, log_updated_event);
    assert(!entries_list.empty());
    std::sort(entries_list.begin(), entries_list.end());
    loadEntries(entries_list.begin(), entries_list.end());
    assert(!last_loaded_entry.empty());
    assert(latest_snapshot == parseCSN(last_loaded_entry));
    local_tid_counter = Tx::MaxReservedLocalTID;
}

void TransactionLog::runUpdatingThread()
{
    while (true)
    {
        try
        {
            log_updated_event->wait();
            if (stop_flag.load())
                return;

            if (!zookeeper)
            {
                auto new_zookeeper = global_context->getZooKeeper();
                std::lock_guard lock{mutex};
                zookeeper = new_zookeeper;
            }

            loadNewEntries();
        }
        catch (const Coordination::Exception & e)
        {
            tryLogCurrentException(log);
            /// TODO better backoff
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            if (Coordination::isHardwareError(e.code))
            {
                std::lock_guard lock{mutex};
                zookeeper.reset();
            }
            log_updated_event->set();
        }
        catch (...)
        {
            tryLogCurrentException(log);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            log_updated_event->set();
        }
    }
}

void TransactionLog::loadNewEntries()
{
    Strings entries_list = zookeeper->getChildren(zookeeper_path, nullptr, log_updated_event);
    assert(!entries_list.empty());
    std::sort(entries_list.begin(), entries_list.end());
    auto it = std::upper_bound(entries_list.begin(), entries_list.end(), last_loaded_entry);
    loadEntries(it, entries_list.end());
    assert(last_loaded_entry == entries_list.back());
    assert(latest_snapshot == parseCSN(last_loaded_entry));
    latest_snapshot.notify_all();
}


Snapshot TransactionLog::getLatestSnapshot() const
{
    return latest_snapshot.load();
}

MergeTreeTransactionPtr TransactionLog::beginTransaction()
{
    MergeTreeTransactionPtr txn;
    {
        std::lock_guard lock{running_list_mutex};
        Snapshot snapshot = latest_snapshot.load();
        LocalTID ltid = 1 + local_tid_counter.fetch_add(1);
        txn = std::make_shared<MergeTreeTransaction>(snapshot, ltid, ServerUUID::get());
        bool inserted = running_list.try_emplace(txn->tid.getHash(), txn).second;
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "I's a bug: TID {} {} exists", txn->tid.getHash(), txn->tid);
        txn->snapshot_in_use_it = snapshots_in_use.insert(snapshots_in_use.end(), snapshot);
    }

    LOG_TEST(log, "Beginning transaction {} ({})", txn->tid, txn->tid.getHash());
    tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::BEGIN, txn->tid);

    return txn;
}

CSN TransactionLog::commitTransaction(const MergeTreeTransactionPtr & txn)
{
    txn->beforeCommit();

    CSN new_csn;
    if (txn->isReadOnly())
    {
        LOG_TEST(log, "Closing readonly transaction {}", txn->tid);
        new_csn = txn->snapshot;
        tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::COMMIT, txn->tid, new_csn);
    }
    else
    {
        LOG_TEST(log, "Committing transaction {}", txn->dumpDescription());
        /// TODO handle connection loss
        /// TODO support batching
        auto current_zookeeper = getZooKeeper();
        String path_created = current_zookeeper->create(zookeeper_path + "/csn-", writeTID(txn->tid), zkutil::CreateMode::PersistentSequential);    /// Commit point
        new_csn = parseCSN(path_created.substr(zookeeper_path.size() + 1));

        LOG_INFO(log, "Transaction {} committed with CSN={}", txn->tid, new_csn);
        tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::COMMIT, txn->tid, new_csn);

        /// Wait for committed changes to become actually visible, so the next transaction will see changes
        /// TODO it's optional, add a setting for this
        auto current_latest_snapshot = latest_snapshot.load();
        while (current_latest_snapshot < new_csn && !stop_flag)
        {
            latest_snapshot.wait(current_latest_snapshot);
            current_latest_snapshot = latest_snapshot.load();
        }
    }

    txn->afterCommit(new_csn);

    {
        std::lock_guard lock{running_list_mutex};
        bool removed = running_list.erase(txn->tid.getHash());
        if (!removed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "I's a bug: TID {} {} doesn't exist", txn->tid.getHash(), txn->tid);
        snapshots_in_use.erase(txn->snapshot_in_use_it);
    }

    return new_csn;
}

void TransactionLog::rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept
{
    LOG_TRACE(log, "Rolling back transaction {}{}", txn->tid,
              std::uncaught_exceptions() ? fmt::format(" due to uncaught exception (code: {})", getCurrentExceptionCode()) : "");

    if (!txn->rollback())
        return;

    {
        std::lock_guard lock{running_list_mutex};
        bool removed = running_list.erase(txn->tid.getHash());
        if (!removed)
            abort();
        snapshots_in_use.erase(txn->snapshot_in_use_it);
    }

    tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::ROLLBACK, txn->tid);
}

MergeTreeTransactionPtr TransactionLog::tryGetRunningTransaction(const TIDHash & tid)
{
    std::lock_guard lock{running_list_mutex};
    auto it = running_list.find(tid);
    if (it == running_list.end())
        return nullptr;
    return it->second;
}

CSN TransactionLog::getCSN(const TransactionID & tid)
{
    return getCSN(tid.getHash());
}

CSN TransactionLog::getCSN(const TIDHash & tid)
{
    /// Avoid creation of the instance if transactions are not actually involved
    if (tid == Tx::PrehistoricTID.getHash())
        return Tx::PrehistoricCSN;

    return instance().getCSNImpl(tid);
}

CSN TransactionLog::getCSNImpl(const TIDHash & tid) const
{
    assert(tid);
    assert(tid != Tx::EmptyTID.getHash());

    std::lock_guard lock{mutex};
    auto it = tid_to_csn.find(tid);
    if (it == tid_to_csn.end())
        return Tx::UnknownCSN;
    return it->second;
}

Snapshot TransactionLog::getOldestSnapshot() const
{
    std::lock_guard lock{running_list_mutex};
    if (snapshots_in_use.empty())
        return getLatestSnapshot();
    return snapshots_in_use.front();
}

TransactionLog::TransactionsList TransactionLog::getTransactionsList() const
{
    std::lock_guard lock{running_list_mutex};
    return running_list;
}

}
