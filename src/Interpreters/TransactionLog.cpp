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
#include <Common/logger_useful.h>


/// It's used in critical places to exit on unexpected exceptions.
/// SIGABRT is usually better that broken state in memory with unpredictable consequences.
#define NOEXCEPT_SCOPE SCOPE_EXIT({ if (std::uncaught_exceptions()) { tryLogCurrentException("NOEXCEPT_SCOPE"); abort(); } })

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

    zookeeper_path = global_context->getConfigRef().getString("transaction_log.zookeeper_path", "/clickhouse/txn");
    zookeeper_path_log = zookeeper_path + "/log";

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

UInt64 TransactionLog::deserializeCSN(const String & csn_node_name)
{
    ReadBufferFromString buf{csn_node_name};
    assertString("csn-", buf);
    UInt64 res;
    readText(res, buf);
    assertEOF(buf);
    return res;
}

String TransactionLog::serializeCSN(CSN csn)
{
    return zkutil::getSequentialNodeName("csn-", csn);
}

TransactionID TransactionLog::deserializeTID(const String & csn_node_content)
{
    TransactionID tid = Tx::EmptyTID;
    if (csn_node_content.empty())
        return tid;

    ReadBufferFromString buf{csn_node_content};
    tid = TransactionID::read(buf);
    assertEOF(buf);
    return tid;
}

String TransactionLog::serializeTID(const TransactionID & tid)
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
    LOG_TRACE(log, "Loading {} entries from {}: {}..{}", entries_count, zookeeper_path_log, *beg, last_entry);
    futures.reserve(entries_count);
    for (auto it = beg; it != end; ++it)
        futures.emplace_back(zookeeper->asyncGet(fs::path(zookeeper_path_log) / *it));

    std::vector<std::pair<TIDHash, CSNEntry>> loaded;
    loaded.reserve(entries_count);
    auto it = beg;
    for (size_t i = 0; i < entries_count; ++i, ++it)
    {
        auto res = futures[i].get();
        CSN csn = deserializeCSN(*it);
        TransactionID tid = deserializeTID(res.data);
        loaded.emplace_back(tid.getHash(), CSNEntry{csn, tid});
        LOG_TEST(log, "Got entry {} -> {}", tid, csn);
    }
    futures.clear();

    NOEXCEPT_SCOPE;
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    std::lock_guard lock{mutex};
    for (const auto & entry : loaded)
    {
        if (entry.first == Tx::EmptyTID.getHash())
            continue;

        tid_to_csn.emplace(entry.first, entry.second);
    }
    last_loaded_entry = last_entry;
    latest_snapshot = loaded.back().second.csn;
    local_tid_counter = Tx::MaxReservedLocalTID;
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
    Coordination::Error code = zookeeper->tryCreate(zookeeper_path_log + "/csn-", "", zkutil::CreateMode::PersistentSequential);
    if (code != Coordination::Error::ZOK)
    {
        /// Log probably does not exist, create it
        assert(code == Coordination::Error::ZNONODE);
        zookeeper->createAncestors(zookeeper_path_log);
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/tail_ptr", serializeCSN(Tx::MaxReservedCSN), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path_log, "", zkutil::CreateMode::Persistent));

        /// Fast-forward sequential counter to skip reserved CSNs
        for (size_t i = 0; i <= Tx::MaxReservedCSN; ++i)
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path_log + "/csn-", "", zkutil::CreateMode::PersistentSequential));
        Coordination::Responses res;
        code = zookeeper->tryMulti(ops, res);
        if (code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, res);
    }

    /// TODO Split log into "subdirectories" to:
    /// 1. fetch it more optimal way (avoid listing all CSNs on further incremental updates)
    /// 2. simplify log rotation
    /// 3. support 64-bit CSNs on top of Apache ZooKeeper (it uses Int32 for sequential numbers)
    Strings entries_list = zookeeper->getChildren(zookeeper_path_log, nullptr, log_updated_event);
    assert(!entries_list.empty());
    std::sort(entries_list.begin(), entries_list.end());
    loadEntries(entries_list.begin(), entries_list.end());
    assert(!last_loaded_entry.empty());
    assert(latest_snapshot == deserializeCSN(last_loaded_entry));
    local_tid_counter = Tx::MaxReservedLocalTID;

    tail_ptr = deserializeCSN(zookeeper->get(zookeeper_path + "/tail_ptr"));
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

            if (getZooKeeper()->expired())
            {
                auto new_zookeeper = global_context->getZooKeeper();
                std::lock_guard lock{mutex};
                zookeeper = new_zookeeper;
            }

            loadNewEntries();
            removeOldEntries();
        }
        catch (const Coordination::Exception &)
        {
            tryLogCurrentException(log);
            /// TODO better backoff
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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
    Strings entries_list = zookeeper->getChildren(zookeeper_path_log, nullptr, log_updated_event);
    assert(!entries_list.empty());
    std::sort(entries_list.begin(), entries_list.end());
    auto it = std::upper_bound(entries_list.begin(), entries_list.end(), last_loaded_entry);
    loadEntries(it, entries_list.end());
    assert(last_loaded_entry == entries_list.back());
    assert(latest_snapshot == deserializeCSN(last_loaded_entry));
    latest_snapshot.notify_all();
}

void TransactionLog::removeOldEntries()
{
    /// Try to update tail pointer. It's (almost) safe to set it to the oldest snapshot
    /// because if a transaction released snapshot, then CSN is already written into metadata.
    /// Why almost? Because on server startup we do not have the oldest snapshot (it's simply equal to the latest one),
    /// but it's possible that some CSNs are not written into data parts (and we will write them during startup).
    if (!global_context->isServerCompletelyStarted())
        return;

    /// Also similar problem is possible if some table was not attached during startup (for example, if table is detached permanently).
    /// Also we write CSNs into data parts without fsync, so it's theoretically possible that we wrote CSN, finished transaction,
    /// removed its entry from the log, but after that server restarts and CSN is not actually saved to metadata on disk.
    /// We should store a bit more entries in ZK and keep outdated entries for a while.

    /// TODO we will need a bit more complex logic for multiple hosts
    Coordination::Stat stat;
    CSN old_tail_ptr = deserializeCSN(zookeeper->get(zookeeper_path + "/tail_ptr", &stat));
    CSN new_tail_ptr = getOldestSnapshot();
    if (new_tail_ptr < old_tail_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected tail_ptr {}, oldest snapshot is {}, it's a bug", old_tail_ptr, new_tail_ptr);
    else if (new_tail_ptr == old_tail_ptr)
        return;

    /// (it's not supposed to fail with ZBADVERSION while there is only one host)
    LOG_TRACE(log, "Updating tail_ptr from {} to {}", old_tail_ptr, new_tail_ptr);
    zookeeper->set(zookeeper_path + "/tail_ptr", serializeCSN(new_tail_ptr), stat.version);
    tail_ptr.store(new_tail_ptr);

    /// Now we can find and remove old entries
    TIDMap tids;
    {
        std::lock_guard lock{mutex};
        tids = tid_to_csn;
    }

    /// TODO support batching
    std::vector<TIDHash> removed_entries;
    CSN latest_entry_csn = latest_snapshot.load();
    for (const auto & elem : tids)
    {
        /// Definitely not safe to remove
        if (new_tail_ptr <= elem.second.tid.start_csn)
            continue;

        /// Keep at least one node (the latest one we fetched)
        if (elem.second.csn == latest_entry_csn)
            continue;

        LOG_TEST(log, "Removing entry {} -> {}", elem.second.tid, elem.second.csn);
        auto code = zookeeper->tryRemove(zookeeper_path_log + "/" + serializeCSN(elem.second.csn));
        if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE)
            removed_entries.push_back(elem.first);
    }

    std::lock_guard lock{mutex};
    for (const auto & tid_hash : removed_entries)
        tid_to_csn.erase(tid_hash);
}

CSN TransactionLog::getLatestSnapshot() const
{
    return latest_snapshot.load();
}

MergeTreeTransactionPtr TransactionLog::beginTransaction()
{
    MergeTreeTransactionPtr txn;
    {
        std::lock_guard lock{running_list_mutex};
        CSN snapshot = latest_snapshot.load();
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
    /// Some precommit checks, may throw
    auto committing_lock = txn->beforeCommit();

    CSN new_csn;
    if (txn->isReadOnly())
    {
        /// Don't need to allocate CSN in ZK for readonly transactions, it's safe to use snapshot/start_csn as "commit" timestamp
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
        String path_created = current_zookeeper->create(zookeeper_path_log + "/csn-", serializeTID(txn->tid), zkutil::CreateMode::PersistentSequential);    /// Commit point
        NOEXCEPT_SCOPE;

        /// FIXME Transactions: Sequential node numbers in ZooKeeper are Int32, but 31 bit is not enough for production use
        /// (overflow is possible in a several weeks/months of active usage)
        new_csn = deserializeCSN(path_created.substr(zookeeper_path_log.size() + 1));

        LOG_INFO(log, "Transaction {} committed with CSN={}", txn->tid, new_csn);
        tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::COMMIT, txn->tid, new_csn);

        /// Wait for committed changes to become actually visible, so the next transaction in this session will see the changes
        /// TODO it's optional, add a setting for this
        auto current_latest_snapshot = latest_snapshot.load();
        while (current_latest_snapshot < new_csn && !stop_flag)
        {
            latest_snapshot.wait(current_latest_snapshot);
            current_latest_snapshot = latest_snapshot.load();
        }
    }

    /// Write allocated CSN, so we will be able to cleanup log in ZK. This method is noexcept.
    txn->afterCommit(new_csn);

    {
        /// Finally we can remove transaction from the list and release the snapshot
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
    {
        /// Transaction was cancelled concurrently, it's already rolled back.
        assert(txn->csn == Tx::RolledBackCSN);
        return;
    }

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
        return NO_TRANSACTION_PTR;
    return it->second;
}

CSN TransactionLog::getCSN(const TransactionID & tid)
{
    /// Avoid creation of the instance if transactions are not actually involved
    if (tid == Tx::PrehistoricTID)
        return Tx::PrehistoricCSN;
    return instance().getCSNImpl(tid.getHash());
}

CSN TransactionLog::getCSN(const TIDHash & tid)
{
    /// Avoid creation of the instance if transactions are not actually involved
    if (tid == Tx::PrehistoricTID.getHash())
        return Tx::PrehistoricCSN;
    return instance().getCSNImpl(tid);
}

CSN TransactionLog::getCSNImpl(const TIDHash & tid_hash) const
{
    assert(tid_hash);
    assert(tid_hash != Tx::EmptyTID.getHash());

    std::lock_guard lock{mutex};
    auto it = tid_to_csn.find(tid_hash);
    if (it != tid_to_csn.end())
        return it->second.csn;

    return Tx::UnknownCSN;
}

void TransactionLog::assertTIDIsNotOutdated(const TransactionID & tid)
{
    if (tid == Tx::PrehistoricTID)
        return;

    /// Ensure that we are not trying to get CSN for TID that was already removed from the log
    CSN tail = instance().tail_ptr.load();
    if (tail <= tid.start_csn)
        return;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get CSN for too old TID {}, current tail_ptr is {}, probably it's a bug", tid, tail);
}

CSN TransactionLog::getOldestSnapshot() const
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
