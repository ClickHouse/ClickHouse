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
#include <Common/noexcept_scope.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
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
    system_log->add(std::move(elem));
}
catch (...)
{
    tryLogCurrentException(log);
}


TransactionLog::TransactionLog()
    : global_context(Context::getGlobalContextInstance())
    , log(&Poco::Logger::get("TransactionLog"))
    , zookeeper_path(global_context->getConfigRef().getString("transaction_log.zookeeper_path", "/clickhouse/txn"))
    , zookeeper_path_log(zookeeper_path + "/log")
    , fault_probability_before_commit(global_context->getConfigRef().getDouble("transaction_log.fault_probability_before_commit", 0))
    , fault_probability_after_commit(global_context->getConfigRef().getDouble("transaction_log.fault_probability_after_commit", 0))
{
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
    size_t entries_count = std::distance(beg, end);
    if (!entries_count)
        return;

    String last_entry = *std::prev(end);
    LOG_TRACE(log, "Loading {} entries from {}: {}..{}", entries_count, zookeeper_path_log, *beg, last_entry);
    std::vector<std::string> entry_paths;
    entry_paths.reserve(entries_count);
    for (auto it = beg; it != end; ++it)
        entry_paths.emplace_back(fs::path(zookeeper_path_log) / *it);

    auto entries = TSA_READ_ONE_THREAD(zookeeper)->get(entry_paths);
    std::vector<std::pair<TIDHash, CSNEntry>> loaded;
    loaded.reserve(entries_count);
    auto it = beg;
    for (size_t i = 0; i < entries_count; ++i, ++it)
    {
        auto res = entries[i];
        CSN csn = deserializeCSN(*it);
        TransactionID tid = deserializeTID(res.data);
        loaded.emplace_back(tid.getHash(), CSNEntry{csn, tid});
        LOG_TEST(log, "Got entry {} -> {}", tid, csn);
    }

    NOEXCEPT_SCOPE_STRICT({
        std::lock_guard lock{mutex};
        for (const auto & entry : loaded)
        {
            if (entry.first == Tx::EmptyTID.getHash())
                continue;

            tid_to_csn.emplace(entry.first, entry.second);
        }
        last_loaded_entry = last_entry;
    });

    {
        std::lock_guard lock{running_list_mutex};
        latest_snapshot = loaded.back().second.csn;
        local_tid_counter = Tx::MaxReservedLocalTID;
    }
}

void TransactionLog::loadLogFromZooKeeper()
{
    chassert(!zookeeper);
    chassert(tid_to_csn.empty());
    chassert(last_loaded_entry.empty());
    zookeeper = global_context->getZooKeeper();

    /// We do not write local_tid_counter to disk or zk and maintain it only in memory.
    /// Create empty entry to allocate new CSN to safely start counting from the beginning and avoid TID duplication.
    /// TODO It's possible to skip this step in come cases (especially for multi-host configuration).
    Coordination::Error code = zookeeper->tryCreate(zookeeper_path_log + "/csn-", "", zkutil::CreateMode::PersistentSequential);
    if (code != Coordination::Error::ZOK)
    {
        /// Log probably does not exist, create it
        chassert(code == Coordination::Error::ZNONODE);
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
    chassert(!entries_list.empty());
    ::sort(entries_list.begin(), entries_list.end());
    loadEntries(entries_list.begin(), entries_list.end());
    chassert(!last_loaded_entry.empty());
    chassert(latest_snapshot == deserializeCSN(last_loaded_entry));
    local_tid_counter = Tx::MaxReservedLocalTID;

    tail_ptr = deserializeCSN(zookeeper->get(zookeeper_path + "/tail_ptr"));
}

void TransactionLog::runUpdatingThread()
{
    while (true)
    {
        try
        {
            /// Do not wait if we have some transactions to finalize
            if (TSA_READ_ONE_THREAD(unknown_state_list_loaded).empty())
                log_updated_event->wait();

            if (stop_flag.load())
                return;

            bool connection_loss = getZooKeeper()->expired();
            if (connection_loss)
            {
                auto new_zookeeper = global_context->getZooKeeper();
                {
                    std::lock_guard lock{mutex};
                    zookeeper = new_zookeeper;
                }

                /// It's possible that we connected to different [Zoo]Keeper instance
                /// so we may read a bit stale state.
                TSA_READ_ONE_THREAD(zookeeper)->sync(zookeeper_path_log);
            }

            loadNewEntries();
            removeOldEntries();
            tryFinalizeUnknownStateTransactions();
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
    Strings entries_list = TSA_READ_ONE_THREAD(zookeeper)->getChildren(zookeeper_path_log, nullptr, log_updated_event);
    chassert(!entries_list.empty());
    ::sort(entries_list.begin(), entries_list.end());
    auto it = std::upper_bound(entries_list.begin(), entries_list.end(), TSA_READ_ONE_THREAD(last_loaded_entry));
    loadEntries(it, entries_list.end());
    chassert(TSA_READ_ONE_THREAD(last_loaded_entry) == entries_list.back());
    chassert(latest_snapshot == deserializeCSN(TSA_READ_ONE_THREAD(last_loaded_entry)));
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
    CSN old_tail_ptr = deserializeCSN(TSA_READ_ONE_THREAD(zookeeper)->get(zookeeper_path + "/tail_ptr", &stat));
    CSN new_tail_ptr = getOldestSnapshot();
    if (new_tail_ptr < old_tail_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected tail_ptr {}, oldest snapshot is {}, it's a bug", old_tail_ptr, new_tail_ptr);
    else if (new_tail_ptr == old_tail_ptr)
        return;

    /// (it's not supposed to fail with ZBADVERSION while there is only one host)
    LOG_TRACE(log, "Updating tail_ptr from {} to {}", old_tail_ptr, new_tail_ptr);
    TSA_READ_ONE_THREAD(zookeeper)->set(zookeeper_path + "/tail_ptr", serializeCSN(new_tail_ptr), stat.version);
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
        auto code = TSA_READ_ONE_THREAD(zookeeper)->tryRemove(zookeeper_path_log + "/" + serializeCSN(elem.second.csn));
        if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE)
            removed_entries.push_back(elem.first);
    }

    std::lock_guard lock{mutex};
    for (const auto & tid_hash : removed_entries)
        tid_to_csn.erase(tid_hash);
}

void TransactionLog::tryFinalizeUnknownStateTransactions()
{
    /// We just recovered connection to [Zoo]Keeper.
    /// Check if transactions in unknown state were actually committed or not and finalize or rollback them.
    UnknownStateList list;
    {
        /// We must be sure that the corresponding CSN entry is loaded from ZK.
        /// Otherwise we may accidentally rollback committed transaction in case of race condition like this:
        ///   - runUpdatingThread: loaded some entries, ready to call tryFinalizeUnknownStateTransactions()
        ///   - commitTransaction: creates CSN entry in the log (txn is committed)
        ///   - [session expires]
        ///   - commitTransaction: catches Coordination::Exception (maybe due to fault injection), appends txn to unknown_state_list
        ///   - runUpdatingThread: calls tryFinalizeUnknownStateTransactions(), fails to find CSN for this txn, rolls it back
        /// So all CSN entries that might exist at the moment of appending txn to unknown_state_list
        /// must be loaded from ZK before we start finalize that txn.
        /// That's why we use two lists here:
        ///    1. At first we put txn into unknown_state_list
        ///    2. We move it to unknown_state_list_loaded when runUpdatingThread done at least one iteration
        ///    3. Then we can safely finalize txns from unknown_state_list_loaded, because all required entries are loaded
        std::lock_guard lock{running_list_mutex};
        std::swap(list, unknown_state_list);
        std::swap(list, unknown_state_list_loaded);
    }

    for (auto & [txn, state_guard] : list)
    {
        /// CSNs must be already loaded, only need to check if the corresponding mapping exists.
        if (auto csn = getCSN(txn->tid))
        {
            finalizeCommittedTransaction(txn.get(), csn, state_guard);
        }
        else
        {
            assertTIDIsNotOutdated(txn->tid);
            state_guard = {};
            rollbackTransaction(txn->shared_from_this());
        }
    }
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
        auto snapshot_lock = snapshots_in_use.insert(snapshots_in_use.end(), snapshot);
        txn = std::make_shared<MergeTreeTransaction>(snapshot, ltid, ServerUUID::get(), snapshot_lock);
        bool inserted = running_list.try_emplace(txn->tid.getHash(), txn).second;
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "I's a bug: TID {} {} exists", txn->tid.getHash(), txn->tid);
    }

    LOG_TEST(log, "Beginning transaction {} ({})", txn->tid, txn->tid.getHash());
    tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::BEGIN, txn->tid);

    return txn;
}

CSN TransactionLog::commitTransaction(const MergeTreeTransactionPtr & txn, bool throw_on_unknown_status)
{
    /// Some precommit checks, may throw
    auto state_guard = txn->beforeCommit();

    CSN allocated_csn = Tx::UnknownCSN;
    if (txn->isReadOnly())
    {
        /// Don't need to allocate CSN in ZK for readonly transactions, it's safe to use snapshot/start_csn as "commit" timestamp
        LOG_TEST(log, "Closing readonly transaction {}", txn->tid);
    }
    else
    {
        LOG_TEST(log, "Committing transaction {}", txn->dumpDescription());
        /// TODO support batching
        auto current_zookeeper = getZooKeeper();
        String csn_path_created;
        try
        {
            if (unlikely(fault_probability_before_commit > 0.0))
            {
                std::bernoulli_distribution fault(fault_probability_before_commit);
                if (fault(thread_local_rng))
                    throw Coordination::Exception::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Fault injected (before commit)");
            }

            /// Commit point
            csn_path_created = current_zookeeper->create(zookeeper_path_log + "/csn-", serializeTID(txn->tid), zkutil::CreateMode::PersistentSequential);

            if (unlikely(fault_probability_after_commit > 0.0))
            {
                std::bernoulli_distribution fault(fault_probability_after_commit);
                if (fault(thread_local_rng))
                    throw Coordination::Exception::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Fault injected (after commit)");
            }
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
                throw;

            /// We don't know if transaction has been actually committed or not.
            /// The only thing we can do is to postpone its finalization.
            {
                std::lock_guard lock{running_list_mutex};
                unknown_state_list.emplace_back(txn, std::move(state_guard));
            }
            log_updated_event->set();
            if (throw_on_unknown_status)
                throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION,
                                "Connection lost on attempt to commit transaction {}, will finalize it later: {}",
                                txn->tid, e.message());

            LOG_INFO(log, "Connection lost on attempt to commit transaction {}, will finalize it later: {}", txn->tid, e.message());
            return Tx::CommittingCSN;
        }

        /// Do not allow exceptions between commit point and the and of transaction finalization
        /// (otherwise it may stuck in COMMITTING state holding snapshot).
        NOEXCEPT_SCOPE_STRICT({
            /// FIXME Transactions: Sequential node numbers in ZooKeeper are Int32, but 31 bit is not enough for production use
            /// (overflow is possible in a several weeks/months of active usage)
            allocated_csn = deserializeCSN(csn_path_created.substr(zookeeper_path_log.size() + 1));
        });
    }

    return finalizeCommittedTransaction(txn.get(), allocated_csn, state_guard);
}

CSN TransactionLog::finalizeCommittedTransaction(MergeTreeTransaction * txn, CSN allocated_csn, scope_guard & state_guard) noexcept
{
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    chassert(!allocated_csn == txn->isReadOnly());
    if (allocated_csn)
    {
        LOG_INFO(log, "Transaction {} committed with CSN={}", txn->tid, allocated_csn);
        tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::COMMIT, txn->tid, allocated_csn);
    }
    else
    {
        /// Transaction was readonly
        allocated_csn = txn->snapshot;
        tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::COMMIT, txn->tid, allocated_csn);
    }

    /// Write allocated CSN, so we will be able to cleanup log in ZK. This method is noexcept.
    txn->afterCommit(allocated_csn);
    state_guard = {};

    {
        /// Finally we can remove transaction from the list and release the snapshot
        std::lock_guard lock{running_list_mutex};
        snapshots_in_use.erase(txn->snapshot_in_use_it);
        bool removed = running_list.erase(txn->tid.getHash());
        if (!removed)
        {
            LOG_ERROR(log, "It's a bug: TID {} {} doesn't exist", txn->tid.getHash(), txn->tid);
            abort();
        }
    }

    txn->afterFinalize();
    return allocated_csn;
}

bool TransactionLog::waitForCSNLoaded(CSN csn) const
{
    auto current_latest_snapshot = latest_snapshot.load();
    while (current_latest_snapshot < csn && !stop_flag)
    {
        latest_snapshot.wait(current_latest_snapshot);
        current_latest_snapshot = latest_snapshot.load();
    }
    return csn <= current_latest_snapshot;
}

void TransactionLog::rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept
{
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    LOG_TRACE(log, "Rolling back transaction {}{}", txn->tid,
              std::uncaught_exceptions() ? fmt::format(" due to uncaught exception (code: {})", getCurrentExceptionCode()) : "");

    if (!txn->rollback())
    {
        /// Transaction was cancelled or committed concurrently
        chassert(txn->csn != Tx::UnknownCSN);
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
    txn->afterFinalize();
}

MergeTreeTransactionPtr TransactionLog::tryGetRunningTransaction(const TIDHash & tid)
{
    std::lock_guard lock{running_list_mutex};
    auto it = running_list.find(tid);
    if (it == running_list.end())
        return NO_TRANSACTION_PTR;
    return it->second;
}

CSN TransactionLog::getCSN(const TransactionID & tid, const std::atomic<CSN> * failback_with_strict_load_csn)
{
    /// Avoid creation of the instance if transactions are not actually involved
    if (tid == Tx::PrehistoricTID)
        return Tx::PrehistoricCSN;
    return instance().getCSNImpl(tid.getHash(), failback_with_strict_load_csn);
}

CSN TransactionLog::getCSN(const TIDHash & tid, const std::atomic<CSN> * failback_with_strict_load_csn)
{
    /// Avoid creation of the instance if transactions are not actually involved
    if (tid == Tx::PrehistoricTID.getHash())
        return Tx::PrehistoricCSN;
    return instance().getCSNImpl(tid, failback_with_strict_load_csn);
}

CSN TransactionLog::getCSNImpl(const TIDHash & tid_hash, const std::atomic<CSN> * failback_with_strict_load_csn) const
{
    chassert(tid_hash);
    chassert(tid_hash != Tx::EmptyTID.getHash());

    {
        std::lock_guard lock{mutex};
        auto it = tid_to_csn.find(tid_hash);
        if (it != tid_to_csn.end())
            return it->second.csn;
    }

    /// Usually commit csn checked by load memory with memory_order_relaxed option just for performance improvements
    /// If fast loading fails than getCSN is called.
    /// There is a race possible, transaction could be committed concurrently. Right before getCSN has been called. In that case tid_to_csn has no tid_hash but commit csn is set.
    /// In order to be sure, commit csn has to be loaded with memory_order_seq_cst after lookup at tid_to_csn
    if (failback_with_strict_load_csn)
        if (CSN maybe_csn = failback_with_strict_load_csn->load())
            return maybe_csn;

    return Tx::UnknownCSN;
}

CSN TransactionLog::getCSNAndAssert(const TransactionID & tid, std::atomic<CSN> & failback_with_strict_load_csn)
{
    /// failback_with_strict_load_csn is not provided to getCSN
    /// Because it would be checked after assertTIDIsNotOutdated
    if (CSN maybe_csn = getCSN(tid))
        return maybe_csn;

    assertTIDIsNotOutdated(tid, &failback_with_strict_load_csn);

   /// If transaction is not outdated then it might be already committed
   /// We should load CSN again to distinguish it
   /// Otherwise the transactiuon hasn't been committed yet
    if (CSN maybe_csn = failback_with_strict_load_csn.load())
        return maybe_csn;

    return Tx::UnknownCSN;
}

void TransactionLog::assertTIDIsNotOutdated(const TransactionID & tid, const std::atomic<CSN> * failback_with_strict_load_csn)
{
    if (tid == Tx::PrehistoricTID)
        return;

    /// Ensure that we are not trying to get CSN for TID that was already removed from the log
    CSN tail = instance().tail_ptr.load();
    if (tail <= tid.start_csn)
        return;

    /// At this point of execution tail is lesser that tid.start_csn
    /// This mean that transaction is either outdated or just has been committed concurrently and the tail moved forward.
    /// If the second case takes place transaction's commit csn has to be set.
    /// We should load CSN again to distinguish the second case.
    if (failback_with_strict_load_csn)
        if (CSN maybe_csn = failback_with_strict_load_csn->load())
            return;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get CSN for too old TID {}, current tail_ptr is {}, probably it's a bug", tid, tail);
}

CSN TransactionLog::getOldestSnapshot() const
{
    std::lock_guard lock{running_list_mutex};
    if (snapshots_in_use.empty())
        return getLatestSnapshot();
    chassert(running_list.size() == snapshots_in_use.size());
    chassert(snapshots_in_use.size() < 2 || snapshots_in_use.front() <= *++snapshots_in_use.begin());
    return snapshots_in_use.front();
}

TransactionLog::TransactionsList TransactionLog::getTransactionsList() const
{
    std::lock_guard lock{running_list_mutex};
    return running_list;
}


void TransactionLog::sync() const
{
    Strings entries_list = getZooKeeper()->getChildren(zookeeper_path_log);
    chassert(!entries_list.empty());
    ::sort(entries_list.begin(), entries_list.end());
    CSN newest_csn = deserializeCSN(entries_list.back());
    waitForCSNLoaded(newest_csn);
}

}
