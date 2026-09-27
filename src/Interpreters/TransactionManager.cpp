#include "config.h"

#include <Common/SipHash.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <numeric>
#include <unordered_set>
#include <fmt/ranges.h>
#include <Core/ServerUUID.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/TransactionManager.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <base/defines.h>
#include <base/sort.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/logger_useful.h>
#include <Common/noexcept_scope.h>
#include <Common/threadPoolCallbackRunner.h>

#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{

std::atomic<Int64> TransactionManager::async_tables_loading_job_number{0};
std::atomic<bool> TransactionManager::transactions_allowed{false};

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
    extern const int SERIALIZATION_ERROR;
}

namespace FailPoints
{
    extern const char transaction_force_unknown_state_after_commit[];
    extern const char smt_force_txn_rollback_invalidation[];
}

static void tryWriteEventToSystemLog(LoggerPtr log, ContextPtr context,
                                     TransactionsInfoLogElement::Type type, const TransactionID & tid, CSN csn = Tx::UnknownCSN)
try
{
    auto system_log = context->getTransactionsInfoLog();
    if (!system_log)
        return;

    system_log->add([&](TransactionsInfoLogElement & element)
    {
        element.type = type;
        element.tid = tid;
        element.csn = csn;
        element.fillCommonFields(nullptr);
    });
}
catch (...)
{
    tryLogCurrentException(log);
}

TransactionManager::TransactionManager()
    : global_context(Context::getGlobalContextInstance())
    , log(getLogger("TransactionManager"))
    , zookeeper_path(global_context->getConfigRef().getString("transaction_log.zookeeper_path", "/clickhouse/txn"))
    , zookeeper_path_replicas(zookeeper_path + "/replicas")
    , zookeeper_path_invalid_tids(zookeeper_path + "/invalid_tids")
    , my_replica_id(ServerUUID::get())
    , session(
          zookeeper_path_replicas,
          zookeeper_path_invalid_tids,
          my_replica_id,
          global_context->getConfigRef().getInt64("transaction_log.dead_replica_threshold_ms", 30'000),
          log)
    , txn_log(zookeeper_path, session, stop_flag, log)
    , fault_probability_before_commit(global_context->getConfigRef().getDouble("transaction_log.fault_probability_before_commit", 0))
    , fault_probability_after_commit(global_context->getConfigRef().getDouble("transaction_log.fault_probability_after_commit", 0))
{
    auto component_guard = Coordination::setCurrentComponent("TransactionManager::TransactionManager");

    /// Fail-close before any list-with-data, multi-read or check-stat call runs. Without
    /// `CHECK_STAT` Keeper answers `Unsupported operation: CheckStat` and drops the session, which
    /// surfaces as a lost commit rather than a clear error.
    auto zk = global_context->getZooKeeper();
    if (!zk->isFeatureEnabled(KeeperFeatureFlag::LIST_WITH_STAT_AND_DATA)
        || !zk->isFeatureEnabled(KeeperFeatureFlag::FILTERED_LIST)
        || !zk->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ)
        || !zk->isFeatureEnabled(KeeperFeatureFlag::CHECK_STAT))
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Transactions require Keeper to advertise `LIST_WITH_STAT_AND_DATA`, "
            "`FILTERED_LIST`, `MULTI_READ` and `CHECK_STAT` feature flags. Upgrade the Keeper cluster.");
    }

    loadLogFromZooKeeper();

    updating_thread = std::make_unique<ThreadFromGlobalPool>(&TransactionManager::runUpdatingThread, this);
}

TransactionManager::~TransactionManager()
{
    shutdown();
}

void TransactionManager::shutdown()
{
    if (stop_flag.exchange(true))
        return;
    txn_log.notifyUpdated();
    if (updating_thread)
        updating_thread->join();

    std::lock_guard lock{mutex};
    /// Destroy ephemeral node holders before resetting zookeeper. Two invariants must hold:
    ///  1. The holders must be reset before zookeeper.reset() — they hold a ZooKeeper& reference
    ///     and their destructors call ZooKeeper::expired(), which would be a use-after-free if
    ///     zookeeper were freed first. Member destruction order in ~TransactionManager() cannot save
    ///     us because shutdown() calls zookeeper.reset() explicitly here.
    ///  2. A Coordination component must be set on this thread before tryRemove is called inside
    ///     ~EphemeralNodeHolder, because ZooKeeper::pushRequest asserts it is non-empty.
    {
        auto component_guard = Coordination::setCurrentComponent("TransactionManager::shutdown");
        cleanup_lock_holder.reset();
        session.releaseActiveNode();
    }
    /// Reset zookeeper last to avoid a race inside Poco::Logger (Coordination::ZooKeeper::log).
    zookeeper.reset();
}

ZooKeeperPtr TransactionManager::getZooKeeper() const
{
    std::lock_guard lock{mutex};
    return zookeeper;
}

String TransactionManager::cleanupLockPath() const { return zookeeper_path + "/cleanup_lock"; }

void TransactionManager::tryAcquireCleanupLock()
{
    /// Single Multi with plain Create — the Multi's transaction zxid (echoed
    /// on each sub-response) equals the created znode's czxid, which we use
    /// to detect a peer's later re-create. Avoids the Create2 feature flag.
    auto create_request = zkutil::makeCreateRequest(cleanupLockPath(), toString(my_replica_id), zkutil::CreateMode::Ephemeral);

    Coordination::Requests requests{create_request};
    Coordination::Responses responses;
    /// Only a user error reaches here: `tryMulti` throws on hardware errors. A lost reply needs no
    /// recovery either — the session is finalized, so Keeper reaps the lease it may have created.
    const auto err = TSA_READ_ONE_THREAD(zookeeper)->tryMulti(requests, responses);
    if (err == Coordination::Error::ZNODEEXISTS)
    {
        LOG_DEBUG(log, "Another replica holds the cleanup lease — skipping cleanup on this replica");
        return;
    }
    if (err != Coordination::Error::ZOK)
    {
        LOG_WARNING(log, "Could not acquire the cleanup lease ({}) — skipping cleanup on this replica", err);
        return;
    }

    cleanup_lock_czxid = responses[0]->zxid;
    cleanup_lock_holder = zkutil::EphemeralNodeHolder::existing(cleanupLockPath(), *TSA_READ_ONE_THREAD(zookeeper));
    LOG_INFO(log, "Acquired cleanup lease — this replica will run dead-replica detection and log cleanup");
}

void TransactionManager::initReplicaNodes()
{
    txn_log.initTableNodes(zookeeper);
    session.initInvalidTidsNode(zookeeper);

    /// Its disappearance is what signals session loss to peers.
    session.createActiveNode(zookeeper);

    /// Initialize our own tail_ptr in Keeper if absent.
    zookeeper->tryCreate(session.replicaTailPtrPath(), Tx::serializeCSN(Tx::MaxReservedCSN), zkutil::CreateMode::Persistent);

    /// Try to become the designated cleaner. Exactly one replica holds the cleanup_lock
    /// ephemeral node at any time; others skip markDeadReplicas/removeOldEntries.
    tryAcquireCleanupLock();
}

void TransactionManager::initOwnReplicaState()
{
    txn_log.restoreOwnTailPtr(zookeeper);

    /// Publish the session first so a cleanup-owner peer includes us in
    /// `computeGlobalMinTailPtr` and won't prune past our floor.
    session.initSessionNode(zookeeper);
    initReplicaNodes();

    /// A peer's `markDeadReplicas` guards only on `_active`, so it could still mark us between
    /// the two calls above. Now that `_active` exists it cannot, so adopt whatever version is there.
    if (auto new_version = session.updateSessionVersionIfChanged(zookeeper, TransactionSession::SessionCheck::AtStartup))
        session.setVersion(*new_version);
}

void TransactionManager::publishLoadedSnapshot(std::optional<CSN> new_snapshot)
{
    if (!new_snapshot)
        return;
    std::lock_guard lock{running_list_mutex};
    txn_log.publishSnapshot(*new_snapshot);
    local_tid_counter = Tx::MaxReservedLocalTID;
}

void TransactionManager::loadLogFromZooKeeper()
{
    chassert(!zookeeper);
    zookeeper = global_context->getZooKeeper();

    txn_log.initLogRoot(zookeeper);

    initOwnReplicaState();

    publishLoadedSnapshot(txn_log.reloadCSNLogs(zookeeper));
    txn_log.assertLoaded();
}

std::optional<ZooKeeperNodeVersion> TransactionManager::handleReconnection()
{
    auto new_zookeeper = global_context->getZooKeeper();
    std::lock_guard lock{mutex};

    /// Both holders reference the handle, so release them before replacing it.
    session.releaseActiveNode();
    cleanup_lock_holder.reset();

    zookeeper = new_zookeeper;

    /// No `/log` fence: `reloadCSNLogs` is the only reader that can move the snapshot backwards,
    /// and it fences itself.
    return session.renewSession(zookeeper);
}

void TransactionManager::updateOwnTailPtr()
{
    if (!global_context->isServerCompletelyStarted())
        return;

    if (!updated_tail_ptr.load(std::memory_order_relaxed) && asyncTablesLoadingJobNumber() != 0)
    {
        LOG_TRACE(log, "There are running async tables loading jobs, skip updating tail_ptr");
        return;
    }
    updated_tail_ptr.store(true, std::memory_order_relaxed);

    /// Clamp by the oldest unfinalized-transaction start_csn so
    /// `assertTIDIsNotOutdated` in `tryFinalizeUnknownStateTransactions` cannot
    /// fire on entries that are still waiting to be processed.
    CSN oldest_unfinalized_start_csn = std::numeric_limits<CSN>::max();
    {
        std::lock_guard rlock{running_list_mutex};
        for (const auto & [txn, _] : unknown_state_list)
            oldest_unfinalized_start_csn = std::min(oldest_unfinalized_start_csn, txn->tid.start_csn);
        for (const auto & [txn, _] : unknown_state_list_loaded)
            oldest_unfinalized_start_csn = std::min(oldest_unfinalized_start_csn, txn->tid.start_csn);
    }

    txn_log.advanceOwnTailPtr(TSA_READ_ONE_THREAD(zookeeper), getOldestSnapshot(), oldest_unfinalized_start_csn);
}

void TransactionManager::advanceSessionVersionAndRollbackStaleTxns(ZooKeeperNodeVersion new_session_v)
{
    std::vector<MergeTreeTransactionPtr> txns_to_rollback;
    {
        std::lock_guard lock{running_list_mutex};
        session.setVersion(new_session_v);

        /// An unknown-state txn may already be committed (CSN entry created, response lost),
        /// so don't roll it back by session version here. Leave it to
        /// tryFinalizeUnknownStateTransactions, which decides from the loaded CSN.
        std::unordered_set<TIDHash> in_unknown_state;
        for (const auto & [txn, _] : unknown_state_list)
            in_unknown_state.insert(txn->tid.getHash());
        for (const auto & [txn, _] : unknown_state_list_loaded)
            in_unknown_state.insert(txn->tid.getHash());

        for (const auto & [hash, txn] : running_list)
            if (ZooKeeperNodeVersion{txn->tid.session_node_version} < new_session_v && !in_unknown_state.contains(hash))
                txns_to_rollback.push_back(txn);
    }
    for (auto & txn : txns_to_rollback)
        rollbackTransaction(txn);
}

void TransactionManager::runUpdatingThread()
{
    auto component_guard = Coordination::setCurrentComponent("TransactionManager::runUpdatingThread");
    while (true)
    {
        try
        {
            /// Do not wait if we have some transactions to finalize.
            /// On an idle cluster (no commits ever poke `log_updated_event`) the periodic
            /// tasks below — `updateOwnTailPtr`, dead-replica detection — would otherwise
            /// sit forever. Use a bounded `tryWait` so the loop wakes about once per second.
            if (TSA_READ_ONE_THREAD(unknown_state_list_loaded).empty())
                txn_log.waitForUpdate(1000);

            if (stop_flag.load())
                return;

            bool connection_loss = getZooKeeper()->expired();
            if (connection_loss)
            {
                auto new_session_ver = handleReconnection();
                if (new_session_ver.has_value())
                {
                    advanceSessionVersionAndRollbackStaleTxns(*new_session_ver);
                    /// Log cleanup may have advanced past our `_tail_ptr` while dead.
                    publishLoadedSnapshot(txn_log.reloadCSNLogs(getZooKeeper()));
                }
            }

            publishLoadedSnapshot(txn_log.loadNewEntries(getZooKeeper()));
            session.loadReplicaMap(getZooKeeper());
            session.loadInvalidTids(getZooKeeper());
            tryFinalizeUnknownStateTransactions();
            updateOwnTailPtr();

            /// Only the replica holding the cleanup_lock ephemeral runs dead-replica detection
            /// and log cleanup. If the lease holder dies its session expires, the ephemeral
            /// disappears, and any other replica can claim it on the next iteration.
            bool is_cleanup_owner{false};
            {
                std::lock_guard lock{mutex};
                if (!cleanup_lock_holder)
                    tryAcquireCleanupLock();
                is_cleanup_owner = cleanup_lock_holder != nullptr;
            }

            /// Runs on every replica — durably persist locally-queued TID invalidations.
            session.storePendingInvalidTids(getZooKeeper());

            if (is_cleanup_owner)
            {
                session.markDeadReplicas(getZooKeeper());
                {
                    std::lock_guard lock{mutex};
                    txn_log.removeOldEntries(zookeeper, cleanupLockPath(), cleanup_lock_czxid);
                }
                session.evictInvalidTids(getZooKeeper());
            }

            /// Same reason as the orphan prune above, for `tid_to_csn`.
            txn_log.pruneInMemoryEntriesRemovedFromLog(TSA_READ_ONE_THREAD(zookeeper));
        }
        catch (const Coordination::Exception &)
        {
            tryLogCurrentException(log);
            /// TODO better backoff
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            txn_log.notifyUpdated();
        }
        catch (...)
        {
            tryLogCurrentException(log);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            txn_log.notifyUpdated();
        }
    }
}

void TransactionManager::tryFinalizeUnknownStateTransactions()
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
        try
        {
            /// CSNs are already loaded here, so a map lookup is enough. Use `lookupCSNInMap`, not
            /// `getCSN`: this runs on `runUpdatingThread`, and `getCSN`'s Keeper fallback on a miss
            /// would be a wasted round-trip from the thread that just loaded the entries.
            CSN csn = (txn->tid.isNonTransactional()
                           ? Tx::NonTransactionalCSN
                           : txn_log.lookupCSNInMap(txn->tid.getHash()));
            if (csn != Tx::UnknownCSN)
            {
                /// Snapshot before finalizing: `finalizeCommittedTransaction` clears the
                /// txn's contents, so a later `getAffectedSMTTables` would return nothing.
                finalizeCommittedTransaction(txn.get(), csn, state_guard);
            }
            else
            {
                assertTIDIsNotOutdated(txn->tid);
                state_guard = {};
                rollbackTransaction(txn->shared_from_this());
            }
        }
        catch (...)
        {
            /// One bad entry must not lose the tail. Log and requeue so other entries in
            /// this batch still get processed; the next reconnect retries this one.
            tryLogCurrentException(log, fmt::format("Failed to finalize unknown-state txn {}, will retry", txn->tid));
            std::lock_guard lock{running_list_mutex};
            unknown_state_list_loaded.emplace_back(std::move(txn), std::move(state_guard));
        }
    }
}

void TransactionManager::updateTableStampFromTID(Int64 cross_replica_id, const TransactionID & stamp_tid)
{
    /// `EmptyTID` means the discovery fetched ZNONODE — the table has
    /// never had a transactional commit, so there's no stamp to record.
    if (stamp_tid == Tx::EmptyTID)
        return;

    CSN stamp_csn = Tx::UnknownCSN;
    if (stamp_tid == Tx::NonTransactionalTID)
    {
        /// The sentinel value `commitTransaction` writes via the
        /// `CreateIfNotExists(sentinel)` stamp op on first commit. Maps to
        /// `NonTransactionalCSN = 1`, the lowest starting point for the
        /// watermark walk.
        stamp_csn = Tx::NonTransactionalCSN;
    }
    else
    {
        stamp_csn = getCSN(stamp_tid);
        if (stamp_csn == Tx::UnknownCSN)
        {
            /// The CSN log consumer on this replica has not yet seen the
            /// log entry for `stamp_tid`. Skip the update — the next
            /// discovery cycle will retry, and by then `getCSN` should
            /// resolve.
            return;
        }
    }
    updateTableStamp(cross_replica_id, stamp_csn);
}

MergeTreeTransactionPtr TransactionManager::beginTransaction()
{
    MergeTreeTransactionPtr txn;
    {
        std::lock_guard lock{running_list_mutex};
        CSN snapshot = txn_log.getLatestSnapshot();
        LocalTID ltid = 1 + local_tid_counter.fetch_add(1);
        auto snapshot_lock = snapshots_in_use.insert(snapshots_in_use.end(), snapshot);
        txn = std::make_shared<MergeTreeTransaction>(
            snapshot, ltid, ServerUUID::get(), session.getVersion().toInt64(), snapshot_lock);
        bool inserted = running_list.try_emplace(txn->tid.getHash(), txn).second;
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: TID {} {} exists", txn->tid.getHash(), txn->tid);
    }

    LOG_TEST(log, "Beginning transaction {} ({})", txn->tid, txn->tid.getHash());
    tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::BEGIN, txn->tid);

    return txn;
}

CSN TransactionManager::commitTransaction(const MergeTreeTransactionPtr & txn, bool throw_on_unknown_status)
{
    auto component_guard = Coordination::setCurrentComponent("TransactionManager::commitTransaction");
    /// Some precommit checks, may throw. Sets the transaction to COMMITTING under `commit_gate`,
    /// which makes `isRunning` reject new background operation commits.
    auto state_guard = txn->beforeCommit();

    CSN allocated_csn = Tx::UnknownCSN;
    auto requests = txn->getRequestsOnCommit();
    std::vector<MergeTreeTransaction::AffectedSMTTable> affected_smt_tables;
    if (txn->isReadOnly())
    {
        chassert(requests.empty());
        /// Don't need to allocate CSN in ZK for readonly transactions, it's safe to use snapshot/start_csn as "commit" timestamp
        LOG_TEST(log, "Closing readonly transaction {}", txn->tid);
    }
    else
    {
        LOG_TEST(log, "Committing transaction {}", txn->dumpDescription());
        /// TODO support batching
        auto current_zookeeper = getZooKeeper();
        String csn_path_created;
        affected_smt_tables = txn->getAffectedSMTTables();
        try
        {
            Coordination::SimpleFaultInjection fault(fault_probability_before_commit, fault_probability_after_commit, "commit");

            /// Guard against ghost commits: if a peer declared this replica dead and bumped _session,
            /// this check will fail atomically, preventing the stale session from committing.
            requests.push_back(zkutil::makeCheckRequest(
                session.replicaSessionPath(),
                static_cast<int32_t>(txn->tid.session_node_version)));

            /// CSN log entry — MUST be the last request: `res.back()` below is read
            /// as its CreateResponse to recover the allocated CSN.
            requests.push_back(zkutil::makeCreateRequest(
                txn_log.logPath() + "/csn-",
                Tx::CSNEntryData{.tid = txn->tid, .replica_id = my_replica_id, .smt = affected_smt_tables}.serialize(),
                zkutil::CreateMode::PersistentSequential));

            /// Commit point
            auto res = current_zookeeper->multi(requests, /* check_session_valid */ true);

            csn_path_created = dynamic_cast<const Coordination::CreateResponse *>(res.back().get())->path_created;

            fiu_do_on(FailPoints::transaction_force_unknown_state_after_commit,
            {
                /// CSN znode is already created in ZK; simulate the response being lost.
                /// The catch block below will postpone finalization to runUpdatingThread,
                /// reproducing the fault_probability_after_commit code path deterministically.
                throw Coordination::Exception::fromMessage(Coordination::Error::ZOPERATIONTIMEOUT,
                    "Fault injected: forced unknown state after commit");
            });
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
            {
                /// A commit request lost a conflict (lock taken over, marker collision, part gone).
                /// Report a clear transaction error instead of the raw Keeper code, then roll back.
                if (e.code == Coordination::Error::ZNODEEXISTS
                    || e.code == Coordination::Error::ZBADVERSION
                    || e.code == Coordination::Error::ZNONODE)
                {
                    String failed_path;
                    if (const auto * multi_ex = dynamic_cast<const zkutil::KeeperMultiException *>(&e))
                        failed_path = multi_ex->getPathForFirstFailedOp();
                    LOG_INFO(log, "Transaction {} cannot commit: commit request on path '{}' failed with {}",
                        txn->tid, failed_path, e.code);
                    throw Exception(ErrorCodes::SERIALIZATION_ERROR,
                        "Transaction {} cannot commit because a concurrent operation changed a part it modified "
                        "(path '{}', {})", txn->tid, failed_path, e.code);
                }
                throw;
            }

            /// We don't know if transaction has been actually committed or not.
            /// The only thing we can do is to postpone its finalization.
            {
                std::lock_guard lock{running_list_mutex};
                unknown_state_list.emplace_back(txn, std::move(state_guard));
            }
            txn_log.notifyUpdated();
            if (throw_on_unknown_status)
                throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION,
                                "Connection lost on attempt to commit transaction {}, will finalize it later: {}",
                                txn->tid, e.message());

            LOG_INFO(log, "Connection lost on attempt to commit transaction {}, will finalize it later: {}", txn->tid, e.message());
            return Tx::CommittingCSN;
        }

        /// Do not allow exceptions between the commit point and the end of transaction finalization
        /// (otherwise it may get stuck in COMMITTING state holding snapshot).
        NOEXCEPT_SCOPE_STRICT({
            allocated_csn = Tx::deserializeCSN(csn_path_created.substr(txn_log.logPath().size() + 1));
        });
    }

    return finalizeCommittedTransaction(txn.get(), allocated_csn, state_guard);
}

CSN TransactionManager::finalizeCommittedTransaction(MergeTreeTransaction * txn, CSN allocated_csn, scope_guard & state_guard) noexcept
{
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    auto blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();
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

    /// Write allocated CSN so we can later clean up the log in ZK.
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

void TransactionManager::rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept
{
    auto component_guard = Coordination::setCurrentComponent("TransactionManager::rollbackTransaction");
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    LOG_TRACE(log, "Rolling back transaction {}{}", txn->tid,
              std::uncaught_exceptions() ? fmt::format(" due to uncaught exception (code: {})", getCurrentExceptionCode()) : "");

    const auto rollback_result = txn->rollback();
    if (rollback_result == MergeTreeTransaction::RollbackResult::NotNeeded)
    {
        /// Transaction was cancelled or committed concurrently
        chassert(txn->csn != Tx::UnknownCSN);
        return;
    }
    /// If `rollback()` failed to restore a previously-removed part, it
    /// aborted the server inside the catch — control does not reach here.
    /// See the comment in `MergeTreeTransaction::rollback` for why.

    /// Invalidate this TID once for the whole rollback: on a `rollback()` failure or a
    /// requests-on-rollback failure, `invalidateTID` (below) records the TID durably so every
    /// replica resolves it to `RolledBackCSN` and reclaims its orphan locks/parts — without a
    /// replica-wide `_session` bump that would also roll back unrelated concurrent transactions.
    bool needs_tid_invalidation = (rollback_result == MergeTreeTransaction::RollbackResult::Failed);

    /// Test hook: simulate a failed-cleanup rollback so tests can drive the TID-invalidation path
    /// deterministically without a real Keeper fault.
    fiu_do_on(FailPoints::smt_force_txn_rollback_invalidation, { needs_tid_invalidation = true; });

    {
        auto requests = txn->getRequestsOnRollback();
        if (!requests.empty())
        {
            /// This function is `noexcept`; a Keeper error must not escape and terminate the
            /// server. On any failure, invalidate the TID so peers reclaim orphan
            /// `<part>/removal_lock` znodes via `isTIDInvalid`.
            try
            {
                Coordination::Responses responses;
                auto code = getZooKeeper()->tryMulti(requests, responses);
                if (code == Coordination::Error::ZOK)
                {
                    LOG_INFO(log, "Processed requests on rollback {}", requests.size());
                }
                else
                {
                    /// Only user errors reach here; hardware errors throw and are caught below.
                    zkutil::KeeperMultiException exception(code, requests, responses);
                    LOG_WARNING(
                        log,
                        "Failed to process requests on rollback {} because of {} on path {}",
                        requests.size(),
                        Coordination::toString(code),
                        exception.getPathForFirstFailedOp());
                    needs_tid_invalidation = true;
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to process requests on rollback");
                needs_tid_invalidation = true;
            }
        }
    }

    if (needs_tid_invalidation)
        invalidateTID(txn->tid, Tx::MainJobId, "rollback cleanup failed");

    {
        std::lock_guard lock{running_list_mutex};
        bool removed = running_list.erase(txn->tid.getHash());
        if (!removed)
        {
            LOG_FATAL(log, "Transaction {} not found in running_list during rollback; aborting", txn->tid);
            abort();
        }
        snapshots_in_use.erase(txn->snapshot_in_use_it);
    }

    tryWriteEventToSystemLog(log, global_context, TransactionsInfoLogElement::ROLLBACK, txn->tid);
    txn->afterFinalize();
}

MergeTreeTransactionPtr TransactionManager::tryGetRunningTransaction(const TIDHash & tid)
{
    std::lock_guard lock{running_list_mutex};
    auto it = running_list.find(tid);
    if (it == running_list.end())
        return NO_TRANSACTION_PTR;
    return it->second;
}

std::vector<MergeTreeTransactionPtr> TransactionManager::getLocalRunningTransactions() const
{
    std::vector<MergeTreeTransactionPtr> result;
    std::lock_guard lock{running_list_mutex};
    result.reserve(running_list.size());
    for (const auto & [hash, txn] : running_list)
        if (txn->tid.host_id == my_replica_id && txn->getState() == MergeTreeTransaction::RUNNING)
            result.push_back(txn);
    return result;
}

TransactionID TransactionManager::getMyNonTransactionalTID() const
{
    /// Each call mints a unique non-transactional TID. Callers allocate one per operation and
    /// reuse it across lock-acquire, removal stamp, lock-release, and marker (do NOT call this
    /// twice within one operation and expect the same value).
    return TransactionID{
        Tx::NonTransactionalCSN,
        non_transactional_local_tid_counter.fetch_add(1),
        my_replica_id,
        session.getVersion().toInt64(),
    };
}

TransactionID TransactionManager::getMyNonTransactionalTID(bool transactions_enabled)
{
    /// Mirrors the pattern used in `getCSN`: avoid creating the singleton when the
    /// caller's table has transactions disabled. The sentinel carries no host or
    /// session info — peers cannot reclaim a lock stamped with it after our death,
    /// but tx-disabled tables don't take session-tracked locks in the first place.
    if (transactions_enabled)
        return instance().getMyNonTransactionalTID();
    return Tx::NonTransactionalTID;
}

CSN TransactionManager::getCSN(const TransactionID & tid)
{
    /// Avoid creation of the instance if transactions are not actually involved.
    /// `isNonTransactional()` covers both the sentinel `Tx::NonTransactionalTID` and a
    /// host-identified non-tx TID (`getMyNonTransactionalTID`).
    if (tid.isNonTransactional())
        return Tx::NonTransactionalCSN;
    return instance().getCSNImpl(tid);
}

CSN TransactionManager::getCSNImpl(const TransactionID & tid)
{
    const TIDHash tid_hash = tid.getHash();

    /// Fast path: in-memory map already has the CSN (entries at or below `latest_snapshot`).
    if (CSN csn = txn_log.lookupCSNInMap(tid_hash); csn != Tx::UnknownCSN)
        return csn;

    /// Already resolved by an earlier gap read. A CSN never changes, so this is safe to reuse.
    if (CSN csn = txn_log.lookupGapCSN(tid_hash); csn != Tx::UnknownCSN)
        return csn;

    /// Invalidated TID — report it as rolled back. Checked after the committed lookups above,
    /// because a TID is never both committed and invalidated.
    if (session.isInvalidated(tid_hash))
        return Tx::RolledBackCSN;

    /// Miss in both. Read the TID from the Keeper `/log` tail instead of calling `sync()`:
    /// `sync()` waits for `runUpdatingThread`, which may itself be waiting for a data-parts
    /// lock the caller holds — the deadlock we are avoiding. A returned UnknownCSN is
    /// authoritative (the TID is not in the log up to the tip we read).
    if (CSN csn = txn_log.resolveGapCSNFromKeeper([this] { return getZooKeeper(); }, tid_hash); csn != Tx::UnknownCSN)
        return csn;

    /// While we read the gap, `runUpdatingThread` may have absorbed the entry into `tid_to_csn`
    /// and advanced `latest_snapshot` past it, so the gap no longer covers it. Re-check the map
    /// before concluding "unknown": any CSN at or below `latest_snapshot` is resolvable
    /// via `lookupCSNInMap` (`tid_to_csn`).
    if (CSN csn = txn_log.lookupCSNInMap(tid_hash); csn != Tx::UnknownCSN)
        return csn;

    /// Host-self rule. For a TID we minted on our current session, look it up directly.
    /// Note the gap read above can be stale: a concurrent commit may have created the
    /// `csn-*` znode and finalized the txn right after it ran.
    if (tid.host_id == my_replica_id && ZooKeeperNodeVersion{tid.session_node_version} == session.getVersion())
    {
        {
            std::lock_guard lock{running_list_mutex};
            auto it = running_list.find(tid_hash);
            if (it != running_list.end())
            {
                /// A committing txn holds `CommittingCSN` until its real CSN is durable. Don't
                /// leak that sentinel: only a real CSN or `RolledBackCSN` (both above
                /// `MaxReservedCSN`) is resolved; anything else means "not resolved yet".
                CSN csn = it->second->csn.load();
                return csn > Tx::MaxReservedCSN ? csn : Tx::UnknownCSN;
            }
        }

        /// Gone from `running_list`: committed-and-finalized or rolled back. The commit writes the
        /// `csn-*` znode before erasing the txn under `running_list_mutex`, so if our miss is real
        /// the znode is durable now. Re-read Keeper (outside the mutex) to tell the two apart.
        if (CSN csn = txn_log.resolveGapCSNFromKeeper([this] { return getZooKeeper(); }, tid_hash); csn != Tx::UnknownCSN)
            return csn;
        if (CSN csn = txn_log.lookupCSNInMap(tid_hash); csn != Tx::UnknownCSN)
            return csn;
        return Tx::RolledBackCSN;
    }

    return Tx::UnknownCSN;
}

void TransactionManager::assertTIDIsNotOutdated(const TransactionID & tid)
{
    /// `isNonTransactional()` covers both the sentinel `Tx::NonTransactionalTID` and a
    /// host-identified non-tx TID (`getMyNonTransactionalTID`); neither is logged so neither
    /// can be outdated.
    if (tid.isNonTransactional())
        return;

    /// Ensure that we are not trying to get CSN for TID that was already removed from the log.
    /// Use global_tail_ptr (min across all live replicas) as the safe truncation boundary.
    CSN tail = instance().txn_log.getGlobalTailPtr();
    if (tail == Tx::UnknownCSN || tail <= tid.start_csn)
        return;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get CSN for too old TID {}, current tail_ptr is {}, probably it's a bug", tid, tail);
}

CSN TransactionManager::getOldestSnapshot() const
{
    std::lock_guard lock{running_list_mutex};
    if (snapshots_in_use.empty())
        return getLatestSnapshot();
    chassert(running_list.size() == snapshots_in_use.size());
    /// Full ascending-order check. `beginTransaction` appends at `end()`, but this
    /// only stays sorted because `bounded_snapshot` is monotonically non-decreasing
    /// across calls (see the comment at the insert site for the proof). Catching a
    /// violation here pins the regression to the next out-of-order append instead
    /// of letting `front()` silently return a non-minimum element.
    chassert(std::is_sorted(snapshots_in_use.begin(), snapshots_in_use.end()));
    return snapshots_in_use.front();
}

TransactionManager::TransactionsList TransactionManager::getTransactionsList() const
{
    std::lock_guard lock{running_list_mutex};
    return running_list;
}

void TransactionManager::increaseAsyncTablesLoadingJobNumber()
{
    async_tables_loading_job_number.fetch_add(1);
}
void TransactionManager::decreaseAsyncTablesLoadingJobNumber()
{
    async_tables_loading_job_number.fetch_sub(1);
}
Int64 TransactionManager::asyncTablesLoadingJobNumber()
{
    return async_tables_loading_job_number.load();
}

void TransactionManager::allowTransactions()
{
    transactions_allowed.store(true);
}
bool TransactionManager::areTransactionsAllowed()
{
    return transactions_allowed.load();
}
}
