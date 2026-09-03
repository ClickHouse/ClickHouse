#include "config.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#if CLICKHOUSE_CLOUD
#include <Interpreters/MergeTreeTransaction/VersionMetadataOnKeeper.h>
#endif
#include <Interpreters/TransactionManager.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#if CLICKHOUSE_CLOUD
#include <Storages/StorageSharedMergeTree.h>
#endif
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadPool.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/noexcept_scope.h>

#include <base/sleep.h>
#include <fmt/ranges.h>
#include <Core/UUID.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int INVALID_TRANSACTION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_DATA_PART;
    extern const int TRANSACTION_ROLLBACK_PARTIAL_FAILURE;
}

namespace FailPoints
{
    extern const char transaction_after_commit_pause[];
    extern const char transaction_rollback_reset_removal_tid_fail[];
}

static void checkNotOrdinaryDatabase(const StoragePtr & storage)
{
    if (storage->getStorageID().uuid != UUIDHelpers::Nil)
        return;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table {} belongs to database with Ordinary engine. "
                    "This engine is deprecated and is not supported in transactions.", storage->getStorageID().getNameForLogs());
}

MergeTreeTransaction::MergeTreeTransaction(
    CSN snapshot_, LocalTID local_tid_, UUID host_id, Int64 session_version_, std::list<CSN>::iterator snapshot_it_)
    : tid({snapshot_, local_tid_, host_id, session_version_})
    , snapshot(snapshot_)
    , snapshot_in_use_it(snapshot_it_)
    , csn(Tx::UnknownCSN)
{
}

void MergeTreeTransaction::setSnapshot(CSN new_snapshot)
{
    snapshot.store(new_snapshot, std::memory_order_relaxed);
}

MergeTreeTransaction::State MergeTreeTransaction::getState() const
{
    CSN c = csn.load();
    if (c == Tx::UnknownCSN)
        return RUNNING;
    if (c == Tx::CommittingCSN)
        return COMMITTING;
    if (c == Tx::RolledBackCSN)
        return ROLLED_BACK;
    return COMMITTED;
}

/// Blocks until `csn` leaves `current_state_csn`. Every writer that changes `csn`
/// must call `csn.notify_all`, otherwise `csn.wait` here is not guaranteed to wake.
bool MergeTreeTransaction::waitStateChange(CSN current_state_csn) const
{
    CSN current_value = current_state_csn;
    while (current_value == current_state_csn && !TransactionManager::instance().isShuttingDown())
    {
        csn.wait(current_value);
        current_value = csn.load();
    }
    return current_value != current_state_csn;
}

void MergeTreeTransaction::checkIsNotCancelled() const
{
    CSN c = csn.load();
    if (c == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");
    if (c != Tx::UnknownCSN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", c);
}

void MergeTreeTransaction::addNewPart(const StoragePtr & storage, const DataPartPtr & new_part, MergeTreeTransaction * txn)
{
    /// Creation TID was written to data part earlier on part creation.
    /// We only need to ensure that it's written and add part to in-memory set of new parts.
    new_part->assertHasVersionMetadata(txn);
    if (txn)
    {
        txn->addNewPart(storage, new_part);
        /// Now we know actual part name and can write it to system log table.
        tryWriteEventToSystemLog(
            new_part->version->getLogger(),
            TransactionsInfoLogElement::ADD_PART,
            txn->tid,
            TransactionInfoContext{storage->getStorageID(), new_part->name});
    }
}

void MergeTreeTransaction::removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, MergeTreeTransaction * txn, LockKind kind)
{
    TransactionInfoContext transaction_context{storage->getStorageID(), part_to_remove->name};
    if (txn)
    {
        /// Lock part for removal and write current TID into version metadata file.
        /// If server crash just after committing transactions
        /// we will find this TID in version metadata and will finally remove part.
        txn->removeOldPart(storage, part_to_remove, transaction_context, kind);
        return;
    }
    part_to_remove->version->setAndStoreNonTransactionalRemovalTID(kind, transaction_context);
}

void MergeTreeTransaction::addNewPartAndRemoveCovered(
    const StoragePtr & storage, const DataPartPtr & new_part, const DataPartsVector & covered_parts,
    MergeTreeTransaction * txn, LockKind kind)
{
    TransactionID tid = txn ? txn->tid : Tx::NonTransactionalTID;
    TransactionInfoContext transaction_context{storage->getStorageID(), new_part->name};

    /// When a part that was originally created by a real transaction is loaded onto a replica
    /// (e.g. via addCurrentPart), its AddPart event was already logged at creation time.
    /// Logging it again with NonTransactionalTID would produce a spurious duplicate entry.
    const bool already_logged_by_transaction = !txn && new_part->version
        && !new_part->version->getInfo().creation_tid.isEmpty()
        && !new_part->version->getInfo().creation_tid.isNonTransactional();

    if (!already_logged_by_transaction)
        tryWriteEventToSystemLog(new_part->version->getLogger(), TransactionsInfoLogElement::ADD_PART, tid, transaction_context);
    transaction_context.covering_part = std::move(transaction_context.part_name);
    new_part->assertHasVersionMetadata(txn);

    if (txn)
    {
        txn->addNewPart(storage, new_part);
        for (const auto & covered : covered_parts)
        {
            transaction_context.part_name = covered->name;
            /// This path serves plain MergeTree's `commit()` (no held lock): `removeOldPart` acquires
            /// a fresh lock and stamps. The SMT merge instead calls `removeOldPart` directly with the
            /// intention's held fingerprint (record-only), bypassing this function.
            txn->removeOldPart(storage, covered, transaction_context, kind);
        }
    }
    else
    {
        for (const auto & covered : covered_parts)
        {
            transaction_context.part_name = covered->name;
            covered->version->setAndStoreNonTransactionalRemovalTID(kind, transaction_context);
        }
    }
}

void MergeTreeTransaction::addNewPart(const StoragePtr & storage, const DataPartPtr & new_part)
{
    checkNotOrdinaryDatabase(storage);
    std::lock_guard lock{mutex};
    checkIsNotCancelled();
    storages.insert(storage);
    creating_parts.push_back(new_part);
}

void MergeTreeTransaction::removeOldPart(
    const StoragePtr & storage, const DataPartPtr & part_to_remove, const TransactionInfoContext & context,
    LockKind kind, const LockFingerprint & held_lock_fingerprint)
{
    checkNotOrdinaryDatabase(storage);

    {
        std::lock_guard lock{mutex};
        checkIsNotCancelled();

        /// A source that already holds its removal lock, with `removal_tid` stamped under it by the
        /// caller's Multi: record the part against the held fingerprint and stop — no fresh lock, no
        /// re-stamp, no seal (the caller seals in its own commit Multi). Commit finalizes the removal;
        /// rollback clears `removal_tid` and releases the lock.
        ///
        /// The fingerprint alone selects this path. Only Keeper-backed metadata can produce one, and
        /// every caller of this protocol passes it — a merge / mutation / optimize source, or a
        /// `DROP PARTITION` committed part from `dropPartitionInTx`. A single `DROP PART` passes none
        /// and takes the plain path below.
        if (held_lock_fingerprint.hasFingerprint())
        {
            chassert(kind == LockKind::BG_MERGE || kind == LockKind::MUTATION || kind == LockKind::OPTIMIZE
                    || kind == LockKind::OPTIMIZE_FINAL || kind == LockKind::DROP,
                "Only a merge / mutation / optimize / DROP PARTITION source arrives holding its removal lock");
            NOEXCEPT_SCOPE({
                storages.insert(storage);
                removing_parts.push_back({storage, part_to_remove, kind, held_lock_fingerprint});
            });
            return;
        }

        /// Plain removal (e.g. a part dropped within the transaction): acquire a fresh lock and stamp.
        /// Capture the acquire-time fingerprint: the commit Multi uses it to mark the lock committed and
        /// rollback to release it. It also lets `unlockRemovalTID` detect that a peer took the lock over
        /// (throws `ABORTED`, which is caught and ignored) instead of `LOGICAL_ERROR`.
        /// `false` means the part already vanished (a peer's merge / TRUNCATE / DROP removed it): nothing
        /// to register, skip so commit proceeds and the background executor doesn't retry forever. A real
        /// conflict throws `SERIALIZATION_ERROR`, which propagates to the caller.
        LockFingerprint acquired;
        if (!part_to_remove->version->lockRemovalTID(tid, kind, context, &acquired))
        {
            LOG_DEBUG(getLogger("MergeTreeTransaction"),
                "Part {} vanished, skipping transactional removal", part_to_remove->name);
            return;
        }
        NOEXCEPT_SCOPE({
            storages.insert(storage);
            removing_parts.push_back({storage, part_to_remove, kind, acquired});
        });

        /// Stamp `removal_tid` under the freshly acquired lock so a peer that reclaimed the lock cannot overwrite our stamp.
        part_to_remove->version->setAndStoreRemovalTID(tid, acquired);

#if CLICKHOUSE_CLOUD
        /// Mark the fresh lock committed in the commit Multi so the removal is recorded on the lock
        /// and no peer can take it over.
        if (auto * vm = dynamic_cast<VersionMetadataOnKeeper *>(part_to_remove->version.get()))
        {
            auto committed_ops = vm->makeMarkRemovalLockCommittedRequests(tid, kind, acquired);
            requests_on_commit.insert(requests_on_commit.end(), committed_ops.begin(), committed_ops.end());
        }
#endif
    }
}

void MergeTreeTransaction::addMutation(const StoragePtr & table, const String & mutation_id)
{
    checkNotOrdinaryDatabase(table);
    std::lock_guard lock{mutex};
    checkIsNotCancelled();
    storages.insert(table);
    mutations.emplace_back(table, mutation_id);
}

void MergeTreeTransaction::addLockedPart(
    const StoragePtr & storage, const DataPartPtr & part, LockKind kind, LockFingerprint acquired)
{
    checkNotOrdinaryDatabase(storage);
    std::lock_guard lock{mutex};
    if (csn.load() == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction {} was cancelled", tid);
    storages.insert(storage);
    locked_parts.push_back({storage, part, kind, acquired});
}

bool MergeTreeTransaction::isReadOnly() const
{
    std::lock_guard lock{mutex};
    if (finalized)
        return is_read_only;
    /// `addLockedPart` also inserts into `storages`, so a transaction that only
    /// acquired source-part locks (e.g. a merge that failed at commit) has a
    /// non-empty `storages` with no created/removed part or mutation. Add
    /// `locked_parts.empty()` so this check still holds in that case.
    chassert((creating_parts.empty() && removing_parts.empty() && mutations.empty() && locked_parts.empty()) == storages.empty());
    return storages.empty();
}

void MergeTreeTransaction::addRequestsOnCommit(const Coordination::Requests & requests)
{
    std::lock_guard lock{mutex};
    requests_on_commit.insert(requests_on_commit.end(), requests.begin(), requests.end());
}

void MergeTreeTransaction::addRequestOnCommit(Coordination::RequestPtr request)
{
    std::lock_guard lock{mutex};
    requests_on_commit.push_back(std::move(request));
}

Coordination::Requests MergeTreeTransaction::getRequestsOnCommit() const
{
    std::lock_guard lock{mutex};
    return requests_on_commit;
}

void MergeTreeTransaction::addRequestsOnRollback(const Coordination::Requests & requests)
{
    std::lock_guard lock{mutex};
    requests_on_rollback.insert(requests_on_rollback.end(), requests.begin(), requests.end());
}

void MergeTreeTransaction::addRequestOnRollback(Coordination::RequestPtr request)
{
    std::lock_guard lock{mutex};
    requests_on_rollback.push_back(std::move(request));
}

Coordination::Requests MergeTreeTransaction::getRequestsOnRollback() const
{
    std::lock_guard lock{mutex};
    return requests_on_rollback;
}

std::vector<MergeTreeTransaction::AffectedSMTTable> MergeTreeTransaction::getAffectedSMTTables() const
{
    std::lock_guard lock{mutex};
    std::vector<AffectedSMTTable> result;
#if CLICKHOUSE_CLOUD
    /// Only `SharedMergeTree` (cloud-only) is stamped; public builds leave this empty.
    /// Replicas of one table share a `cross_replica_id`, so collapse them into one row:
    /// the fan-out is per table and `processCSNLogs` rejects the same id twice.
    std::unordered_map<Int64, size_t> row_index_by_cross_replica_id;
    for (const auto & storage : storages)
    {
        const auto * smt = dynamic_cast<const StorageSharedMergeTree *>(storage.get());
        if (!smt)
            continue;
        const UUID storage_uuid = storage->getStorageID().uuid;
        const Int64 cross_replica_id = smt->getCrossReplicaId();

        auto [it, inserted] = row_index_by_cross_replica_id.try_emplace(cross_replica_id, result.size());
        if (inserted)
            result.push_back(AffectedSMTTable{cross_replica_id, smt->getZooKeeperPath(), {}, {}});
        AffectedSMTTable & row = result[it->second];

        for (const auto & part : creating_parts)
            if (part->storage.getStorageID().uuid == storage_uuid)
                row.added_part_names.push_back(part->name);
        for (const auto & lp : removing_parts)
            if (lp.storage.get() == storage.get())
                row.removed_part_names.push_back(lp.part->name);
    }
#endif
    return result;
}

scope_guard MergeTreeTransaction::beforeCommit()
{
    RunningMutationsList mutations_to_wait;
    {
        std::lock_guard lock{mutex};
        mutations_to_wait = mutations;
    }

    /// We should wait for mutations to finish before committing transaction, because some mutation may fail and cause rollback.
    for (const auto & table_and_mutation : mutations_to_wait)
        table_and_mutation.first->waitForMutation(table_and_mutation.second, /* wait_for_another_mutation */ false);

    chassert([&]()
    {
        std::lock_guard lock{mutex};
        return mutations == mutations_to_wait;
    }());

    /// Flip to COMMITTING under `commit_gate` so a background merge of this transaction's parts
    /// never sees the state change mid-commit. See `isRunning`.
    {
        std::unique_lock commit_gate_lock{commit_gate};
        CSN expected = Tx::UnknownCSN;
        bool can_commit = csn.compare_exchange_strong(expected, Tx::CommittingCSN);
        if (!can_commit)
        {
            /// Transaction was concurrently cancelled by KILL TRANSACTION or KILL MUTATION
            if (expected == Tx::RolledBackCSN)
                throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", expected);
        }
    }

    csn.notify_all(); /// Wake `waitStateChange`.

    /// We should set CSN back to Unknown if we will fail to commit transaction for some reason (connection loss, etc)
    return [this]()
    {
        CSN expected_value = Tx::CommittingCSN;
        if (csn.compare_exchange_strong(expected_value, Tx::UnknownCSN))
            csn.notify_all();
    };
}

/// `noexcept` is intentional. The commit CSN is already durable by now; the work below
/// only finalizes per-part state (`VersionMetadata` on disk or in Keeper, mutation CSNs,
/// table stamps). An exception escaping any of these means in-memory and durable state
/// diverged — `std::terminate` lets the next restart re-derive consistency from the log.
/// Swallowing would leave partial finalization that nothing reconciles.
void MergeTreeTransaction::afterCommit(CSN assigned_csn) noexcept
{
    auto blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);

    DataPartsVector created_parts;
    std::vector<LockedPart> removed_parts;
    RunningMutationsList committed_mutations;
    std::unordered_set<StoragePtr> affected_storages;
    {
        /// We don't really need mutex here, because no concurrent modifications of transaction object may happen after commit.
        std::lock_guard lock{mutex};
        created_parts = creating_parts;
        removed_parts = removing_parts;
        committed_mutations = mutations;
        affected_storages = storages;
    }

    /// Persist per-part version metadata BEFORE flipping `csn` below.
    /// `csn.exchange(assigned_csn)` is the signal that `MergeTreeTransaction::waitStateChange`
    /// blocks on; doing the disk-backed `setAndStore...CSN` calls first ensures that once a
    /// waiter wakes up, the new `creation_csn` / `removal_csn` are already visible through
    /// `VersionMetadata::getInfo`, and therefore through `system.parts`.
    ///
    /// Use `assigned_csn` directly because `this->csn` is still `Tx::CommittingCSN` here.
    ///
    /// Crash-safe: if the process terminates inside this loop, the CSN znode in ZK plus
    /// `removal_tid` / `creation_tid` on disk are enough to recover any part whose
    /// `setAndStore...CSN` did not complete; `TransactionManager::getCSN(tid)` returns the right
    /// answer after restart.
    for (const auto & part : created_parts)
    {
        part->version->setAndStoreCreationCSN(assigned_csn);
    }

    for (const auto & removed : removed_parts)
    {
        /// The removal lock was already committed in the commit Multi (kept parts) or removed by
        /// the merge commit (merge sources), so there is no lock to release here — just stamp
        /// the committed CSN onto the part.
        removed.part->version->setAndStoreRemovalCSN(assigned_csn);
    }

    for (const auto & storage_and_mutation : committed_mutations)
        storage_and_mutation.first->setMutationCSN(storage_and_mutation.second, assigned_csn);

#if CLICKHOUSE_CLOUD
    StorageSharedMergeTree::bumpVirtualPartsForStorages(
        affected_storages,
        "MergeTreeTransaction::afterCommit::bumpVirtualParts",
        "Failed to bump virtual_parts znode after tx commit; promotion will run on the next watched event");
#endif

    /// Test-only pause point. With this failpoint enabled, a regression test can verify that
    /// `waitStateChange` does not return until every part has its new CSN persisted (above).
    /// Not wrapped in try/catch: `pauseFailPoint` only takes a mutex and a condvar, and the
    /// surrounding `setAndStore...CSN` calls already trust their callees not to throw under
    /// the same `noexcept` contract.
    FailPointInjection::pauseFailPoint(FailPoints::transaction_after_commit_pause);

    /// Flip the atomic last so that `waitStateChange` only wakes up after all metadata is durable.
    [[maybe_unused]] CSN prev_value = csn.exchange(assigned_csn);
    chassert(prev_value == Tx::CommittingCSN);
    /// `std::atomic::wait` requires a matching `notify`; a bare store does not wake a waiter
    /// (works on the Linux libc++ global-table fallback by luck, but hangs on the native wait used for 8-byte atomics on macOS).
    csn.notify_all();
}

MergeTreeTransaction::RollbackResult MergeTreeTransaction::rollback() noexcept
{
    auto blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    /// Exclusive like `beforeCommit`: a background merge holds the gate across both its commit `multi`
    /// and the adoption that registers its parts here, so rollback cannot land between the two.
    bool need_rollback = false;
    {
        std::unique_lock commit_gate_lock{commit_gate};
        CSN expected = Tx::UnknownCSN;
        need_rollback = csn.compare_exchange_strong(expected, Tx::RolledBackCSN);
    }

    /// Check that it was not rolled back concurrently
    if (!need_rollback)
        return RollbackResult::NotNeeded;

    /// Wake any `waitStateChange` waiter (see the `notify` note in `afterCommit`).
    csn.notify_all();

    /// It's not a problem if server crash at this point
    /// because on startup we will see that TID is not committed and will simply discard these changes.

    RunningMutationsList mutations_to_kill;
    DataPartsVector parts_to_remove;
    /// Parts to restore on rollback, with the `<part>/removal_lock` fingerprint that
    /// `lockRemovalTID` captured in `removeOldPart`. The fingerprint is consumed by
    /// the `unlockRemovalTID` loop below to detect a peer's `tryTakeOver` (which would
    /// otherwise make `unlock` throw `LOGICAL_ERROR`).
    std::vector<LockedPart> parts_to_activate;
    std::vector<LockedPart> locks_to_release;

    {
        std::lock_guard lock{mutex};
        mutations_to_kill = mutations;
        parts_to_remove = creating_parts;
        parts_to_activate = removing_parts;
        locks_to_release = locked_parts;
    }

    /// Forcefully stop related mutations if any
    for (const auto & table_and_mutation : mutations_to_kill)
    {
        try
        {
            table_and_mutation.first->killMutation(table_and_mutation.second);
        }
        catch (...)
        {
            /// Safe: the mutation task will detect that the transaction is rolled back (RolledBackCSN)
            /// and will stop on its own. Failing to send the kill signal is not fatal.
            tryLogCurrentException(getLogger("MergeTreeTransaction"), fmt::format(
                "Failed to kill mutation {} during rollback, ignoring", table_and_mutation.second));
        }
    }

    /// Discard changes in active parts set
    /// Remove parts that were created, restore parts that were removed (except parts that were created by this transaction too)

    /// Any best-effort Keeper failure below sets `any_failed`; the caller invalidates the TID once
    /// so peers' `isTIDInvalid` can detect the rolled-back part without waiting for restart.
    bool any_failed = false;
    for (const auto & part : parts_to_remove)
    {
        try
        {
            part->version->setAndStoreCreationCSN(Tx::RolledBackCSN);
        }
        catch (...)
        {
            tryLogCurrentException(part->version->getLogger(),
                fmt::format("Failed to persist RolledBackCSN for part {}", part->name));
            any_failed = true;
        }
    }

    for (const auto & part : parts_to_remove)
    {
        /// Skip parts in `Temporary` state — they are not in `data_parts_indexes`,
        /// so `removePartsFromWorkingSet` would raise `LOGICAL_ERROR` and (under
        /// `abort_on_logical_error`) abort the server before the catch can swallow it.
        ///
        /// How a `Temporary` part reaches a rolling-back transaction: `SharedMergeTreeSink::commitPart`
        /// hits a hardware error on Keeper commit, calls `rollbackPartsToTemporaryState` (which
        /// erases the part and marks it `Temporary`), then on retry the memo-ID check determines
        /// the original commit actually succeeded and re-registers the part via `txn->addNewPart`
        /// without restoring it to the working set. The Keeper-side `RolledBackCSN` above is the
        /// durable rollback marker; the uncommitted part's znode and S3 blobs are reaped by `processPartsUpdate`.
        if (part->getState() == MergeTreeDataPartState::Temporary)
            continue;

        /// NOTE It's possible that part is already removed from working set in the same transaction
        /// (or, even worse, in a separate non-transactional query with NonTransactionalTID),
        /// but it's not a problem: removePartsFromWorkingSet(...) will do nothing in this case.
        try
        {
            const_cast<MergeTreeData &>(part->storage).removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part}, true);
        }
        catch (...)
        {
            /// Safe: if the part was already removed by another path, the working set is already correct.
            tryLogCurrentException(part->version->getLogger(),
                fmt::format("Failed to remove part {} from working set during rollback, ignoring", part->name));
        }
    }

    /// Undo each removal, then restore only what the undo saw: `resetRemovalTID` reads Keeper, the
    /// cached `VersionInfo` can be stale. A peer can still commit in between; the next parts update
    /// demotes the part again.
    for (const auto & entry : parts_to_activate)
    {
        const auto & part = entry.part;
        /// Clear removal_tid so we needn't tell never-committed TIDs apart from long-committed ones.
        /// The fingerprint gates the clear on still owning the lock.
        bool removal_tid_cleared = true;
        /// Our clear landed, so the removal was still ours to undo.
        bool removal_undone = false;
        /// A peer removed the part for good, so there is nothing left to restore.
        bool part_gone = false;
        try
        {
            fiu_do_on(FailPoints::transaction_rollback_reset_removal_tid_fail,
            {
                throw Exception(ErrorCodes::ABORTED, "Injected failure of resetRemovalTID during rollback");
            });
            removal_undone = part->version->resetRemovalTID(entry.acquired);
        }
        catch (const Exception & e)
        {
            part_gone = e.code() == ErrorCodes::NO_SUCH_DATA_PART;
            tryLogCurrentException(part->version->getLogger(),
                fmt::format("Failed to clear removal_tid for part {} during rollback", part->name));
            any_failed = true;
            removal_tid_cleared = false;
        }
        catch (...)
        {
            tryLogCurrentException(part->version->getLogger(),
                fmt::format("Failed to clear removal_tid for part {} during rollback", part->name));
            any_failed = true;
            removal_tid_cleared = false;
        }

        /// Restore only a removal still ours: either our clear landed, or it threw and left both our
        /// stamp and our lock in place. A clear that quietly did nothing means a peer owns the removal
        /// now — its `removal_tid` looks the same as a cleared one, so only the outcome tells them apart.
        const auto & info = part->version->getInfo();
        if (!part_gone && info.creation_tid != tid && info.removal_csn == Tx::UnknownCSN
            && (removal_undone || !removal_tid_cleared))
        {
            try
            {
                const_cast<MergeTreeData &>(part->storage).restoreAndActivatePart(part);
            }
            catch (...)
            {
                /// Abort rather than continue: a half-restored partition can be made durable by a
                /// later transaction, and a restart brings the parts back cleanly. Constructing the
                /// exception without throwing names the cause in `system.errors`.
                Exception ex(ErrorCodes::TRANSACTION_ROLLBACK_PARTIAL_FAILURE,
                    "Rollback of transaction {} failed to restore part {} to "
                    "Active; aborting to avoid serving the half-restored state",
                    tid, part->name);

                tryLogCurrentException(part->version->getLogger(), ex.message());

                std::abort();
            }
        }

        /// Keep the lock guarding the stamp we failed to clear. See `rollback` in the header.
        if (!removal_tid_cleared)
            continue;

        try
        {
            /// Pass the acquire-time fingerprint so a peer's `tryTakeOver` between our
            /// `lockRemovalTID` (in `removeOldPart`) and this `unlockRemovalTID` surfaces
            /// as `ABORTED` (caught and ignored below) instead of `LOGICAL_ERROR`,
            /// which would trip `abortOnFailedAssertion` in sanitizer builds.
            part->version->unlockRemovalTID(
                tid,
                TransactionInfoContext{part->storage.getStorageID(), part->name},
                entry.acquired);
        }
        catch (...)
        {
            tryLogCurrentException(part->version->getLogger(),
                fmt::format("Failed to unlock removal_tid for part {} during rollback", part->name));
            any_failed = true;
        }
    }

    /// Release the source-part `<part>/removal_lock` znodes held by this transaction's merges,
    /// mutations and optimizes (`BG_MERGE` / `MUTATION` / `OPTIMIZE` / `OPTIMIZE_FINAL`). The
    /// locks of parts it removes were already cleared by the `unlockRemovalTID` loop above.
    for (const auto & locked : locks_to_release)
    {
        try
        {
#if CLICKHOUSE_CLOUD
            /// SMT-only: these locks live on `VersionMetadataOnKeeper`,
            /// which is not built into public ClickHouse.
            if (auto * vm = dynamic_cast<VersionMetadataOnKeeper *>(locked.part->version.get()))
                vm->unlockPartLock(
                    tid,
                    TransactionInfoContext{locked.storage->getStorageID(), locked.part->name},
                    locked.acquired);
#endif
        }
        catch (...)
        {
            tryLogCurrentException(locked.part->version->getLogger(),
                fmt::format("Failed to release removal_lock for source part {} during rollback", locked.part->name));
            any_failed = true;
        }
    }

    chassert([&]()
    {
        std::lock_guard lock{mutex};
        chassert(mutations_to_kill == mutations);
        chassert(parts_to_remove == creating_parts);
        chassert(parts_to_activate == removing_parts);
        chassert(locks_to_release.size() == locked_parts.size());
        return csn == Tx::RolledBackCSN;
    }());

#if CLICKHOUSE_CLOUD
    std::unordered_set<StoragePtr> rollback_storages;
    {
        std::lock_guard lock{mutex};
        rollback_storages = storages;
    }
    StorageSharedMergeTree::bumpVirtualPartsForStorages(
        rollback_storages,
        "MergeTreeTransaction::rollback::bumpVirtualParts",
        "Failed to bump virtual_parts znode after tx rollback; cleanup will run on the next watched event");
#endif

    return any_failed ? RollbackResult::Failed : RollbackResult::Ok;
}

void MergeTreeTransaction::afterFinalize()
{
    std::lock_guard lock{mutex};
    /// `locked_parts.empty()` is part of this invariant; see `isReadOnly` for why.
    chassert((creating_parts.empty() && removing_parts.empty() && mutations.empty() && locked_parts.empty()) == storages.empty());

    /// Remember if it was read-only transaction before we clear storages
    is_read_only = storages.empty();

    /// Release shared pointers just in case
    creating_parts.clear();
    removing_parts.clear();
    storages.clear();
    mutations.clear();
    /// Lock release already happened by now; clear so the held `StoragePtr` /
    /// `DataPartPtr` don't outlive the transaction, like the containers above.
    locked_parts.clear();
    finalized = true;
}

void MergeTreeTransaction::onException()
{
    TransactionManager::instance().rollbackTransaction(shared_from_this());
}

String MergeTreeTransaction::dumpDescription() const
{
    String res = fmt::format("{} state: {}, snapshot: {}", tid, getState(), getSnapshot());

    if (isReadOnly())
    {
        res += ", readonly";
        return res;
    }

    std::lock_guard lock{mutex};
    if (finalized)
    {
        res += ", cannot dump detailed description, transaction is finalized";
        return res;
    }

    res += fmt::format(", affects {} tables:", storages.size());

    using ChangesInTable = std::tuple<Strings, Strings, Strings>;
    std::unordered_map<const IStorage *, ChangesInTable> storage_to_changes;

    for (const auto & part : creating_parts)
        std::get<0>(storage_to_changes[&(part->storage)]).push_back(part->name);

    for (const auto & entry : removing_parts)
    {
        const auto & part = entry.part;
        auto current_version_info = part->version->getInfo();
        String info = fmt::format("{} ({})", part->name, current_version_info.toString(/*one_line=*/true));
        std::get<1>(storage_to_changes[&(part->storage)]).push_back(std::move(info));
        chassert(!current_version_info.creation_csn || current_version_info.creation_csn <= getSnapshot());
    }

    for (const auto & mutation : mutations)
        std::get<2>(storage_to_changes[mutation.first.get()]).push_back(mutation.second);

    for (const auto & storage_changes : storage_to_changes)
    {
        res += fmt::format("\n\t{}:", storage_changes.first->getStorageID().getNameForLogs());
        const auto & creating_info = std::get<0>(storage_changes.second);
        const auto & removing_info = std::get<1>(storage_changes.second);
        const auto & mutations_info = std::get<2>(storage_changes.second);

        if (!creating_info.empty())
            res += fmt::format("\n\t\tcreating parts:\n\t\t\t{}", fmt::join(creating_info, "\n\t\t\t"));
        if (!removing_info.empty())
            res += fmt::format("\n\t\tremoving parts:\n\t\t\t{}", fmt::join(removing_info, "\n\t\t\t"));
        if (!mutations_info.empty())
            res += fmt::format("\n\t\tmutations:\n\t\t\t{}", fmt::join(mutations_info, "\n\t\t\t"));
    }

    return res;
}

}
