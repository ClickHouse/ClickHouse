#pragma once
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <base/scope_guard.h>
#include <boost/noncopyable.hpp>
#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/IKeeper.h>

#include <list>
#include <unordered_set>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

/// Holds the removal locks of a batch of data parts that are being removed without a transaction.
///
/// A non-transactional removal cannot be undone: writing `removal_tid = Tx::NonTransactionalTID`
/// also sets `removal_csn = Tx::NonTransactionalCSN`, which hides the part for good, and no
/// rollback path clears it. Removing a batch part by part therefore means that a conflict on the
/// third part leaves the first two durably invisible even though the statement failed.
///
/// This class splits the removal in two phases: `lock` takes the removal lock of every part of the
/// batch and reports conflicts before anything is written, `store` then stamps them all. Locks that
/// are never stored are released by the destructor, so a failed statement leaves the parts intact
/// and can be retried.
class NonTransactionalRemovalLocks : private boost::noncopyable
{
public:
    NonTransactionalRemovalLocks() = default;
    ~NonTransactionalRemovalLocks();

    /// Locks every part of `parts_to_remove` for removal. `covering_part` is only used for error
    /// messages and system log events, and may be null when the parts are removed without being
    /// covered by a new one.
    /// Throws `SERIALIZATION_ERROR` if another transaction is already removing one of the parts,
    /// or if the transaction that created one of them has not committed yet.
    void lock(const StoragePtr & storage, const DataPartPtr & covering_part, const DataPartsVector & parts_to_remove);

    /// Writes `removal_tid` to every locked part and releases the locks.
    void store();

private:
    struct LockedPart
    {
        DataPartPtr part;
        TransactionInfoContext context;
    };

    std::vector<LockedPart> locked_parts;
};

/// This object is responsible for tracking all changes that some transaction is making in MergeTree tables.
/// It collects all changes that queries of current transaction made in data part sets of all MergeTree tables
/// to either make them visible when transaction commits or undo when transaction rolls back.
class MergeTreeTransaction : public std::enable_shared_from_this<MergeTreeTransaction>, private boost::noncopyable
{
    friend class TransactionLog;
public:
    enum State
    {
        RUNNING,
        COMMITTING,
        COMMITTED,
        ROLLED_BACK,
    };

    CSN getSnapshot() const { return snapshot.load(std::memory_order_relaxed); }
    void setSnapshot(CSN new_snapshot);
    State getState() const;

    const TransactionID tid;

    MergeTreeTransaction(CSN snapshot_, LocalTID local_tid_, UUID host_id, std::list<CSN>::iterator snapshot_it_);

    void addNewPart(const StoragePtr & storage, const DataPartPtr & new_part);
    void removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, const TransactionInfoContext & context);

    void addMutation(const StoragePtr & table, const String & mutation_id);

    static void addNewPart(const StoragePtr & storage, const DataPartPtr & new_part, MergeTreeTransaction * txn);

    /// When `txn` is null the part is not removed right away: it is only locked into
    /// `removal_locks`, and the caller stamps the whole batch with `NonTransactionalRemovalLocks::store`.
    static void removeOldPart(
        const StoragePtr & storage, const DataPartPtr & part_to_remove, MergeTreeTransaction * txn,
        NonTransactionalRemovalLocks & removal_locks);

    /// Same deferral as `removeOldPart` above: with a null `txn` the covered parts are only locked
    /// into `removal_locks`.
    static void addNewPartAndRemoveCovered(
        const StoragePtr & storage, const DataPartPtr & new_part, const DataPartsVector & covered_parts,
        MergeTreeTransaction * txn, NonTransactionalRemovalLocks & removal_locks);

    bool isReadOnly() const;

    void onException();

    String dumpDescription() const;

    Float64 elapsedSeconds() const { return elapsed.elapsedSeconds(); }

    /// Waits for transaction state to become not equal to the state corresponding to current_state_csn
    bool waitStateChange(CSN current_state_csn) const;

    CSN getCSN() const { return csn; }

    void addRequestsOnCommit(const Coordination::Requests & requests);
    void addRequestOnCommit(Coordination::RequestPtr request);
    Coordination::Requests getRequestsOnCommit() const;

    void addRequestsOnRollback(const Coordination::Requests & requests);
    void addRequestOnRollback(Coordination::RequestPtr request);
    Coordination::Requests getRequestsOnRollback() const;

private:
    scope_guard beforeCommit();
    void afterCommit(CSN assigned_csn) noexcept;
    bool rollback() noexcept;
    void afterFinalize();

    void checkIsNotCancelled() const;

    mutable std::mutex mutex;
    Stopwatch elapsed;

    /// Usually it's equal to tid.start_csn, but can be changed by SET SNAPSHOT query (for introspection purposes and time-traveling)
    std::atomic<CSN> snapshot;
    const std::list<CSN>::iterator snapshot_in_use_it;

    bool finalized TSA_GUARDED_BY(mutex) = false;

    /// Indicates if transaction was read-only before `afterFinalize`
    bool is_read_only TSA_GUARDED_BY(mutex) = false;

    /// Lists of changes made by transaction
    std::unordered_set<StoragePtr> storages TSA_GUARDED_BY(mutex);
    DataPartsVector creating_parts TSA_GUARDED_BY(mutex);
    DataPartsVector removing_parts TSA_GUARDED_BY(mutex);
    using RunningMutationsList = std::vector<std::pair<StoragePtr, String>>;
    RunningMutationsList mutations TSA_GUARDED_BY(mutex);

    Coordination::Requests requests_on_commit TSA_GUARDED_BY(mutex);
    Coordination::Requests requests_on_rollback TSA_GUARDED_BY(mutex);

    std::atomic<CSN> csn;
};

using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

}
