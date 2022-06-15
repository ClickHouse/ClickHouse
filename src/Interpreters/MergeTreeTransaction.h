#pragma once
#include <Interpreters/TransactionVersionMetadata.h>
#include <boost/noncopyable.hpp>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/Stopwatch.h>
#include <base/scope_guard.h>

#include <list>
#include <unordered_set>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

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

    CSN getSnapshot() const { return snapshot; }
    void setSnapshot(CSN new_snapshot);
    State getState() const;

    const TransactionID tid;

    MergeTreeTransaction(CSN snapshot_, LocalTID local_tid_, UUID host_id);

    void addNewPart(const StoragePtr & storage, const DataPartPtr & new_part);
    void removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, const TransactionInfoContext & context);

    void addMutation(const StoragePtr & table, const String & mutation_id);

    static void addNewPart(const StoragePtr & storage, const DataPartPtr & new_part, MergeTreeTransaction * txn);
    static void removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, MergeTreeTransaction * txn);
    static void addNewPartAndRemoveCovered(const StoragePtr & storage, const DataPartPtr & new_part, const DataPartsVector & covered_parts, MergeTreeTransaction * txn);

    bool isReadOnly() const;

    void onException();

    String dumpDescription() const;

    Float64 elapsedSeconds() const { return elapsed.elapsedSeconds(); }

    /// Waits for transaction state to become not equal to the state corresponding to current_state_csn
    bool waitStateChange(CSN current_state_csn) const;

    CSN getCSN() const { return csn; }

private:
    scope_guard beforeCommit();
    void afterCommit(CSN assigned_csn) noexcept;
    bool rollback() noexcept;
    void checkIsNotCancelled() const;

    mutable std::mutex mutex;
    Stopwatch elapsed;

    /// Usually it's equal to tid.start_csn, but can be changed by SET SNAPSHOT query (for introspection purposes and time-traveling)
    CSN snapshot;
    std::list<CSN>::iterator snapshot_in_use_it;

    /// Lists of changes made by transaction
    std::unordered_set<StoragePtr> storages;
    std::vector<TableLockHolder> table_read_locks_for_ordinary_db;
    DataPartsVector creating_parts;
    DataPartsVector removing_parts;
    using RunningMutationsList = std::vector<std::pair<StoragePtr, String>>;
    RunningMutationsList mutations;

    std::atomic<CSN> csn;
};

using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

}
