#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>

#include <atomic>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
class MergeTreeData;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

/// Proof that the caller holds a partition's write guard. `UniqueKeyTxnManager::commitTransaction`
/// is the only thing that can construct one, and it hands the same one to `stage` and `publish`.
class PartitionWriteGuard;

/// One unique-key write -- an INSERT, a DELETE, a MERGE -- as the commit protocol sees it:
///
///     Work outside the critical section, write the temp part
///     Enter the critical section for the partition
///       1. stage:   check conflicts, write the bitmaps, register them in the store. Durable,
///                   not yet visible.
///       2. publish: register the part on the transaction. Active, not yet visible.
///       3. commit:  csn = TransactionLog::commitTransaction(). The bitmaps become visible.
///                   If the commit's Keeper reply was lost, wait until the transaction resolves.
///     Exit the critical section for the partition
///
/// A write may stage several bitmaps and publishes one part. `UniqueKeyTxnManager` drives the
/// steps; the implementation supplies `stage` and `publish`.
class IUniqueKeyCommit
{
public:
    struct StagedWrite
    {
        /// The parts a staged bitmap was written for
        std::vector<MergeTreePartInfo> targets;
        /// The links to the bitmaps this write copied in rather than originated -- see
        /// `DeleteBitmapStore::selectCarriedBitmaps`. The bytes are already on disk by then.
        std::vector<DeleteBitmapStore::BitmapLink> carried;

        /// Every target this write is now on the hook for, in either role.
        std::vector<MergeTreePartInfo> allTargets() const
        {
            std::vector<MergeTreePartInfo> all = targets;
            for (const auto & link : carried)
                all.push_back(link.target);
            return all;
        }
    };

    virtual ~IUniqueKeyCommit() = default;

    /// The partition this write publishes into
    virtual String partitionId() const = 0;

    /// Names this write in the log. Every line the commit protocol emits is keyed by it and the
    /// partition, so one grep replays a whole commit.
    virtual std::string_view writeKind() const = 0;

    /// Put everything on disk inside the part this write will publish
    virtual std::optional<StagedWrite> stage(const PartitionWriteGuard & guard) = 0;

    /// Register the part `stage` filled on `txn`, making it Active but not yet visible.
    /// Takes the same guard `stage` was given -- one hold has to span both.
    virtual const IMergeTreeDataPart & publish(
        const PartitionWriteGuard & guard, const MergeTreeTransactionPtr & txn, const StagedWrite & staged) = 0;
};

/// Begin the transaction a unique-key write commits under, refusing the query's explicit one.
MergeTreeTransactionHolder beginUniqueKeyTransaction(const ContextPtr & context, std::string_view operation);

/// For a caller with no query context that knows the answer -- today only the background merge task,
/// which is handed the OPTIMIZE query's transaction as a member. Prefer the overload above: it
/// cannot be passed the wrong transaction.
/// The snapshot covers every source part's creation csn.
MergeTreeTransactionHolder beginUniqueKeyTransaction(
    const MergeTreeTransactionPtr & current, std::string_view operation, const std::vector<MergeTreeDataPartPtr> & source_parts);

class UniqueKeyTxnManager
{
public:
    UniqueKeyTxnManager(const MergeTreeData & data_, DeleteBitmapStorePtr delete_bitmap_store_);

    DeleteBitmapStore & deleteBitmapStore() { return *delete_bitmap_store; }

    /// Commit a write under a transaction, returning the commit sequence number of the commit point.
    /// Throws if a lost commit reply resolves to a rollback, or if @cancelled turns true while it is unresolved.
    CSN commitTransaction(
        MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & write, const std::atomic<bool> * cancelled = nullptr);

    /// Returns once every write that published a part into @partition_id before the call has left its commit.
    void waitForCommitsInFlight(const String & partition_id);

private:
    /// The pessimistic write lock for a partition
    std::mutex & partitionLock(const String & partition_id);

    /// Created on demand, never evicted. Node-based, so a reference survives a rehash.
    std::mutex partition_locks_mutex;
    std::unordered_map<String, std::mutex> partition_locks;

    const MergeTreeData & data;
    DeleteBitmapStorePtr delete_bitmap_store;

    LoggerPtr log;
};

using UniqueKeyTxnManagerPtr = std::unique_ptr<UniqueKeyTxnManager>;

}
