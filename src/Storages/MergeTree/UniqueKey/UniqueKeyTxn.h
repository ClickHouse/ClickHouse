#pragma once

#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>

#include <unordered_map>
#include <mutex>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
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
///       3. commit:  csn = TransactionLog::commitTransaction(). Staged bitmaps become visible.
///       4. settle:  move the staged bitmaps into their targets under the assigned csn.
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
MergeTreeTransactionHolder beginUniqueKeyTransaction(const MergeTreeTransactionPtr & current, std::string_view operation);

/// Unique-key's per-table transaction manager, built on top of the TransactionLog and IBitmapStore.
class UniqueKeyTxnManager
{
public:
    explicit UniqueKeyTxnManager(BitmapStorePtr bitmap_store_);

    IBitmapStore & bitmapStore() { return *bitmap_store; }

    /// Commit a write under a transaction, returning the commit sequence number of the commit point.
    CSN commitTransaction(MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & write);

    /// Settle every bitmap staged inside `part`, into its targets, at `part`'s own `creation_csn`.
    IBitmapStore::SettleReport settleStagedBitmaps(const IMergeTreeDataPart & part);

    /// Run a garbage collection round for the given partition and its parts.
    size_t runGCRound(const String & partition_id, const std::vector<MergeTreePartInfo> & parts);

    /// Publish every part's staged bitmaps into their targets at the owner's csn. Runs once at
    /// table load, over the whole part set -- an owner still without a csn waits for the next
    /// settle, and a rolled-back owner's bitmaps go with its directory.
    void runRecovery(const std::vector<MergeTreeDataPartPtr> & parts);

private:
    /// The pessimistic write lock for a partition
    std::mutex & partitionLock(const String & partition_id);

    /// Created on demand, never evicted. Node-based, so a reference survives a rehash.
    std::mutex partition_locks_mutex;
    std::unordered_map<String, std::mutex> partition_locks;

    BitmapStorePtr bitmap_store;

    LoggerPtr log;
};

using UniqueKeyTxnManagerPtr = std::unique_ptr<UniqueKeyTxnManager>;

}
