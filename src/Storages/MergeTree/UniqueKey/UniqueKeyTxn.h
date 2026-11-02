#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>
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
class MergeTreeData;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

/// Proof that the caller holds a partition's write guard. `UniqueKeyTxnManager::commitTransaction`
/// is the only thing that can construct one, and it hands the same one to `stage` and `publish`.
class PartitionWriteGuard;

/// One unique-key write (INSERT, DELETE, MERGE): its part and the bitmaps that kill the rows it replaces
/// become visible at one csn. Inside the partition's critical section, so `stage` sees every committed write:
///   0. refuse the write while a part's creation csn is not stamped yet (`UniqueKeyTxnManager::throwIfUnresolvedPart`);
///   1. stage: write the bitmaps. Durable, not visible;
///   2. publish: register the part on the transaction. Active, not visible; `addNewPart` refuses a
///      committing transaction, so this precedes the commit;
///   3. commit. A lost Keeper reply is waited out here, so the next writer never stages beside a part that
///      may be live.
/// Then wait for `latest_snapshot` to reach the csn, so the writer reads its own write.
/// `UniqueKeyTxnManager` drives the steps; the implementation supplies `stage` and `publish`.
class IUniqueKeyCommit
{
public:
    struct StagedWrite
    {
        /// The parts a staged bitmap was written for
        std::vector<MergeTreePartInfo> targets;
        /// The bitmaps this write copied in rather than wrote (`DeleteBitmapStore::selectCarriedBitmaps`)
        std::vector<DeleteBitmapStore::BitmapLink> carried;

        /// `targets` plus the targets of `carried`
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

    /// Names this write in the commit protocol's log lines
    virtual std::string_view writeKind() const = 0;

    /// Put everything on disk inside the part this write will publish
    virtual std::optional<StagedWrite> stage(const PartitionWriteGuard & guard) = 0;

    /// Register the part `stage` filled on `txn`, under the same guard hold as `stage`
    virtual const IMergeTreeDataPart & publish(
        const PartitionWriteGuard & guard, const MergeTreeTransactionPtr & txn, const StagedWrite & staged) = 0;
};

/// Begin the transaction a unique-key write commits under, refusing the query's explicit one.
MergeTreeTransactionHolder beginUniqueKeyTransaction(const ContextPtr & context, std::string_view operation);

/// For a caller with no query context that knows the answer -- today only the background merge task,
/// which is handed the OPTIMIZE query's transaction as a member. Prefer the overload above: it
/// cannot be passed the wrong transaction.
MergeTreeTransactionHolder beginUniqueKeyTransaction(const MergeTreeTransactionPtr & current, std::string_view operation);

class UniqueKeyTxnManager
{
public:
    UniqueKeyTxnManager(const MergeTreeData & data_, DeleteBitmapStorePtr delete_bitmap_store_);

    DeleteBitmapStore & deleteBitmapStore() { return *delete_bitmap_store; }

    /// Commits one unique-key write (INSERT, DELETE, MERGE) under @transaction, returning the csn of the commit point.
    /// Throws ABORTED if a lost commit reply resolves to a rollback, or if the partition has an unresolved part:
    /// Active, with no creation csn stamped yet. Throws UNKNOWN_STATUS_OF_TRANSACTION if a lost reply is still
    /// unresolved at shutdown.
    ///
    /// Commit wait involve:
    /// - commits in flight: writers holding the partition guard;
    /// - a lost commit reply: the outcome of the writer's own transaction is unknown;
    /// - csn loaded: `latest_snapshot` has reached a csn.
    CSN commitTransaction(
        MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & commit);

private:
    /// Throws ABORTED if an Active part in @partition_id is unresolved
    void throwIfUnresolvedPart(const String & partition_id, std::string_view kind) const;

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
