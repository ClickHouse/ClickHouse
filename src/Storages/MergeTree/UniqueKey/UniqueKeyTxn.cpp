#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>

#include <Interpreters/TransactionManager.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>
#include <utility>

namespace ProfileEvents
{
    extern const Event UniqueKeyMutexHoldMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int SERIALIZATION_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
}

class PartitionWriteGuard
{
public:
    explicit PartitionWriteGuard(std::mutex & mutex)
        : guard(mutex), measure(ProfileEvents::UniqueKeyMutexHoldMicroseconds)
    {
    }

private:
    /// Declared before `measure`, so its stopwatch starts once the lock is in hand -- this times the
    /// hold, not the wait. Reordering these two silently changes what the counter means.
    std::unique_lock<std::mutex> guard;
    ProfileEventTimeIncrement<Time::Microseconds> measure;
};

namespace
{

/// Idempotent, and a no-op once the transaction has left RUNNING.
void rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept
{
    if (txn && txn->getState() == MergeTreeTransaction::RUNNING)
        TransactionManager::instance().rollbackTransaction(txn);
}

/// The commit's Keeper reply was lost. The transaction log's updating thread resolves the transaction once
/// it knows whether the csn entry exists.
CSN waitForLostCommitReply(const MergeTreeTransactionPtr & txn, std::string_view kind)
{
    txn->waitForCommitOutcome();

    if (txn->getState() == MergeTreeTransaction::ROLLED_BACK)
        throw Exception(ErrorCodes::ABORTED,
            "UNIQUE KEY {}: transaction {} lost its commit reply and was rolled back, retry the query", kind, txn->tid);
    if (txn->getState() != MergeTreeTransaction::COMMITTED)
        throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION,
            "UNIQUE KEY {}: transaction {} lost its commit reply and is unresolved, will finalize it later", kind, txn->tid);
    return txn->getCSN();
}

}

UniqueKeyTxnManager::UniqueKeyTxnManager(const MergeTreeData & data_, DeleteBitmapStorePtr delete_bitmap_store_)
    : data(data_)
    , delete_bitmap_store(std::move(delete_bitmap_store_))
    , log(getLogger("UniqueKeyTxnManager"))
{
    chassert(delete_bitmap_store, "UniqueKeyTxnManager requires a non-null delete bitmap store");
}

std::mutex & UniqueKeyTxnManager::partitionLock(const String & partition_id)
{
    std::lock_guard registry_lock(partition_locks_mutex);
    return partition_locks[partition_id];
}

void UniqueKeyTxnManager::waitForCommitsInFlight(const String & partition_id)
{
    std::lock_guard wait_out(partitionLock(partition_id));
}

/// A part is active from `publish` on, inside its commit's partition guard, so an unresolved creation is
/// waited out there.
CSN UniqueKeyTxnManager::creationCSN(const IMergeTreeDataPart & part)
{
    const VersionInfo info = part.version->getInfo();
    CSN csn = info.creation_csn != Tx::UnknownCSN ? info.creation_csn : TransactionManager::getCSN(info.creation_tid);
    if (csn == Tx::UnknownCSN)
    {
        waitForCommitsInFlight(part.info.getPartitionId());
        csn = TransactionManager::getCSN(info.creation_tid);
    }

    if (csn == Tx::UnknownCSN || csn == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::SERIALIZATION_ERROR,
            "Source part {} was created by transaction {}, which is not committed", part.name, info.creation_tid);
    return csn;
}

void throwIfInsideTransaction(const MergeTreeTransactionPtr & current, std::string_view operation)
{
    if (current)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "{} on a UNIQUE KEY table is not supported inside an explicit transaction", operation);
}

MergeTreeTransactionHolder beginUniqueKeyTransaction(
    const MergeTreeTransactionPtr & current, std::string_view operation, const std::vector<MergeTreeDataPartPtr> & source_parts)
{
    throwIfInsideTransaction(current, operation);

    /// A commit stamps its parts' csn before `latest_snapshot` reaches it, and the scheduler takes a part
    /// as soon as its creation resolves. A snapshot below that csn misses the source and still sees the part
    /// the source replaced, which the commit then fails to remove.
    auto & manager = TransactionManager::instance();
    CSN sources_csn = Tx::NonTransactionalCSN;
    for (const auto & part : source_parts)
        sources_csn = std::max(sources_csn, part->storage.uniqueKeyTxnManager().creationCSN(*part));
    manager.waitForCSNLoaded(sources_csn);

    auto txn = manager.beginTransaction();
    chassert(txn->getSnapshot() >= sources_csn || manager.isShuttingDown(),
        fmt::format("snapshot {}, newest source csn {}", txn->getSnapshot(), sources_csn));
    return MergeTreeTransactionHolder(txn, /*autocommit=*/false);
}

MergeTreeTransactionHolder beginUniqueKeyTransaction(const ContextPtr & context, std::string_view operation)
{
    return beginUniqueKeyTransaction(context->getCurrentTransaction(), operation, /*source_parts=*/{});
}

CSN UniqueKeyTxnManager::commitTransaction(
    MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & commit)
{
    const MergeTreeTransactionPtr txn = transaction.getTransaction();
    chassert(txn, "UNIQUE KEY commit requires the transaction the part was written under");

    std::optional<IUniqueKeyCommit::StagedWrite> staged;
    std::optional<MergeTreePartInfo> registered_holder;
    CSN csn = INVALID_CSN;

    const std::string_view kind = commit.writeKind();
    const String partition_id = commit.partitionId();

    LOG_TRACE(log, "UNIQUE KEY {} (partition {}): waiting for the partition guard, tid {}",
        kind, partition_id, txn->tid);

    /// Outside the `try`, so a failed write is rolled back before the next writer probes the partition.
    std::optional<PartitionWriteGuard> write_guard(std::in_place, partitionLock(partition_id));

    try
    {
        /// Before staging, it needs to see the latest state.
        /// TODO(unique-key): wait for the part's writer again once commit waits can be interrupted.
        throwIfUnresolvedPart(partition_id, kind);

        staged = commit.stage(*write_guard);
        if (!staged)
        {
            LOG_DEBUG(log, "UNIQUE KEY {} (partition {}): staged nothing, rolling back tid {}",
                kind, partition_id, txn->tid);
            rollbackTransaction(txn);
            return INVALID_CSN;
        }

        LOG_TRACE(log, "UNIQUE KEY {} (partition {}): staged bitmaps for {} target(s), carried {}",
            kind, partition_id, staged->targets.size(), staged->carried.size());

        /// Must precede the commit, which moves the transaction to `CommittingCSN`: `addNewPart`
        /// rejects a transaction already there. The part is Active but not yet visible.
        const IMergeTreeDataPart & holder = commit.publish(*write_guard, txn, *staged);

        LOG_TRACE(log, "UNIQUE KEY {} (partition {}): published part {}, not yet visible",
            kind, partition_id, holder.name);

        /// Before the commit point, so the moment this part becomes visible its staged bitmaps
        /// are already discoverable from their targets.
        deleteBitmapStore().registerStagedBitmaps(holder.info, staged->targets);
        deleteBitmapStore().registerLinks(holder.info, staged->carried);
        registered_holder = holder.info;

        /// Commit point
        csn = TransactionManager::instance().commitTransaction(txn, /*throw_on_unknown_status=*/false);

        /// Still inside the guard: a writer that probed this partition now would find this part live beside the
        /// row it replaced, with the kill that hides that row not yet visible.
        if (csn == Tx::CommittingCSN)
        {
            LOG_DEBUG(log, "UNIQUE KEY {} (partition {}): lost the commit reply, waiting for tid {} to resolve",
                kind, partition_id, txn->tid);
            csn = waitForLostCommitReply(txn, kind);
        }

        LOG_TRACE(log, "UNIQUE KEY {} (partition {}): committed part {} at csn {}",
            kind, partition_id, holder.name, csn);
    }
    catch (...)
    {
        LOG_DEBUG(log, "UNIQUE KEY {} (partition {}): threw, rolling back tid {}",
            kind, partition_id, txn->tid);
        rollbackTransaction(txn);

        if (registered_holder && txn && txn->getState() == MergeTreeTransaction::ROLLED_BACK)
            deleteBitmapStore().removeStagedBitmaps(*registered_holder, staged->allTargets());

        throw;
    }

    write_guard.reset();

    /// Read-your-own-writes: `latest_snapshot` only advances on the updating thread, so a
    /// SELECT issued right after this would otherwise bind a snapshot below `csn`.
    TransactionManager::instance().waitForCSNLoaded(csn);

    return csn;
}

void UniqueKeyTxnManager::throwIfUnresolvedPart(const String & partition_id, std::string_view kind) const
{
    const auto parts = data.getDataPartsVectorInPartitionForInternalUsage(MergeTreeData::DataPartState::Active, partition_id);

    /// Normally nothing is unresolved: every writer resolves inside the guard.
    const auto it = std::ranges::find_if(parts, [](const auto & part) { return part->version->getInfo().creation_csn == Tx::UnknownCSN; });
    if (it != parts.end())
        throw Exception(ErrorCodes::ABORTED,
            "UNIQUE KEY {} (partition {}): part {} has no creation csn yet; retry the query", kind, partition_id, (*it)->name);
}

}
