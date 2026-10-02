#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>

#include <Interpreters/TransactionManager.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <optional>
#include <thread>
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

/// Why a lost-reply wait has to give up, or empty while it may go on. Server shutdown kills queries and
/// cancels background merges and mutations long before it stops the transaction log.
std::string_view reasonToStopWaiting(const std::atomic<bool> * cancelled)
{
    if (cancelled && cancelled->load())
        return "a cancelled background task";
    if (CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
        return "a killed query";
    if (TransactionManager::instance().isShuttingDown())
        return "shutdown";
    return {};
}

/// Polls `resolved` every 100 ms rather than waiting on a condition, which nothing wakes for the reasons above;
/// throws UNKNOWN_STATUS_OF_TRANSACTION once one of them holds.
void waitUntilResolved(const std::function<bool()> & resolved, const std::atomic<bool> * cancelled, std::string_view what)
{
    while (!resolved())
    {
        if (const auto reason = reasonToStopWaiting(cancelled); !reason.empty())
            throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION, "UNIQUE KEY {}, stopped waiting on {}", what, reason);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

/// The commit's Keeper reply was lost. The transaction log's updating thread resolves the transaction once
/// it knows whether the csn entry exists.
CSN waitForLostCommitReply(const MergeTreeTransactionPtr & txn, std::string_view kind, const std::atomic<bool> * cancelled)
{
    waitUntilResolved(
        [&] { return txn->getState() == MergeTreeTransaction::COMMITTED || txn->getState() == MergeTreeTransaction::ROLLED_BACK; },
        cancelled,
        fmt::format("{}: transaction {} lost its commit reply and is still unresolved", kind, txn->tid));

    if (txn->getState() == MergeTreeTransaction::ROLLED_BACK)
        throw Exception(ErrorCodes::ABORTED,
            "UNIQUE KEY {}: transaction {} lost its commit reply and was rolled back, retry the query", kind, txn->tid);
    return txn->getCSN();
}

bool hasUnresolvedPart(const MergeTreeData & data, const String & partition_id)
{
    MergeTreeData::DataPartsVector parts;
    {
        auto parts_lock = data.readLockParts();
        parts = data.getDataPartsVectorInPartitionForInternalUsage(MergeTreeData::DataPartState::Active, partition_id, parts_lock);
    }
    return std::ranges::any_of(parts, [](const auto & part) { return part->version->getInfo().creation_csn == Tx::UnknownCSN; });
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

void throwIfInsideTransaction(const MergeTreeTransactionPtr & current, std::string_view operation)
{
    if (current)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "{} on a UNIQUE KEY table is not supported inside an explicit transaction", operation);
}

MergeTreeTransactionHolder beginUniqueKeyTransaction(const MergeTreeTransactionPtr & current, std::string_view operation)
{
    throwIfInsideTransaction(current, operation);

    return MergeTreeTransactionHolder(TransactionManager::instance().beginTransaction(), /*autocommit=*/false);
}

MergeTreeTransactionHolder beginUniqueKeyTransaction(const ContextPtr & context, std::string_view operation)
{
    return beginUniqueKeyTransaction(context->getCurrentTransaction(), operation);
}

CSN UniqueKeyTxnManager::commitTransaction(
    MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & write, const std::atomic<bool> * cancelled)
{
    const MergeTreeTransactionPtr txn = transaction.getTransaction();
    chassert(txn, "UNIQUE KEY commit requires the transaction the part was written under");

    std::optional<IUniqueKeyCommit::StagedWrite> staged;
    std::optional<MergeTreePartInfo> registered_holder;
    CSN csn = INVALID_CSN;

    const std::string_view kind = write.writeKind();
    const String partition_id = write.partitionId();

    LOG_TRACE(log, "UNIQUE KEY {} (partition {}): waiting for the partition guard, tid {}",
        kind, partition_id, txn->tid);

    /// Outside the `try`, so a failed write is rolled back before the next writer probes the partition.
    std::optional<PartitionWriteGuard> write_guard(std::in_place, partitionLock(partition_id));

    try
    {
        /// Before staging, it needs to see the latest state
        waitForUnresolvedParts(partition_id, kind, txn->tid, cancelled);

        staged = write.stage(*write_guard);
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
        const IMergeTreeDataPart & holder = write.publish(*write_guard, txn, *staged);

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
            csn = waitForLostCommitReply(txn, kind, cancelled);
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

void UniqueKeyTxnManager::waitForUnresolvedParts(
    const String & partition_id, std::string_view kind, const TransactionID & tid, const std::atomic<bool> * cancelled) const
{
    /// Normally nothing is unresolved: every writer resolves inside the guard.
    if (!hasUnresolvedPart(data, partition_id))
        return;

    LOG_DEBUG(log, "UNIQUE KEY {} (partition {}): waiting for an unresolved part, tid {}", kind, partition_id, tid);
    waitUntilResolved(
        [&] { return !hasUnresolvedPart(data, partition_id); },
        cancelled,
        fmt::format("{} (partition {}): a part is still unresolved", kind, partition_id));
}

}
