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

#include <chrono>
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

/// The commit's Keeper reply was lost. The transaction log's updating thread resolves the transaction once
/// it knows whether the csn entry exists. Polled rather than `waitStateChange`, which nothing wakes for
/// the reasons above.
CSN waitForLostCommitReply(const MergeTreeTransactionPtr & txn, std::string_view kind, const std::atomic<bool> * cancelled)
{
    while (true)
    {
        const auto state = txn->getState();
        if (state == MergeTreeTransaction::COMMITTED)
            return txn->getCSN();

        if (state == MergeTreeTransaction::ROLLED_BACK)
            throw Exception(ErrorCodes::ABORTED,
                "UNIQUE KEY {}: transaction {} lost its commit reply and was rolled back, retry the query", kind, txn->tid);

        if (const auto reason = reasonToStopWaiting(cancelled); !reason.empty())
            throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION,
                "UNIQUE KEY {}: transaction {} lost its commit reply and is still {}, stopped waiting on {}",
                kind, txn->tid, state, reason);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

}

UniqueKeyTxnManager::UniqueKeyTxnManager(DeleteBitmapStorePtr delete_bitmap_store_)
    : delete_bitmap_store(std::move(delete_bitmap_store_))
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

namespace
{

/// A part is active from `publish` on, inside its commit's partition guard, so an unresolved creation is
/// waited out there.
CSN creationCSN(const IMergeTreeDataPart & part)
{
    const VersionInfo info = part.version->getInfo();
    CSN csn = info.creation_csn != Tx::UnknownCSN ? info.creation_csn : TransactionManager::getCSN(info.creation_tid);
    if (csn == Tx::UnknownCSN)
    {
        part.storage.uniqueKeyTxnManager().waitForCommitsInFlight(part.info.getPartitionId());
        csn = TransactionManager::getCSN(info.creation_tid);
    }

    if (csn == Tx::UnknownCSN || csn == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::SERIALIZATION_ERROR,
            "Source part {} was created by transaction {}, which is not committed", part.name, info.creation_tid);
    return csn;
}

}

MergeTreeTransactionHolder beginUniqueKeyTransaction(
    const MergeTreeTransactionPtr & current, std::string_view operation, const std::vector<MergeTreeDataPartPtr> & source_parts)
{
    if (current)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "{} on a UNIQUE KEY table is not supported inside an explicit transaction", operation);

    /// A commit stamps its parts' csn before `latest_snapshot` reaches it, and the scheduler takes a part
    /// as soon as its creation resolves. A snapshot below that csn misses the source and still sees the part
    /// the source replaced, which the commit then fails to remove.
    auto & manager = TransactionManager::instance();
    CSN sources_csn = Tx::NonTransactionalCSN;
    for (const auto & part : source_parts)
        sources_csn = std::max(sources_csn, creationCSN(*part));
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
    MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & write, const std::atomic<bool> * cancelled)
{
    const MergeTreeTransactionPtr txn = transaction.getTransaction();
    chassert(txn, "UNIQUE KEY commit requires the transaction the part was written under");

    std::optional<IUniqueKeyCommit::StagedWrite> staged;
    std::optional<MergeTreePartInfo> registered_holder;
    CSN csn = INVALID_CSN;

    const std::string_view kind = write.writeKind();
    const String partition_id = write.partitionId();

    try
    {
        LOG_TRACE(log, "UNIQUE KEY {} (partition {}): waiting for the partition guard, tid {}",
            kind, partition_id, txn->tid);

        {
            PartitionWriteGuard write_guard(partitionLock(partition_id));

            staged = write.stage(write_guard);
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
            const IMergeTreeDataPart & holder = write.publish(write_guard, txn, *staged);

            LOG_TRACE(log, "UNIQUE KEY {} (partition {}): published part {}, not yet visible",
                kind, partition_id, holder.name);

            /// Before the commit point, so the moment this part becomes visible its staged bitmaps
            /// are already discoverable from their targets.
            deleteBitmapStore().registerStagedBitmaps(holder.info, staged->targets);
            deleteBitmapStore().registerLinks(holder.info, staged->carried);
            registered_holder = holder.info;

            /// Commit point
            csn = TransactionManager::instance().commitTransaction(txn, /*throw_on_unknown_status=*/false);
        }

        /// Outside the guard: the answer needs Keeper, and the partition's other writers must not wait for it.
        if (csn == Tx::CommittingCSN)
        {
            LOG_DEBUG(log, "UNIQUE KEY {} (partition {}): lost the commit reply, waiting for tid {} to resolve",
                kind, partition_id, txn->tid);
            csn = waitForLostCommitReply(txn, kind, cancelled);
        }

        LOG_TRACE(log, "UNIQUE KEY {} (partition {}): committed tid {} at csn {}",
            kind, partition_id, txn->tid, csn);
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

    /// Read-your-own-writes: `latest_snapshot` only advances on the updating thread, so a
    /// SELECT issued right after this would otherwise bind a snapshot below `csn`.
    TransactionManager::instance().waitForCSNLoaded(csn);

    return csn;
}

}
