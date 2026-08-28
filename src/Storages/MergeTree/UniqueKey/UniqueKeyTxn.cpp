#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>

#include <Interpreters/TransactionLog.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

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
    extern const int SUPPORT_IS_DISABLED;
}

namespace FailPoints
{
    extern const char unique_key_defer_bitmap_settle[];
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
        TransactionLog::instance().rollbackTransaction(txn);
}

}

UniqueKeyTxnManager::UniqueKeyTxnManager(BitmapStorePtr bitmap_store_)
    : bitmap_store(std::move(bitmap_store_))
    , log(getLogger("UniqueKeyTxnManager"))
{
    chassert(bitmap_store, "UniqueKeyTxnManager requires a non-null bitmap store");
}

std::mutex & UniqueKeyTxnManager::partitionLock(const String & partition_id)
{
    std::lock_guard registry_lock(partition_locks_mutex);
    return partition_locks[partition_id];
}

size_t UniqueKeyTxnManager::runGCRound(const String & partition_id, const std::vector<MergeTreePartInfo> & parts)
{
    PartitionWriteGuard write_guard(partitionLock(partition_id));

    const CSN oldest_snapshot = TransactionLog::instance().getOldestSnapshot();

    size_t reclaimed = 0;
    for (const auto & part : parts)
        reclaimed += bitmap_store->removeObsoleteBitmaps(part, oldest_snapshot);

    return reclaimed;
}

MergeTreeTransactionHolder beginUniqueKeyTransaction(const MergeTreeTransactionPtr & current, std::string_view operation)
{
    if (current)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "{} on a UNIQUE KEY table is not supported inside an explicit transaction", operation);

    return MergeTreeTransactionHolder(TransactionLog::instance().beginTransaction(), /*autocommit=*/false);
}

MergeTreeTransactionHolder beginUniqueKeyTransaction(const ContextPtr & context, std::string_view operation)
{
    return beginUniqueKeyTransaction(context->getCurrentTransaction(), operation);
}

CSN UniqueKeyTxnManager::commitTransaction(MergeTreeTransactionHolder & transaction, IUniqueKeyCommit & write)
{
    const MergeTreeTransactionPtr txn = transaction.getTransaction();
    chassert(txn, "UNIQUE KEY commit requires the transaction the part was written under");

    std::optional<IUniqueKeyCommit::StagedWrite> staged;
    std::optional<MergeTreePartInfo> registered_owner;
    CSN csn = INVALID_CSN;

    try
    {
        PartitionWriteGuard write_guard(partitionLock(write.partitionId()));

        staged = write.stage(write_guard);
        if (!staged)
        {
            rollbackTransaction(txn);
            return INVALID_CSN;
        }

        /// Must precede the commit, which moves the transaction to `CommittingCSN`: `addNewPart`
        /// rejects a transaction already there. The part is Active but not yet visible.
        const IMergeTreeDataPart & owner = write.publish(write_guard, txn, *staged);

        /// Before the commit point, so the moment this part becomes visible its staged bitmaps
        /// are already discoverable from their targets.
        bitmapStore().registerStagedBitmaps(owner.info, staged->targets);
        registered_owner = owner.info;

        /// Commit point
        csn = TransactionLog::instance().commitTransaction(txn, /*throw_on_unknown_status=*/true);

        bool defer_settle = false;
        fiu_do_on(FailPoints::unique_key_defer_bitmap_settle, { defer_settle = true; });

        if (!defer_settle)
        {
            /// TODO(unique-key): move the settle out of the write lock
            const auto report = bitmapStore().settleStagedBitmaps(owner.info, staged->targets, csn);
            if (report.anyOutstanding())
                LOG_WARNING(log,
                    "Staged bitmaps of part {} are not fully settled at csn {} ({} deferred, {} "
                    "failed{}); the next settle will retry",
                    owner.name, csn, report.deferred, report.failed,
                    report.owner_unresolved ? ", owner unresolved" : "");
        }
    }
    catch (...)
    {
        rollbackTransaction(txn);

        if (registered_owner && txn && txn->getState() == MergeTreeTransaction::ROLLED_BACK)
            bitmapStore().forgetStagedBitmaps(*registered_owner, staged->targets);

        throw;
    }

    /// Read-your-own-writes: `latest_snapshot` only advances on the updating thread, so a
    /// SELECT issued right after this would otherwise bind a snapshot below `csn`.
    TransactionLog::instance().waitForCSNLoaded(csn);

    return csn;
}

IBitmapStore::SettleReport UniqueKeyTxnManager::settleStagedBitmaps(const IMergeTreeDataPart & part)
{
    /// Read the authoritative CSN
    const CSN csn = part.version->getInfo().creation_csn;

    if (csn == Tx::UnknownCSN || csn == Tx::RolledBackCSN)
        return {};

    /// Defer the settle
    if (csn == Tx::CommittingCSN)
        return {.deferred = bitmap_store->stagedTargetsOf(part.info).size()};

    return bitmap_store->settleStagedBitmaps(part.info, csn);
}

void UniqueKeyTxnManager::runRecovery(const std::vector<MergeTreeDataPartPtr> & parts)
{
    auto component_guard = Coordination::setCurrentComponent("UniqueKeyTxnManager::runRecovery");

    for (const auto & part : parts)
    {
        try
        {
            settleStagedBitmaps(*part);
        }
        catch (...)
        {
            /// Not fail-closed: a staged bitmap left in place is still readable through its
            /// owner, and the next settle retries it.
            tryLogCurrentException(log,
                fmt::format("Txn-state recovery: could not reconcile sidecars of part '{}'",
                    part->name));
        }
    }
}

}
