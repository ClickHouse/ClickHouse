#include <Interpreters/TransactionLog.h>
#include <Common/TransactionMetadata.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

TransactionLog & TransactionLog::instance()
{
    static TransactionLog inst;
    return inst;
}

TransactionLog::TransactionLog()
{
    latest_snapshot = 1;
    csn_counter = 1;
    local_tid_counter = 1;
}

Snapshot TransactionLog::getLatestSnapshot() const
{
    return latest_snapshot.load();
}

MergeTreeTransactionPtr TransactionLog::beginTransaction()
{
    Snapshot snapshot = latest_snapshot.load();
    LocalTID ltid = 1 + local_tid_counter.fetch_add(1);
    auto txn = std::make_shared<MergeTreeTransaction>(snapshot, ltid, UUIDHelpers::Nil);
    {
        std::lock_guard lock{running_list_mutex};
        bool inserted = running_list.try_emplace(txn->tid.getHash(), txn).second;     /// Commit point
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "I's a bug: TID {} {} exists", txn->tid.getHash(), txn->tid);
    }
    return txn;
}

CSN TransactionLog::commitTransaction(const MergeTreeTransactionPtr & txn)
{
    txn->beforeCommit();

    CSN new_csn;
    /// TODO Transactions: reset local_tid_counter
    if (txn->isReadOnly())
    {
        new_csn = txn->snapshot;
    }
    else
    {
        std::lock_guard lock{commit_mutex};
        new_csn = 1 + csn_counter.fetch_add(1);
        bool inserted = tid_to_csn.try_emplace(txn->tid.getHash(), new_csn).second;     /// Commit point
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "I's a bug: TID {} {} exists", txn->tid.getHash(), txn->tid);
        latest_snapshot.store(new_csn, std::memory_order_relaxed);
    }

    txn->afterCommit(new_csn);

    {
        std::lock_guard lock{running_list_mutex};
        bool removed = running_list.erase(txn->tid.getHash());
        if (!removed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "I's a bug: TID {} {} doesn't exist", txn->tid.getHash(), txn->tid);
    }
    return new_csn;
}

void TransactionLog::rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept
{
    txn->rollback();
    {
        std::lock_guard lock{running_list_mutex};
        bool removed = running_list.erase(txn->tid.getHash());
        if (!removed)
            abort();
    }
}

MergeTreeTransactionPtr TransactionLog::tryGetRunningTransaction(const TIDHash & tid)
{
    std::lock_guard lock{running_list_mutex};
    auto it = running_list.find(tid);
    if (it == running_list.end())
        return nullptr;
    return it->second;
}

CSN TransactionLog::getCSN(const TransactionID & tid) const
{
    return getCSN(tid.getHash());
}

CSN TransactionLog::getCSN(const TIDHash & tid) const
{
    assert(tid);
    assert(tid != Tx::EmptyTID.getHash());
    if (tid == Tx::PrehistoricTID.getHash())
        return Tx::PrehistoricCSN;

    std::lock_guard lock{commit_mutex};
    auto it = tid_to_csn.find(tid);
    if (it == tid_to_csn.end())
        return Tx::UnknownCSN;
    return it->second;
}

}
