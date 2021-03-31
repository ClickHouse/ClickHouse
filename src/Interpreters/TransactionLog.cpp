#include <Interpreters/TransactionLog.h>

namespace DB
{

TransactionLog & TransactionLog::instance()
{
    static TransactionLog inst;
    return inst;
}

TransactionLog::TransactionLog()
{
    csn_counter = 1;
    local_tid_counter = 1;
}

Snapshot TransactionLog::getLatestSnapshot() const
{
    return csn_counter.load();
}

MergeTreeTransactionPtr TransactionLog::beginTransaction()
{
    Snapshot snapshot = csn_counter.load();
    LocalTID ltid = 1 + local_tid_counter.fetch_add(1);
    return std::make_shared<MergeTreeTransaction>(snapshot, ltid, UUIDHelpers::Nil);
}

CSN TransactionLog::commitTransaction(const MergeTreeTransactionPtr & txn)
{
    txn->csn = 1 + csn_counter.fetch_add(1);
    /// TODO Transactions: reset local_tid_counter
    txn->state = MergeTreeTransaction::COMMITTED;
    return txn->csn;
}

void TransactionLog::rollbackTransaction(const MergeTreeTransactionPtr & txn)
{
    txn->csn = Tx::RolledBackCSN;
    txn->state = MergeTreeTransaction::ROLLED_BACK;
}

}
