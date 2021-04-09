#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/TransactionLog.h>

namespace DB
{

MergeTreeTransactionHolder::MergeTreeTransactionHolder(const MergeTreeTransactionPtr & txn_, bool autocommit_ = false)
    : txn(txn_)
    , autocommit(autocommit_)
{
    assert(!txn || txn->getState() == MergeTreeTransaction::RUNNING);
}

MergeTreeTransactionHolder::MergeTreeTransactionHolder(MergeTreeTransactionHolder && rhs) noexcept
    : txn(std::move(rhs.txn))
    , autocommit(rhs.autocommit)
{
    rhs.txn = {};
}

MergeTreeTransactionHolder & MergeTreeTransactionHolder::operator=(MergeTreeTransactionHolder && rhs) noexcept
{
    onDestroy();
    txn = std::move(rhs.txn);
    autocommit = rhs.autocommit;
    rhs.txn = {};
    return *this;
}

MergeTreeTransactionHolder::~MergeTreeTransactionHolder()
{
    onDestroy();
}

void MergeTreeTransactionHolder::onDestroy() noexcept
{
    if (!txn)
        return;
    if (txn->getState() != MergeTreeTransaction::RUNNING)
        return;

    if (autocommit)
    {
        try
        {
            TransactionLog::instance().commitTransaction(txn);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    } else
    {
        TransactionLog::instance().rollbackTransaction(txn);
    }
}

MergeTreeTransactionHolder::MergeTreeTransactionHolder(const MergeTreeTransactionHolder &)
{
    txn = nullptr;
}

MergeTreeTransactionHolder & MergeTreeTransactionHolder::operator=(const MergeTreeTransactionHolder &)
{
    txn = nullptr;
    return *this;
}

}
