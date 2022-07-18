#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeTransactionHolder::MergeTreeTransactionHolder(const MergeTreeTransactionPtr & txn_, bool autocommit_ = false, const Context * owned_by_session_context_)
    : txn(txn_)
    , autocommit(autocommit_)
    , owned_by_session_context(owned_by_session_context_)
{
    assert(!txn || txn->getState() == MergeTreeTransaction::RUNNING);
    assert(!owned_by_session_context || owned_by_session_context == owned_by_session_context->getSessionContext().get());
}

MergeTreeTransactionHolder::MergeTreeTransactionHolder(MergeTreeTransactionHolder && rhs) noexcept
{
    *this = std::move(rhs);
}

MergeTreeTransactionHolder & MergeTreeTransactionHolder::operator=(MergeTreeTransactionHolder && rhs) noexcept
{
    onDestroy();
    txn = NO_TRANSACTION_PTR;
    autocommit = false;
    owned_by_session_context = nullptr;
    std::swap(txn, rhs.txn);
    std::swap(autocommit, rhs.autocommit);
    std::swap(owned_by_session_context, rhs.owned_by_session_context);
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

    if (autocommit && std::uncaught_exceptions() == 0)
    {
        try
        {
            TransactionLog::instance().commitTransaction(txn);
            return;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    TransactionLog::instance().rollbackTransaction(txn);
}

MergeTreeTransactionHolder::MergeTreeTransactionHolder(const MergeTreeTransactionHolder & rhs)
{
    *this = rhs;
}

MergeTreeTransactionHolder & MergeTreeTransactionHolder::operator=(const MergeTreeTransactionHolder & rhs)  // NOLINT
{
    if (rhs.txn && !rhs.owned_by_session_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Tried to copy non-empty MergeTreeTransactionHolder that is not owned by session context. It's a bug");
    assert(!txn);
    assert(!autocommit);
    assert(!owned_by_session_context);
    return *this;
}

}
