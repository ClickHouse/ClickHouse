#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/TransactionManager.h>
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
    /// A peer can declare this replica dead between `beginTransaction` and this constructor, and
    /// the rollback that follows does not wait for the holder. `onDestroy` already skips a
    /// transaction that is no longer running.
    const auto state = txn ? txn->getState() : MergeTreeTransaction::RUNNING;
    chassert(state == MergeTreeTransaction::RUNNING || state == MergeTreeTransaction::ROLLED_BACK);
    chassert(!owned_by_session_context || owned_by_session_context == owned_by_session_context->getSessionContext().get());
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
            TransactionManager::instance().commitTransaction(txn, /* throw_on_unknown_status */ false);
            return;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    TransactionManager::instance().rollbackTransaction(txn);
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
    chassert(!txn);
    chassert(!autocommit);
    chassert(!owned_by_session_context);
    return *this;
}

}
