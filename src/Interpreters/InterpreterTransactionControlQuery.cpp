#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterTransactionControlQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsTransactionsWaitCSNMode wait_changes_become_visible_after_commit_mode;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_TRANSACTION;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
}

BlockIO InterpreterTransactionControlQuery::execute()
{
    if (!query_context->hasSessionContext())
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction Control Language queries are allowed only inside session");

    ContextMutablePtr session_context = query_context->getSessionContext();
    const auto & tcl = query_ptr->as<const ASTTransactionControl &>();

    switch (tcl.action)
    {
        case ASTTransactionControl::BEGIN:
            return executeBegin(session_context);
        case ASTTransactionControl::COMMIT:
            return executeCommit(session_context);
        case ASTTransactionControl::ROLLBACK:
            return executeRollback(session_context);
        case ASTTransactionControl::SET_SNAPSHOT:
            return executeSetSnapshot(session_context, tcl.snapshot);
    }
}

BlockIO InterpreterTransactionControlQuery::executeBegin(ContextMutablePtr session_context)
{
    if (session_context->getCurrentTransaction())
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Nested transactions are not supported");

    session_context->checkTransactionsAreAllowed(/* explicit_tcl_query = */ true);
    auto txn = TransactionLog::instance().beginTransaction();
    session_context->initCurrentTransaction(txn);
    query_context->setCurrentTransaction(txn);
    return {};
}

BlockIO InterpreterTransactionControlQuery::executeCommit(ContextMutablePtr session_context)
{
    auto txn = session_context->getCurrentTransaction();
    if (!txn)
    {
        if (session_context->getClientInfo().interface == ClientInfo::Interface::MYSQL)
            return {};
        else
            throw Exception(ErrorCodes::INVALID_TRANSACTION, "There is no current transaction");
    }
    if (txn->getState() != MergeTreeTransaction::RUNNING)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction is not in RUNNING state");

    TransactionsWaitCSNMode mode = query_context->getSettingsRef()[Setting::wait_changes_become_visible_after_commit_mode];
    CSN csn;
    try
    {
        csn = TransactionLog::instance().commitTransaction(txn, /* throw_on_unknown_status */ mode != TransactionsWaitCSNMode::WAIT_UNKNOWN);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION)
        {
            /// Detach transaction from current context if connection was lost and its status is unknown
            /// (so it will be possible to start new one)
            session_context->setCurrentTransaction(NO_TRANSACTION_PTR);
        }
        throw;
    }

    if (csn == Tx::CommittingCSN)
    {
        chassert(mode == TransactionsWaitCSNMode::WAIT_UNKNOWN);

        /// Try to wait for connection to be restored and its status to be loaded.
        /// It's useful for testing. It allows to enable fault injection (after commit) without breaking tests.
        txn->waitStateChange(Tx::CommittingCSN);

        CSN csn_changed_state = txn->getCSN();
        if (csn_changed_state == Tx::UnknownCSN)
        {
            /// CommittingCSN -> UnknownCSN -> RolledBackCSN
            /// It's possible if connection was lost before commit
            /// (maybe we should get rid of intermediate UnknownCSN in this transition)
            txn->waitStateChange(Tx::UnknownCSN);
            chassert(txn->getCSN() == Tx::RolledBackCSN);
        }

        if (txn->getState() == MergeTreeTransaction::ROLLED_BACK)
            throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction {} was rolled back", txn->tid);
        if (txn->getState() != MergeTreeTransaction::COMMITTED)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction {} has invalid state {}", txn->tid, txn->getState());

        csn = txn->getCSN();
    }

    /// Wait for committed changes to become actually visible, so the next transaction in this session will see the changes
    if (mode != TransactionsWaitCSNMode::ASYNC)
        TransactionLog::instance().waitForCSNLoaded(csn);

    session_context->setCurrentTransaction(NO_TRANSACTION_PTR);
    return {};
}

BlockIO InterpreterTransactionControlQuery::executeRollback(ContextMutablePtr session_context)
{
    auto txn = session_context->getCurrentTransaction();
    if (!txn)
    {
        if (session_context->getClientInfo().interface == ClientInfo::Interface::MYSQL)
            return {};
        else
            throw Exception(ErrorCodes::INVALID_TRANSACTION, "There is no current transaction");
    }
    if (txn->getState() == MergeTreeTransaction::COMMITTED)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction is in COMMITTED state");
    if (txn->getState() == MergeTreeTransaction::COMMITTING)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction is in COMMITTING state");

    if (txn->getState() == MergeTreeTransaction::RUNNING)
        TransactionLog::instance().rollbackTransaction(txn);
    session_context->setCurrentTransaction(NO_TRANSACTION_PTR);
    return {};
}

BlockIO InterpreterTransactionControlQuery::executeSetSnapshot(ContextMutablePtr session_context, UInt64 snapshot)
{
    auto txn = session_context->getCurrentTransaction();
    if (!txn)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "There is no current transaction");

    if (snapshot <= Tx::MaxReservedCSN && snapshot != Tx::PrehistoricCSN && snapshot != Tx::EverythingVisibleCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Cannot set snapshot to reserved CSN");

    txn->setSnapshot(snapshot);
    return {};
}

void registerInterpreterTransactionControlQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterTransactionControlQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterTransactionControlQuery", create_fn);
}

}
