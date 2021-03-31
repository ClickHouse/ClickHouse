#include <Interpreters/InterpreterTransactionControlQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_TRANSACTION;
}

BlockIO InterpreterTransactionControlQuery::execute()
{
    if (!query_context.hasSessionContext())
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction Control Language queries are allowed only inside session");

    Context & session_context = query_context.getSessionContext();
    const auto & tcl = query_ptr->as<const ASTTransactionControl &>();

    switch (tcl.action)
    {
        case ASTTransactionControl::BEGIN:
            return executeBegin(session_context);
        case ASTTransactionControl::COMMIT:
            return executeCommit(session_context);
        case ASTTransactionControl::ROLLBACK:
            return executeRollback(session_context);
    }
}

BlockIO InterpreterTransactionControlQuery::executeBegin(Context & context)
{
    if (context.getCurrentTransaction())
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Nested transactions are not supported");

    auto txn = TransactionLog::instance().beginTransaction();
    context.setCurrentTransaction(txn);
    query_context.setCurrentTransaction(txn);
    return {};
}

BlockIO InterpreterTransactionControlQuery::executeCommit(Context & context)
{
    auto txn = context.getCurrentTransaction();
    if (!txn)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "There is no current transaction");
    if (txn->getState() != MergeTreeTransaction::RUNNING)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction is not in RUNNING state");

    TransactionLog::instance().commitTransaction(txn);
    context.setCurrentTransaction(nullptr);
    return {};
}

BlockIO InterpreterTransactionControlQuery::executeRollback(Context & context)
{
    auto txn = context.getCurrentTransaction();
    if (!txn)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "There is no current transaction");
    if (txn->getState() == MergeTreeTransaction::COMMITTED)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction is in COMMITTED state");

    if (txn->getState() == MergeTreeTransaction::RUNNING)
        TransactionLog::instance().rollbackTransaction(txn);
    context.getSessionContext().setCurrentTransaction(nullptr);
    return {};
}

}
