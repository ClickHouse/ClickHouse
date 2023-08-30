#include <Interpreters/IInterpreter.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void IInterpreter::extendQueryLogElem(
    QueryLogElement & elem, const ASTPtr & ast, ContextPtr context, const String & query_database, const String & query_table) const
{
    if (!query_database.empty() && query_table.empty())
    {
        elem.query_databases.insert(backQuoteIfNeed(query_database));
    }
    else if (!query_table.empty())
    {
        auto quoted_database = query_database.empty() ? backQuoteIfNeed(context->getCurrentDatabase())
                                                      : backQuoteIfNeed(query_database);
        elem.query_databases.insert(quoted_database);
        elem.query_tables.insert(quoted_database + "." + backQuoteIfNeed(query_table));
    }

    extendQueryLogElemImpl(elem, ast, context);
}

void IInterpreter::checkStorageSupportsTransactionsIfNeeded(const StoragePtr & storage, ContextPtr context)
{
    if (!context->getCurrentTransaction())
        return;

    if (storage->supportsTransactions())
        return;

    if (context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Storage {} (table {}) does not support transactions",
                        storage->getName(), storage->getStorageID().getNameForLogs());
}

}
