#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/QueryLog.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Common/quoteString.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
}

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

void IInterpreter::checkStorageSupportsTransactionsIfNeeded(const StoragePtr & storage, ContextPtr context, bool is_readonly_query)
{
    if (!context->getCurrentTransaction())
        return;

    if (storage->supportsTransactions())
        return;

    if (context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Storage {} (table {}) does not support transactions",
                        storage->getName(), storage->getStorageID().getNameForLogs());

    if (is_readonly_query)
        return;

    /// Do not allow transactions with replicated tables or MergeTree tables anyway (unless it's a readonly SELECT query)
    /// because it may try to process transaction on MergeTreeData-level,
    /// but then fail with a logical error or something on Storage{Replicated,Shared}MergeTree-level.
    if (storage->supportsReplication() || dynamic_cast<StorageMergeTree *>(storage.get()) != nullptr)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} (table {}) does not support transactions",
                        storage->getName(), storage->getStorageID().getNameForLogs());
}

}
