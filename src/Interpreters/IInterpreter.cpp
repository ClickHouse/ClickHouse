#include <Interpreters/IInterpreter.h>
#include <Interpreters/QueryLog.h>

namespace DB
{
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
}
