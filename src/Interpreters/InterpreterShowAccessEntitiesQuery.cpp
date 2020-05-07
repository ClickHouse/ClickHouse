#include <Interpreters/InterpreterShowAccessEntitiesQuery.h>
#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/executeQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using EntityType = IAccessEntity::Type;


InterpreterShowAccessEntitiesQuery::InterpreterShowAccessEntitiesQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterShowAccessEntitiesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}


String InterpreterShowAccessEntitiesQuery::getRewrittenQuery() const
{
    const auto & query = query_ptr->as<ASTShowAccessEntitiesQuery &>();
    String origin;
    String expr = "name";
    String filter;

    if (query.type == EntityType::ROW_POLICY)
    {
        origin = "row_policies";

        const String & table_name = query.table_name;
        String database;
        bool show_short_name = false;
        if (!table_name.empty())
        {
            database = query.database;
            if (database.empty())
                database = context.getCurrentDatabase();
            show_short_name = true;
        }

        if (!table_name.empty())
            filter = "database = " + quoteString(database) + " AND table = " + quoteString(table_name);

        if (show_short_name)
            expr = "short_name";
    }
    else
        throw Exception(toString(query.type) + ": type is not supported by SHOW query", ErrorCodes::NOT_IMPLEMENTED);

    return "SELECT " + expr + " from system." + origin +
            (filter.empty() ? "" : " WHERE " + filter) + " ORDER BY " + expr;
}

}
