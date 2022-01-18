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
    auto & query = query_ptr->as<ASTShowAccessEntitiesQuery &>();
    query.replaceEmptyDatabaseWithCurrent(context.getCurrentDatabase());
    String origin;
    String expr = "*";
    String filter, order;

    switch (query.type)
    {
        case EntityType::ROW_POLICY:
        {
            origin = "row_policies";
            expr = "name";
            if (!query.short_name.empty())
                filter += String{filter.empty() ? "" : " AND "} + "short_name = " + quoteString(query.short_name);
            if (query.database_and_table_name)
            {
                const String & database = query.database_and_table_name->first;
                const String & table_name = query.database_and_table_name->second;
                if (!database.empty())
                    filter += String{filter.empty() ? "" : " AND "} + "database = " + quoteString(database);
                if (!table_name.empty())
                    filter += String{filter.empty() ? "" : " AND "} + "table = " + quoteString(table_name);
                if (!database.empty() && !table_name.empty())
                    expr = "short_name";
            }
            break;
        }

        case EntityType::QUOTA:
        {
            if (query.current_quota)
            {
                origin = "quota_usage";
                order = "duration";
            }
            else
            {
                origin = "quotas";
                expr = "name";
            }
            break;
        }

        case EntityType::SETTINGS_PROFILE:
        {
            origin = "settings_profiles";
            expr = "name";
            break;
        }

        case EntityType::USER:
        {
            origin = "users";
            expr = "name";
            break;
        }

        case EntityType::ROLE:
        {
            if (query.current_roles)
            {
                origin = "current_roles";
                order = "role_name";
            }
            else if (query.enabled_roles)
            {
                origin = "enabled_roles";
                order = "role_name";
            }
            else
            {
                origin = "roles";
                expr = "name";
            }
            break;
        }

        case EntityType::MAX:
            break;
    }

    if (origin.empty())
        throw Exception(toString(query.type) + ": type is not supported by SHOW query", ErrorCodes::NOT_IMPLEMENTED);

    if (order.empty() && expr != "*")
        order = expr;

    return "SELECT " + expr + " from system." + origin +
            (filter.empty() ? "" : " WHERE " + filter) +
            (order.empty() ? "" : " ORDER BY " + order);
}

}
