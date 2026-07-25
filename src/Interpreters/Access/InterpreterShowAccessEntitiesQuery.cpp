#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


InterpreterShowAccessEntitiesQuery::InterpreterShowAccessEntitiesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterShowAccessEntitiesQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId({});

    return executeQuery(getRewrittenQuery(), query_context, QueryFlags{ .internal = true }).second;
}


String InterpreterShowAccessEntitiesQuery::getRewrittenQuery() const
{
    auto & query = query_ptr->as<ASTShowAccessEntitiesQuery &>();

    /// the scope prefix does not apply to access-entity targets
    if (const auto database_info = getContext()->getCurrentDatabaseInfo();
        !database_info.table_prefix.empty() && query.database_and_table_name
        && query.database_and_table_name->first.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "An unqualified policy target is not supported while a table namespace is selected "
            "(USE {}.{}); qualify the table with its database explicitly",
            backQuoteIfNeed(database_info.database), backQuoteIfNeed(database_info.table_prefix));

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    String origin;
    String expr = "*";
    String filter;
    String order;

    switch (query.type)
    {
        case AccessEntityType::ROW_POLICY:
        {
            origin = "row_policies";
            expr = "name";

            if (!query.short_name.empty())
                filter = "short_name = " + quoteString(query.short_name);

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

        case AccessEntityType::QUOTA:
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

        case AccessEntityType::SETTINGS_PROFILE:
        {
            origin = "settings_profiles";
            expr = "name";
            break;
        }

        case AccessEntityType::USER:
        {
            origin = "users";
            expr = "name";
            break;
        }

        case AccessEntityType::ROLE:
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

        case AccessEntityType::MASKING_POLICY:
        {
            origin = "masking_policies";
            expr = "name";

            if (!query.short_name.empty())
                filter = "short_name = " + quoteString(query.short_name);

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

        case AccessEntityType::MAX:
            break;
    }

    if (origin.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: type is not supported by SHOW query", toString(query.type));

    if (order.empty() && expr != "*")
        order = expr;

    return "SELECT " + expr + " from system." + origin +
            (filter.empty() ? "" : " WHERE " + filter) +
            (order.empty() ? "" : " ORDER BY " + order);
}

void registerInterpreterShowAccessEntitiesQuery(InterpreterFactory & factory);
void registerInterpreterShowAccessEntitiesQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowAccessEntitiesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowAccessEntitiesQuery", create_fn, /*supports_table_namespace_scope*/ true);
}

}
