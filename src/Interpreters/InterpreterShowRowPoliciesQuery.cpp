#include <Interpreters/InterpreterShowRowPoliciesQuery.h>
#include <Parsers/ASTShowRowPoliciesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/executeQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <ext/range.h>


namespace DB
{
InterpreterShowRowPoliciesQuery::InterpreterShowRowPoliciesQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterShowRowPoliciesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}


String InterpreterShowRowPoliciesQuery::getRewrittenQuery() const
{
    const auto & query = query_ptr->as<ASTShowRowPoliciesQuery &>();

    const String & table_name = query.table_name;
    String database;
    if (!table_name.empty())
    {
        database = query.database;
        if (database.empty())
            database = context.getCurrentDatabase();
    }

    String filter;
    if (query.current)
    {
        if (table_name.empty())
            filter = "has(currentRowPolicyIDs(), id)";
        else
            filter = "has(currentRowPolicyIDs(" + quoteString(database) + ", " + quoteString(table_name) + "), id)";
    }
    else
    {
        if (!table_name.empty())
            filter = "database = " + quoteString(database) + " AND table = " + quoteString(table_name);
    }

    String expr = table_name.empty() ? "name" : "short_name";

    return "SELECT " + expr + " AS " + backQuote(getResultDescription()) + " from system.row_policies"
            + (filter.empty() ? "" : " WHERE " + filter) + " ORDER BY " + expr;
}


String InterpreterShowRowPoliciesQuery::getResultDescription() const
{
    std::stringstream ss;
    formatAST(*query_ptr, ss, false, true);
    String desc = ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.
    return desc;
}
}
