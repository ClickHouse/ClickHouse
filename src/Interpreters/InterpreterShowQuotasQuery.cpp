#include <Interpreters/InterpreterShowQuotasQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowQuotasQuery.h>
#include <Parsers/formatAST.h>
#include <Access/Quota.h>
#include <Common/quoteString.h>
#include <Common/StringUtils/StringUtils.h>
#include <ext/range.h>


namespace DB
{
InterpreterShowQuotasQuery::InterpreterShowQuotasQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


String InterpreterShowQuotasQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowQuotasQuery &>();

    /// Transform the query into some kind of "SELECT from system.quotas" query.
    String expr;
    String filter;
    String table_name;
    String order_by;
    if (query.usage)
    {
        expr = "name || ' key=\\'' || key || '\\'' || if(isNull(end_of_interval), '', ' interval=[' || "
               "toString(end_of_interval - duration) || ' .. ' || "
               "toString(end_of_interval) || ']'";
        for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
        {
            String column_name = Quota::resourceTypeToColumnName(resource_type);
            expr += String{" || ' "} + column_name + "=' || toString(" + column_name + ")";
            expr += String{" || if(max_"} + column_name + "=0, '', '/' || toString(max_" + column_name + "))";
        }
        expr += ")";

        if (query.current)
            filter = "(id = currentQuotaID()) AND (key = currentQuotaKey())";

        table_name = "system.quota_usage";
        order_by = "name, key, duration";
    }
    else
    {
        expr = "name";
        table_name = "system.quotas";
        order_by = "name";
    }

    /// Prepare description of the result column.
    std::stringstream ss;
    formatAST(query, ss, false, true);
    String desc = ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    /// Build a new query.
    return "SELECT " + expr + " AS " + backQuote(desc) + " FROM " + table_name + (filter.empty() ? "" : (" WHERE " + filter))
        + (order_by.empty() ? "" : (" ORDER BY " + order_by));
}


BlockIO InterpreterShowQuotasQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}

}
