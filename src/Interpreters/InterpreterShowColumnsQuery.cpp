#include <Interpreters/InterpreterShowColumnsQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/parseDatabaseAndTableName.h>
#include <IO/Operators.h>


namespace DB
{


InterpreterShowColumnsQuery::InterpreterShowColumnsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}


String InterpreterShowColumnsQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowColumnsQuery &>();

    WriteBufferFromOwnString rewritten_query;

    rewritten_query << "SELECT name AS field, type AS type, startsWith(type, 'Nullable') AS null, trim(concatWithSeparator(' ', if(is_in_primary_key, 'PRI', ''), if (is_in_sorting_key, 'SOR', ''))) AS key, if(default_kind IN ('ALIAS', 'DEFAULT', 'MATERIALIZED'), default_expression, NULL) AS default, '' AS extra ";

    // TODO Interpret query.extended. It is supposed to show internal/virtual columns. Need to fetch virtual column names, see
    // IStorage::getVirtuals(). We can't easily do that via SQL.

    if (query.full)
    {
        /// "Full" mode is mostly for MySQL compat
        /// - collation: no such thing in ClickHouse
        /// - comment
        /// - privileges: <not implemented, TODO ask system.grants>
        rewritten_query << ", NULL AS collation, comment, '' AS privileges ";
    }

    rewritten_query << "FROM system.columns WHERE ";

    auto [database, table] = parseDatabaseAndTableName(query, getContext()->getCurrentDatabase());

    rewritten_query << "database = " << DB::quote << database;
    rewritten_query << " AND table = " << DB::quote << table;

    if (!query.like.empty())
        rewritten_query
            << " AND name "
            << (query.not_like ? "NOT " : "")
            << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
            << DB::quote << query.like;
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression << ")";

    /// Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
    /// sort the output of SHOW COLUMNS otherwise (SELECT * FROM (SHOW COLUMNS ...) ORDER BY ...) is rejected) and 3. some
    /// SQL tests can take advantage of this.
    rewritten_query << " ORDER BY field, type, null, key, default, extra";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

    return rewritten_query.str();

}


BlockIO InterpreterShowColumnsQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}
