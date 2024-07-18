#include <Interpreters/InterpreterShowColumnsQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>


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

    WriteBufferFromOwnString buf_database;
    String resolved_database = getContext()->resolveDatabase(query.database);
    writeEscapedString(resolved_database, buf_database);
    String database = buf_database.str();

    WriteBufferFromOwnString buf_table;
    writeEscapedString(query.table, buf_table);
    String table = buf_table.str();

    String rewritten_query = R"(
SELECT
    name AS field,
    type AS type,
    startsWith(type, 'Nullable') AS null,
    trim(concatWithSeparator(' ', if (is_in_primary_key, 'PRI', ''), if (is_in_sorting_key, 'SOR', ''))) AS key,
    if (default_kind IN ('ALIAS', 'DEFAULT', 'MATERIALIZED'), default_expression, NULL) AS default,
    '' AS extra )";

    // TODO Interpret query.extended. It is supposed to show internal/virtual columns. Need to fetch virtual column names, see
    // IStorage::getVirtuals(). We can't easily do that via SQL.

    if (query.full)
    {
        /// "Full" mode is mostly for MySQL compat
        /// - collation: no such thing in ClickHouse
        /// - comment
        /// - privileges: <not implemented, TODO ask system.grants>
        rewritten_query += R"(,
    NULL AS collation,
    comment,
    '' AS privileges )";
    }

    rewritten_query += fmt::format(R"(
FROM system.columns
WHERE
    database = '{}'
    AND table = '{}' )", database, table);

    if (!query.like.empty())
    {
        rewritten_query += " AND name ";
        if (query.not_like)
            rewritten_query += "NOT ";
        if (query.case_insensitive_like)
            rewritten_query += "ILIKE ";
        else
            rewritten_query += "LIKE ";
        rewritten_query += fmt::format("'{}'", query.like);
    }
    else if (query.where_expression)
        rewritten_query += fmt::format(" AND ({})", query.where_expression);

    /// Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
    /// sort the output of SHOW COLUMNS otherwise (SELECT * FROM (SHOW COLUMNS ...) ORDER BY ...) is rejected) and 3. some
    /// SQL tests can take advantage of this.
    rewritten_query += " ORDER BY field, type, null, key, default, extra";

    if (query.limit_length)
        rewritten_query += fmt::format(" LIMIT {}", query.limit_length);

    return rewritten_query;
}


BlockIO InterpreterShowColumnsQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}
