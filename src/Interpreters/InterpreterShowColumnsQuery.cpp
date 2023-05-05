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

    // If connected via MySQL Compatibility mode, convert ClickHouse types to MySQL
    if (getContext()->getClientInfo().interface == DB::ClientInfo::Interface::MYSQL)
    {
        rewritten_query += getMySQLQuery();
    }
    else {
        rewritten_query += "SELECT name AS field, type AS type, startsWith(type, 'Nullable') AS null, trim(concatWithSeparator(' ', if(is_in_primary_key, 'PRI', ''), if (is_in_sorting_key, 'SOR', ''))) AS key, if(default_kind IN ('ALIAS', 'DEFAULT', 'MATERIALIZED'), default_expression, NULL) AS default, '' AS extra ";
    }
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

String InterpreterShowColumnsQuery::getMySQLQuery()
{
    String mysql_specific_query;

    mysql_specific_query = R"(SELECT name AS field,
        CASE
          WHEN startsWith(type, 'Nullable') THEN
            CASE
              WHEN substring(type, 10, length(type) - 10) IN ('UInt8', 'Int8') THEN 'tinyint'
              WHEN substring(type, 10, length(type) - 10) IN ('UInt16', 'Int16') THEN 'smallint'
              WHEN substring(type, 10, length(type) - 10) IN ('UInt32', 'Int32') THEN 'int'
              WHEN substring(type, 10, length(type) - 10) IN ('UInt64', 'Int64', 'UInt128', 'Int128', 'UInt256', 'Int256') THEN 'bigint'
              WHEN substring(type, 10, length(type) - 10) = 'Float32' THEN 'float'
              WHEN substring(type, 10, length(type) - 10) = 'Float64' THEN 'double'
              WHEN substring(type, 10, length(type) - 10) LIKE 'Decimal%' THEN 'decimal'
              WHEN substring(type, 10, length(type) - 10) = 'Boolean' THEN 'tinyint'
              WHEN substring(type, 10, length(type) - 10) = 'String' THEN 'text'
              WHEN substring(type, 10, length(type) - 10) LIKE 'FixedString%' THEN 'text'
              WHEN substring(type, 10, length(type) - 10) LIKE 'Date%' THEN 'date'
              WHEN substring(type, 10, length(type) - 10) LIKE 'DateTime%' THEN 'datetime'
              WHEN substring(type, 10, length(type) - 10) = 'JSON' THEN 'json'
              WHEN substring(type, 10, length(type) - 10) = 'UUID' THEN 'binary'
              WHEN substring(type, 10, length(type) - 10) LIKE 'Enum%' THEN 'enum'
              WHEN substring(type, 10, length(type) - 10) LIKE 'LowCardinality%' THEN 'text'
              WHEN substring(type, 10, length(type) - 10) LIKE 'Array%' THEN 'json'
              WHEN substring(type, 10, length(type) - 10) LIKE 'Map%' THEN 'json'
              WHEN substring(type, 10, length(type) - 10) IN ('SimpleAggregateFunction', 'AggregateFunction') THEN 'text'
              WHEN substring(type, 10, length(type) - 10) = 'Nested' THEN 'json'
              WHEN substring(type, 10, length(type) - 10) LIKE 'Tuple%' THEN 'json'
              WHEN substring(type, 10, length(type) - 10) LIKE 'IPv%' THEN 'text'
              WHEN substring(type, 10, length(type) - 10) IN ('Expression', 'Set', 'Nothing', 'Interval') THEN 'text'
              ELSE substring(type, 10, length(type) - 10)
            END
          ELSE
            CASE
              WHEN type IN ('UInt8', 'Int8') THEN 'tinyint'
              WHEN type IN ('UInt16', 'Int16') THEN 'smallint'
              WHEN type IN ('UInt32', 'Int32') THEN 'int'
              WHEN type IN ('UInt64', 'Int64', 'UInt128', 'Int128', 'UInt256', 'Int256') THEN 'bigint'
              WHEN type = 'Float32' THEN 'float'
              WHEN type = 'Float64' THEN 'double'
              WHEN type LIKE 'Decimal%' THEN 'decimal'
              WHEN type = 'Boolean' THEN 'tinyint'
              WHEN type = 'String' THEN 'text'
              WHEN type LIKE 'FixedString%' THEN 'text'
              WHEN type LIKE 'Date%' THEN 'date'
              WHEN type LIKE 'DateTime%' THEN 'datetime'
              WHEN type = 'JSON' THEN 'json'
              WHEN type = 'UUID' THEN 'binary'
              WHEN type LIKE 'Enum%' THEN 'enum'
              WHEN type LIKE 'LowCardinality%' THEN 'text'
              WHEN type LIKE 'Array%' THEN 'json'
              WHEN type LIKE 'Map%' THEN 'json'
              WHEN type IN ('SimpleAggregateFunction', 'AggregateFunction') THEN 'text'
              WHEN type = 'Nested' THEN 'json'
              WHEN type LIKE 'Tuple%' THEN 'json'
              WHEN type LIKE 'IPv%' THEN 'text'
              WHEN type IN ('Expression', 'Set', 'Nothing', 'Interval') THEN 'text'
              ELSE type
            END
        END AS type,
        startsWith(type, 'Nullable') AS null,
        trim(concatWithSeparator(' ', if(is_in_primary_key, 'PRI', ''), if (is_in_sorting_key, 'SOR', ''))) AS key,
        if(default_kind IN ('ALIAS', 'DEFAULT', 'MATERIALIZED'), default_expression, NULL) AS default,
        '' AS extra )";

    return mysql_specific_query.str();
}

BlockIO InterpreterShowColumnsQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}
