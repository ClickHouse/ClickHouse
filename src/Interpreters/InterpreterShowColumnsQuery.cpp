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

    [[maybe_unused]] const bool use_mysql_types = getContext()->getSettingsRef().use_mysql_types_in_show_columns;

    WriteBufferFromOwnString buf_database;
    String resolved_database = getContext()->resolveDatabase(query.database);
    writeEscapedString(resolved_database, buf_database);
    String database = buf_database.str();

    WriteBufferFromOwnString buf_table;
    writeEscapedString(query.table, buf_table);
    String table = buf_table.str();

    String rewritten_query;
    if (use_mysql_types)
        /// Cheapskate mapping from native to MySQL types, see https://dev.mysql.com/doc/refman/8.0/en/data-types.html
        /// Known issues:
        /// - Enums are translated to TEXT
        rewritten_query += R"(
WITH map(
        'Int8',       'TINYINT',
        'Int16',      'SMALLINT',
        'Int32',      'INTEGER',
        'Int64',      'BIGINT',
        'UInt8',      'TINYINT UNSIGNED',
        'UInt16',     'SMALLINT UNSIGNED',
        'UInt32',     'INTEGER UNSIGNED',
        'UInt64',     'BIGINT UNSIGNED',
        'Float32',    'FLOAT',
        'Float64',    'DOUBLE',
        'String',     'BLOB',
        'UUID',       'CHAR',
        'Bool',       'TINYINT',
        'Date',       'DATE',
        'Date32',     'DATE',
        'DateTime',   'DATETIME',
        'DateTime64', 'DATETIME',
        'Map',        'JSON',
        'Tuple',      'JSON',
        'Object',     'JSON') AS native_to_mysql_mapping,
    splitByRegexp('\(|\)', type) AS split,
    multiIf(startsWith(type, 'LowCardinality(Nullable'), split[3],
             startsWith(type, 'LowCardinality'), split[2],
             startsWith(type, 'Nullable'), split[2],
             split[1]) AS inner_type,
     if(length(split) > 1, splitByString(', ', split[2]), []) AS decimal_scale_and_precision,
     multiIf(inner_type = 'Decimal' AND toInt8(decimal_scale_and_precision[1]) <= 65 AND toInt8(decimal_scale_and_precision[2]) <= 30, concat('DECIMAL(', decimal_scale_and_precision[1], ', ', decimal_scale_and_precision[2], ')'),
             mapContains(native_to_mysql_mapping, inner_type) = true, native_to_mysql_mapping[inner_type],
             'TEXT') AS mysql_type
        )";

    rewritten_query += R"(
SELECT
    name AS field,
    )";

    if (use_mysql_types)
        rewritten_query += R"(
    mysql_type AS type,
        )";
    else
        rewritten_query += R"(
    type AS type,
        )";

    rewritten_query += R"(
    multiIf(startsWith(type, 'Nullable('), 'YES', startsWith(type, 'LowCardinality(Nullable('), 'YES', 'NO') AS `null`,
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
