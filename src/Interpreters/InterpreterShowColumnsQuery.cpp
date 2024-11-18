#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowColumnsQuery.h>

#include <Common/quoteString.h>
#include <Common/escapeString.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool mysql_map_fixed_string_to_text_in_show_columns;
    extern const SettingsBool mysql_map_string_to_text_in_show_columns;
}


InterpreterShowColumnsQuery::InterpreterShowColumnsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}


String InterpreterShowColumnsQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowColumnsQuery &>();

    ClientInfo::Interface client_interface = getContext()->getClientInfo().interface;
    const bool use_mysql_types = (client_interface == ClientInfo::Interface::MYSQL); // connection made through MySQL wire protocol

    const auto & settings = getContext()->getSettingsRef();
    const bool remap_string_as_text = settings[Setting::mysql_map_string_to_text_in_show_columns];
    const bool remap_fixed_string_as_text = settings[Setting::mysql_map_fixed_string_to_text_in_show_columns];

    WriteBufferFromOwnString buf_database;
    String resolved_database = getContext()->resolveDatabase(query.database);
    String database = escapeString(resolved_database);
    String table = escapeString(query.table);

    String rewritten_query;
    if (use_mysql_types)
    {
        /// Cheapskate SQL-based mapping from native types to MySQL types, see https://dev.mysql.com/doc/refman/8.0/en/data-types.html
        /// Known issues:
        /// - Enums are translated to TEXT
        rewritten_query += fmt::format(
            R"(
WITH map(
        'Int8',        'TINYINT',
        'Int16',       'SMALLINT',
        'Int32',       'INTEGER',
        'Int64',       'BIGINT',
        'UInt8',       'TINYINT UNSIGNED',
        'UInt16',      'SMALLINT UNSIGNED',
        'UInt32',      'INTEGER UNSIGNED',
        'UInt64',      'BIGINT UNSIGNED',
        'Float32',     'FLOAT',
        'Float64',     'DOUBLE',
        'UUID',        'CHAR',
        'Bool',        'TINYINT',
        'Date',        'DATE',
        'Date32',      'DATE',
        'DateTime',    'DATETIME',
        'DateTime64',  'DATETIME',
        'Map',         'JSON',
        'Tuple',       'JSON',
        'Object',      'JSON',
        'JSON',        'JSON',
        'String',      '{}',
        'FixedString', '{}') AS native_to_mysql_mapping,
        )",
        remap_string_as_text ? "TEXT" : "BLOB",
        remap_fixed_string_as_text ? "TEXT" : "BLOB");

        rewritten_query += R"(
        splitByRegexp('\(|\)', type_) AS split,
        multiIf(startsWith(type_, 'LowCardinality(Nullable'), split[3],
                startsWith(type_, 'LowCardinality'), split[2],
                startsWith(type_, 'Nullable'), split[2],
                split[1]) AS inner_type,
        if (length(split) > 1, splitByString(', ', split[2]), []) AS decimal_scale_and_precision,
        multiIf(inner_type = 'Decimal' AND toInt8(decimal_scale_and_precision[1]) <= 65 AND toInt8(decimal_scale_and_precision[2]) <= 30, concat('DECIMAL(', decimal_scale_and_precision[1], ', ', decimal_scale_and_precision[2], ')'),
                mapContains(native_to_mysql_mapping, inner_type) = true, native_to_mysql_mapping[inner_type],
                'TEXT') AS mysql_type
            )";
    }

    rewritten_query += R"(
SELECT
    name_ AS field,
    )";

    if (use_mysql_types)
        rewritten_query += R"(
    mysql_type AS type,
        )";
    else
        rewritten_query += R"(
    type_ AS type,
        )";

    rewritten_query += R"(
    multiIf(startsWith(type_, 'Nullable('), 'YES', startsWith(type_, 'LowCardinality(Nullable('), 'YES', 'NO') AS `null`,
    trim(concatWithSeparator(' ', if (is_in_primary_key_, 'PRI', ''), if (is_in_sorting_key_, 'SOR', ''))) AS key,
    if (default_kind_ IN ('ALIAS', 'DEFAULT', 'MATERIALIZED'), default_expression_, NULL) AS default,
    '' AS extra )";

    // TODO Interpret query.extended. It is supposed to show internal/virtual columns. Need to fetch virtual column names, see
    // IStorage::getVirtualsList(). We can't easily do that via SQL.

    if (query.full)
    {
        /// "Full" mode is mostly for MySQL compat
        /// - collation: no such thing in ClickHouse
        /// - comment
        /// - privileges: <not implemented, TODO ask system.grants>
        rewritten_query += R"(,
    NULL AS collation,
    comment_ AS comment,
    '' AS privileges )";
    }

    rewritten_query += fmt::format(R"(
-- need to rename columns of the base table to avoid "CYCLIC_ALIASES" errors
FROM (SELECT name AS name_,
             database AS database_,
             table AS table_,
             type AS type_,
             is_in_primary_key AS is_in_primary_key_,
             is_in_sorting_key AS is_in_sorting_key_,
             default_kind AS default_kind_,
             default_expression AS default_expression_,
             comment AS comment_
      FROM system.columns)
WHERE
    database_ = '{}'
    AND table_ = '{}' )", database, table);

    if (!query.like.empty())
    {
        rewritten_query += " AND field ";
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

    rewritten_query += " ORDER BY field, type, null, key, default, extra";

    if (query.limit_length)
        rewritten_query += fmt::format(" LIMIT {}", query.limit_length);

    return rewritten_query;
}


BlockIO InterpreterShowColumnsQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), QueryFlags{ .internal = true }).second;
}

void registerInterpreterShowColumnsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowColumnsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowColumnsQuery", create_fn);
}

}
