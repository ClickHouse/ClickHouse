#include <Interpreters/InterpreterShowIndexesQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>


namespace DB
{


InterpreterShowIndexesQuery::InterpreterShowIndexesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}


String InterpreterShowIndexesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowIndexesQuery &>();

    WriteBufferFromOwnString buf_table;
    writeEscapedString(query.table, buf_table);
    String table = buf_table.str();

    WriteBufferFromOwnString buf_database;
    String resolved_database = getContext()->resolveDatabase(query.database);
    writeEscapedString(resolved_database, buf_database);
    String database = buf_database.str();

    String where_expression = query.where_expression ? fmt::format("WHERE ({})", query.where_expression) : "";

    String rewritten_query = fmt::format(R"(
SELECT *
FROM (
        (SELECT
            name AS table,
            0 AS non_unique,
            'PRIMARY' AS key_name,
            NULL AS seq_in_index,
            NULL AS column_name,
            'A' AS collation,
            NULL AS cardinality,
            NULL AS sub_part,
            NULL AS packed,
            NULL AS null,
            'primary' AS index_type,
            NULL AS comment,
            NULL AS index_comment,
            'YES' AS visible,
            primary_key AS expression
        FROM system.tables
        WHERE
            database = '{0}'
            AND name = '{1}')
    UNION ALL (
        SELECT
            table AS table,
            0 AS non_unique,
            name AS key_name,
            NULL AS seq_in_index,
            NULL AS column_name,
            NULL AS collation,
            NULL AS cardinality,
            NULL AS sub_part,
            NULL AS packed,
            NULL AS null,
            type AS index_type,
            NULL AS comment,
            NULL AS index_comment,
            'YES' AS visible,
            expr AS expression
        FROM system.data_skipping_indices
        WHERE
            database = '{0}'
            AND table = '{1}'))
{2}
ORDER BY index_type, expression;)", database, table, where_expression);

    /// Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
    /// sort the output of SHOW INDEXES otherwise (SELECT * FROM (SHOW INDEXES ...) ORDER BY ...) is rejected) and 3. some
    /// SQL tests can take advantage of this.

    return rewritten_query;
}


BlockIO InterpreterShowIndexesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}

