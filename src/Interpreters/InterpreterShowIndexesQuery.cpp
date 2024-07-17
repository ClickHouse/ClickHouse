#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowIndexesQuery.h>

#include <Common/quoteString.h>
#include <Common/escapeString.h>
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
    String table = escapeString(query.table);
    String resolved_database = getContext()->resolveDatabase(query.database);
    String database = escapeString(resolved_database);
    String where_expression = query.where_expression ? fmt::format("WHERE ({})", query.where_expression) : "";

    String rewritten_query = fmt::format(R"(
SELECT *
FROM (
        (WITH
            t1 AS (
                SELECT
                    name,
                    arrayJoin(splitByString(', ', primary_key)) AS pk_col
                FROM
                    system.tables
                WHERE
                    database = '{0}'
                    AND name = '{1}'
            ),
            t2 AS (
                SELECT
                    name,
                    pk_col,
                    row_number() OVER (ORDER BY 1) AS row_num
                FROM
                    t1
            )
        SELECT
            name AS table,
            1 AS non_unique,
            'PRIMARY' AS key_name,
            -- row_number() over (order by database) AS seq_in_index,
            row_num AS seq_in_index,
            -- arrayJoin(splitByString(', ', primary_key)) AS column_name,
            pk_col,
            'A' AS collation,
            0 AS cardinality,
            NULL AS sub_part,
            NULL AS packed,
            NULL AS null,
            'PRIMARY' AS index_type,
            '' AS comment,
            '' AS index_comment,
            'YES' AS visible,
            '' AS expression
        FROM
            t2
        )
    UNION ALL (
        SELECT
            table AS table,
            1 AS non_unique,
            name AS key_name,
            1 AS seq_in_index,
            '' AS column_name,
            NULL AS collation,
            0 AS cardinality,
            NULL AS sub_part,
            NULL AS packed,
            NULL AS null,
            upper(type) AS index_type,
            '' AS comment,
            '' AS index_comment,
            'YES' AS visible,
            expr AS expression
        FROM
            system.data_skipping_indices
        WHERE
            database = '{0}'
            AND table = '{1}'))
{2}
ORDER BY index_type, expression, seq_in_index;)", database, table, where_expression);

    /// Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
    /// sort the output of SHOW INDEXES otherwise (SELECT * FROM (SHOW INDEXES ...) ORDER BY ...) is rejected) and 3. some
    /// SQL tests can take advantage of this.

    /// Note about compatibility of fields 'column_name', 'seq_in_index' and 'expression' with MySQL:
    /// MySQL has non-functional and functional indexes.
    /// - Non-functional indexes only reference columns, e.g. 'col1, col2'. In this case, `SHOW INDEX` produces as many result rows as there
    ///   are indexed columns. 'column_name' and 'seq_in_index' (an ascending integer 1, 2, ...) are filled, 'expression' is empty.
    /// - Functional indexes can reference arbitrary expressions, e.g. 'col1 + 1, concat(col2, col3)'. 'SHOW INDEX' produces a single row
    ///   with `column_name` and `seq_in_index` empty and `expression` filled with the entire index expression. Only non-primary-key indexes
    ///   can be functional indexes.
    /// Above SELECT tries to emulate that. Caveats:
    /// 1. The primary key index sub-SELECT assumes the primary key expression is non-functional. Non-functional primary key indexes in
    ///    ClickHouse are possible but quite obscure. In MySQL they are not possible at all.
    /// 2. Related to 1.: Poor man's tuple parsing with splitByString() in the PK sub-SELECT messes up for functional primary key index
    ///    expressions where the comma is not only used as separator between tuple components, e.g. in 'col1 + 1, concat(col2, col3)'.
    /// 3. The data skipping index sub-SELECT assumes the index expression is functional. 3rd party tools that expect MySQL semantics from
    ///    SHOW INDEX will probably not care as MySQL has no skipping indexes and they only use the result to figure out the primary key.

    return rewritten_query;
}


BlockIO InterpreterShowIndexesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), QueryFlags{ .internal = true }).second;
}

void registerInterpreterShowIndexesQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowIndexesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowIndexesQuery", create_fn);
}

}
