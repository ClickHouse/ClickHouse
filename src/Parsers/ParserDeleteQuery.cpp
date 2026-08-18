#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/StatementFactory.h>


namespace DB
{

bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTDeleteQuery>();
    node = query;

    ParserKeyword s_delete(Keyword::DELETE);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_in_partition(Keyword::IN_PARTITION);
    ParserKeyword s_where(Keyword::WHERE);
    ParserExpression parser_exp_elem;
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_on{Keyword::ON};

    ParserPartition parser_partition;

    if (s_delete.ignore(pos, expected))
    {
        if (!s_from.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        if (s_on.ignore(pos, expected))
        {
            String cluster_str;
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = cluster_str;
        }

        if (s_in_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, query->partition, expected))
                return false;
        }

        if (!s_where.ignore(pos, expected))
            return false;

        if (!parser_exp_elem.parse(pos, query->predicate, expected))
            return false;

        /// ParserExpression, in contrast to ParserExpressionWithOptionalAlias,
        /// does not expect an alias after the expression. However, in certain cases,
        /// it uses ParserExpressionWithOptionalAlias recursively, and use its result.
        /// This is the case when it parses a single expression in parentheses, e.g.,
        /// it does not allow
        /// 1 AS x
        /// but it can parse
        /// (1 AS x)
        /// which we should not allow as well.
        if (!query->predicate->tryGetAlias().empty())
            return false;

        if (s_settings.ignore(pos, expected))
        {
            ParserSetQuery parser_settings(true);

            if (!parser_settings.parse(pos, query->settings_ast, expected))
                return false;
        }
    }
    else
        return false;

    if (query->partition)
        query->children.push_back(query->partition);

    if (query->predicate)
        query->children.push_back(query->predicate);

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    if (query->settings_ast)
        query->children.push_back(query->settings_ast);

    return true;
}

}

namespace DB
{

REGISTER_STATEMENTS(Delete)
{
    factory.registerStatement("DELETE", "",
    {
        .description = R"(
Removes the rows matching the filter expression from a table. It is only available for tables of the `*MergeTree`
family.

It is called a lightweight `DELETE` to contrast it with `ALTER TABLE ... DELETE`: the rows are only marked as deleted
and are filtered out from the results, whereas the data parts are rewritten later by merges or by
`ALTER TABLE ... APPLY DELETED MASK`.
)",
        .syntax = R"(
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr
)",
        .examples = {{"Delete the rows matching a condition", "DELETE FROM hits WHERE Title LIKE '%hello%';", ""}},
        .related = {"ALTER TABLE ... DELETE", "ALTER TABLE ... APPLY DELETED MASK", "UPDATE", "TRUNCATE"},
    });
}

}
