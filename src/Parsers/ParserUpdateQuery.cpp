#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserPartition.h>

namespace DB
{

bool ParserUpdateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTUpdateQuery>();
    node = query;

    ParserKeyword s_update(Keyword::UPDATE);
    ParserKeyword s_set(Keyword::SET);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_on{Keyword::ON};
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_in_partition(Keyword::IN_PARTITION);

    ParserExpression parser_exp_elem;
    ParserPartition parser_partition;

    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(),
        std::make_unique<ParserToken>(TokenType::Comma));

    if (!s_update.ignore(pos, expected))
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

    if (!s_set.ignore(pos, expected))
        return false;

    if (!parser_assignment_list.parse(pos, query->assignments, expected))
        return false;

    if (s_in_partition.ignore(pos, expected))
    {
        ParserList partition_list_parser(
            std::make_unique<ParserPartition>(), std::make_unique<ParserToken>(TokenType::Comma), false);
        ASTPtr partition_list_ast;
        if (!partition_list_parser.parse(pos, partition_list_ast, expected))
            return false;

        auto & partition_list = partition_list_ast->as<ASTExpressionList &>();
        if (partition_list.children.size() == 1)
            query->partition = std::move(partition_list.children[0]);
        else
            query->partitions = std::move(partition_list_ast);
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

    auto add_to_children = [&](const auto & ast)
    {
        if (ast)
            query->children.push_back(ast);
    };

    add_to_children(query->database);
    add_to_children(query->table);
    add_to_children(query->partition);
    add_to_children(query->partitions);
    add_to_children(query->predicate);
    add_to_children(query->assignments);
    add_to_children(query->settings_ast);

    return true;
}

}
