#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserPartition.h>

namespace DB
{

bool ParserUpdateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTUpdateQuery>();
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
        if (!parser_partition.parse(pos, query->partition, expected))
            return false;
    }

    if (!s_where.ignore(pos, expected))
        return false;

    if (!parser_exp_elem.parse(pos, query->predicate, expected))
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
    add_to_children(query->predicate);
    add_to_children(query->assignments);
    add_to_children(query->settings_ast);

    return true;
}

}
