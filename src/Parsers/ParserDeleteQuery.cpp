#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserPartition.h>


namespace DB
{

bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTDeleteQuery>();
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
