#include <Common/typeid_cast.h>
#include <Parsers/ParserModifyEngineQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserModifyEngineQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTModifyEngineQuery>();
    node = query;

    ParserKeyword s_alter_table("ALTER TABLE");

    if (!s_alter_table.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    query->cluster = cluster_str;

    ParserKeyword modify("MODIFY");
    ParserKeyword engine("ENGINE");
    ParserKeyword to("TO");
    ParserKeyword not_keyword("NOT");
    ParserKeyword replicated("REPLICATED");

    if (!modify.ignore(pos, expected))
        return false;
    if (!engine.ignore(pos, expected))
        return false;
    if (!to.ignore(pos, expected))
        return false;
    if (not_keyword.ignore(pos, expected))
        query->to_replicated = false;
    if (!replicated.ignore(pos, expected))
        return false;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}
