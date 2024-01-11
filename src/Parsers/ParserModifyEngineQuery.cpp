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

    ParserKeyword s_modify("MODIFY");
    ParserStorage storage_p(ParserStorage::EngineKind::TABLE_ENGINE);

    if (s_modify.ignore(pos, expected))
    {
        if (!storage_p.parse(pos, query->storage, expected))
            return false;
    }
    else
        return false;

    if (query->storage)
        query->children.push_back(query->storage);

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}
