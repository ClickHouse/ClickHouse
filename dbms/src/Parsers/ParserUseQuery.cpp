#include <Parsers/ParserUseQuery.h>

#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTUseQuery.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use("USE");
    ParserIdentifier name_p;

    ASTPtr database;

    if (!s_use.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, database, expected))
        return false;

    auto query = std::make_shared<ASTUseQuery>();
    query->database = typeid_cast<ASTIdentifier &>(*database).name;
    node = query;

    return true;
}

}
