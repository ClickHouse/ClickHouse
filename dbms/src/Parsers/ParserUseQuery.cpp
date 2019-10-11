#include <Parsers/ParserUseQuery.h>
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

    if (!s_use.ignore(pos, expected))
        return false;

    ASTPtr database;
    if (!name_p.parse(pos, database, expected))
        return false;

    auto query = std::make_shared<ASTUseQuery>();
    tryGetIdentifierNameInto(database, query->database);
    node = query;

    return true;
}

}
