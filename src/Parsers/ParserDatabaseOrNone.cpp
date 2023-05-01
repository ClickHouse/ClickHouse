#include <Parsers/ParserDatabaseOrNone.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Parsers/CommonParsers.h>

namespace DB
{
bool ParserDatabaseOrNone::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto result = std::make_shared<ASTDatabaseOrNone>();
    node = result;

    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
        result->none = true;
        return true;
    }
    String database_name;
    if (parseIdentifierOrStringLiteral(pos, expected, database_name))
    {
        result->database_name = database_name;
        return true;
    }
    return false;


}

}
