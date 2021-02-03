#include <Parsers/ParserShowPrivilegesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTShowPrivilegesQuery.h>


namespace DB
{

bool ParserShowPrivilegesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTShowPrivilegesQuery>();

    if (!ParserKeyword("SHOW PRIVILEGES").ignore(pos, expected))
        return false;

    node = query;

    return true;
}

}
