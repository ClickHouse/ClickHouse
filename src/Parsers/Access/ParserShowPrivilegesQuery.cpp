#include <Parsers/Access/ParserShowPrivilegesQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool ParserShowPrivilegesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTShowPrivilegesQuery>();

    if (!ParserKeyword(Keyword::SHOW_PRIVILEGES).ignore(pos, expected))
        return false;

    node = query;

    return true;
}

}
