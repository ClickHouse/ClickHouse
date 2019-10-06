#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{

bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword show_grants_for_p("SHOW GRANTS FOR");
    if (!show_grants_for_p.ignore(pos, expected))
        return false;

    String role_name;
    if (!parseRoleName(pos, expected, role_name))
        return false;

    auto query = std::make_shared<ASTShowGrantsQuery>();
    node = query;
    query->role_name = std::move(role_name);
    return true;
}

}
