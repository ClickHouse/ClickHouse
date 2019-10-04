#include <Parsers/ParserShowCreateUserQuery.h>
#include <Parsers/ASTShowCreateUserQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{

bool ParserShowCreateUserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword show_create_user_p("SHOW CREATE USER");
    if (!show_create_user_p.ignore(pos, expected))
        return false;

    String user_name;
    if (!parseUserName(pos, expected, user_name))
        return false;

    auto query = std::make_shared<ASTShowCreateUserQuery>();
    node = query;
    query->user_name = std::move(user_name);
    return true;
}

}
