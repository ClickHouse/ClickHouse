#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{
bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW GRANTS"}.ignore(pos, expected))
        return false;

    String name;
    bool current_user = false;

    if (ParserKeyword{"FOR"}.ignore(pos, expected))
    {
        if (!parseUserNameOrCurrentUserTag(pos, expected, name, current_user))
            return false;
    }
    else
        current_user = true;

    auto query = std::make_shared<ASTShowGrantsQuery>();
    node = query;

    query->name = name;
    query->current_user = current_user;

    return true;
}
}
