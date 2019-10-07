#include <Parsers/ParserShowCreateAccessQuery.h>
#include <Parsers/ASTShowCreateAccessQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseUserName.h>


namespace DB
{

bool ParserShowCreateAccessQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword show_create_p("SHOW CREATE");
    if (!show_create_p.ignore(pos, expected))
        return false;

    using Kind = ASTShowCreateAccessQuery::Kind;
    Kind kind;
    if (ParserKeyword{"USER"}.ignore(pos, expected))
        kind = Kind::USER;
    else if (ParserKeyword{"SETTINGS PROFILE"}.ignore(pos, expected))
        kind = Kind::SETTINGS_PROFILE;
    else
        return false;

    String name;
    if (kind == Kind::USER)
    {
        if (!parseUserName(pos, expected, name))
            return false;
    }
    else
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
            return false;
    }

    auto query = std::make_shared<ASTShowCreateAccessQuery>();
    node = query;
    query->kind = kind;
    query->name = std::move(name);
    return true;
}

}
