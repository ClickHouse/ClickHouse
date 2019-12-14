#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{
bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW CREATE"}.ignore(pos, expected))
        return false;

    using Kind = ASTShowCreateAccessEntityQuery::Kind;
    Kind kind;
    if (ParserKeyword{"QUOTA"}.ignore(pos, expected))
        kind = Kind::QUOTA;
    else
        return false;

    String name;
    bool current_quota = false;

    if ((kind == Kind::QUOTA) && ParserKeyword{"CURRENT"}.ignore(pos, expected))
    {
        /// SHOW CREATE QUOTA CURRENT
        current_quota = true;
    }
    else if (parseIdentifierOrStringLiteral(pos, expected, name))
    {
        /// SHOW CREATE QUOTA name
    }
    else
    {
        /// SHOW CREATE QUOTA
        current_quota = true;
    }

    auto query = std::make_shared<ASTShowCreateAccessEntityQuery>(kind);
    node = query;

    query->name = std::move(name);
    query->current_quota = current_quota;

    return true;
}
}
