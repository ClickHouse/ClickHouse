#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Access/Quota.h>


namespace DB
{
bool ParserDropAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    using Kind = ASTDropAccessEntityQuery::Kind;
    Kind kind;
    if (ParserKeyword{"QUOTA"}.ignore(pos, expected))
        kind = Kind::QUOTA;
    else
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    do
    {
        String name;
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
            return false;

        names.push_back(std::move(name));
    }
    while (ParserToken{TokenType::Comma}.ignore(pos, expected));

    auto query = std::make_shared<ASTDropAccessEntityQuery>(kind);
    node = query;

    query->if_exists = if_exists;
    query->names = std::move(names);

    return true;
}
}
