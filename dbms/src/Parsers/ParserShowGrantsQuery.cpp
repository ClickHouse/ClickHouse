#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword show_p("SHOW");
    if (!show_p.ignore(pos, expected))
        return false;

    ParserKeyword grants_p("GRANTS");
    if (!grants_p.ignore(pos, expected))
        return false;

    ParserKeyword for_p("FOR");
    if (!for_p.ignore(pos, expected))
        return false;

    ParserIdentifier role_p;
    ASTPtr role;
    if (!role_p.parse(pos, role, expected))
        return false;

    auto query = std::make_shared<ASTShowGrantsQuery>();
    node = query;
    query->role = getIdentifierName(role);
    return true;
}

}
