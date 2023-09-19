#include <Parsers/ParserShowFunctionsQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserShowFunctionsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr like;

    auto query = std::make_shared<ASTShowFunctionsQuery>();
    if (!ParserKeyword("SHOW FUNCTIONS").ignore(pos, expected))
        return false;

    if (bool insensitive = ParserKeyword("ILIKE").ignore(pos, expected); insensitive || ParserKeyword("LIKE").ignore(pos, expected))
    {
        if (insensitive)
            query->case_insensitive_like = true;

        if (!ParserStringLiteral().parse(pos, like, expected))
            return false;
    }

    if (like)
        query->like = like->as<ASTLiteral &>().value.safeGet<const String &>();
    node = query;

    return true;
}

}
