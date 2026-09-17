#include <Parsers/ParserShowTypesQuery.h>

#include <Parsers/ASTShowTypesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

bool ParserShowTypesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("SHOW TYPES").ignore(pos, expected))
        return false;

    ASTPtr database;
    ASTPtr like_pattern;
    if (ParserKeyword(Keyword::FROM).ignore(pos, expected) && !ParserIdentifier().parse(pos, database, expected))
        return false;

    if (ParserKeyword(Keyword::LIKE).ignore(pos, expected) && !ParserStringLiteral().parse(pos, like_pattern, expected))
        return false;

    if (ParserKeyword(Keyword::FROM).checkWithoutMoving(pos, expected) || ParserKeyword(Keyword::LIKE).checkWithoutMoving(pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate or out-of-order clause in SHOW TYPES");

    auto query = make_intrusive<ASTShowTypesQuery>();
    query->database = std::move(database);
    query->like_pattern = std::move(like_pattern);
    query->normalizeChildrenOrder();
    node = std::move(query);
    return true;
}

}
