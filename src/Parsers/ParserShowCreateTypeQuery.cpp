#include <Parsers/ParserShowCreateTypeQuery.h>

#include <Parsers/ASTShowCreateTypeQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

namespace
{

bool parseQualifiedTypeName(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & type_name)
{
    ParserIdentifier identifier_parser;
    ParserToken dot(TokenType::Dot);
    if (!identifier_parser.parse(pos, database, expected) || !dot.ignore(pos, expected))
        return false;
    return identifier_parser.parse(pos, type_name, expected) && !dot.check(pos, expected);
}

}

bool ParserShowCreateTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("SHOW CREATE TYPE").ignore(pos, expected))
        return false;

    ASTPtr database;
    ASTPtr type_name;
    /// `SHOW CREATE TYPE` was already valid pre-existing syntax for the table named
    /// TYPE. A qualified UDT target avoids reinterpreting that grammar.
    if (!parseQualifiedTypeName(pos, expected, database, type_name))
        return false;

    auto query = make_intrusive<ASTShowCreateTypeQuery>();
    query->database = std::move(database);
    query->type_name = std::move(type_name);
    query->normalizeChildrenOrder();
    node = std::move(query);
    return true;
}

}
