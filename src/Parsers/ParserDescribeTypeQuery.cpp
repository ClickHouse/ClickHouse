#include <Parsers/ParserDescribeTypeQuery.h>

#include <Parsers/ASTDescribeTypeQuery.h>
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

bool ParserDescribeTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("DESCRIBE TYPE").ignore(pos, expected))
        return false;

    ASTPtr database;
    ASTPtr type_name;
    /// Unqualified `DESCRIBE TYPE alias` is existing table syntax. A qualified
    /// UDT target avoids reinterpreting that grammar.
    if (!parseQualifiedTypeName(pos, expected, database, type_name))
        return false;

    auto query = make_intrusive<ASTDescribeTypeQuery>();
    query->database = std::move(database);
    query->type_name = std::move(type_name);
    query->normalizeChildrenOrder();
    node = std::move(query);
    return true;
}

}
