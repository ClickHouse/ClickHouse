#include <Parsers/ParserEnumElement.h>
#include <Parsers/ASTEnumElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

bool ParserEnumElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserStringLiteral name_parser;
    ParserNumber value_parser;
    ParserToken equality_sign_parser(TokenType::Equals);

    const auto begin = pos;

    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    if (!equality_sign_parser.ignore(pos, expected))
        return false;

    ASTPtr value;
    if (!value_parser.parse(pos, value, expected))
        return false;

    node = std::make_shared<ASTEnumElement>(
        StringRange{begin, pos}, static_cast<const ASTLiteral &>(*name).value.get<String>(), static_cast<const ASTLiteral &>(*value).value);

    return true;
}

}
