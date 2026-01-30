#include <Functions/JSONPath/Parsers/ParserJSONPathStar.h>

#include <Functions/JSONPath/ASTs/ASTJSONPathStar.h>

namespace DB
{
bool ParserJSONPathStar::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningSquareBracket)
    {
        return false;
    }
    ++pos;
    if (pos->type != TokenType::Asterisk)
    {
        return false;
    }
    ++pos;
    if (pos->type != TokenType::ClosingSquareBracket)
    {
        expected.add(pos, "Closing square bracket");
        return false;
    }
    ++pos;

    node = std::make_shared<ASTJSONPathStar>();

    return true;
}

}
