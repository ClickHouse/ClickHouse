#include <Parsers/ParserStringAndSubstitution.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserStringAndSubstitution::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserStringLiteral{}.parse(pos, node, expected) || ParserSubstitution{}.parse(pos, node, expected);
}

}
