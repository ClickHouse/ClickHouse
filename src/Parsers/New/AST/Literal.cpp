#include <Parsers/New/AST/Literal.h>


namespace DB::AST
{

Literal::Literal(const antlr4::tree::TerminalNode * literal) : token(literal)
{
    // FIXME: remove this.
    (void)token;
}

}
