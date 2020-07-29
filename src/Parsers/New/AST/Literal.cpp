#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTLiteral.h>


namespace DB::AST
{

Literal::Literal(const antlr4::tree::TerminalNode * literal) : token(literal)
{
    // FIXME: remove this.
    (void)token;
}

ASTPtr Literal::convertToOld() const
{
    return std::make_shared<ASTLiteral>()
}

}
