#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTLiteral.h>


namespace DB::AST
{

// static
PtrTo<Literal> Literal::createNull(antlr4::tree::TerminalNode * literal)
{
    return PtrTo<Literal>(new Literal(LiteralType::NULL_LITERAL, literal));
}

// static
PtrTo<NumberLiteral> Literal::createNumber(antlr4::tree::TerminalNode * literal)
{
    return std::make_shared<NumberLiteral>(literal);
}

// static
PtrTo<StringLiteral> Literal::createString(antlr4::tree::TerminalNode * literal)
{
    return std::make_shared<StringLiteral>(literal);
}

Literal::Literal(LiteralType type_, antlr4::tree::TerminalNode * literal) : token(literal), type(type_)
{
}

ASTPtr Literal::convertToOld() const
{
    auto as_field = [this] () -> Field
    {
        switch(type)
        {
            case LiteralType::NULL_LITERAL:
                return Field(Null());
            case LiteralType::NUMBER:
                if (auto value = asNumber<Float64>()) return Field(*value);
                if (auto value = asNumber<UInt64>()) return Field(*value);
                if (auto value = asNumber<Int64>()) return Field(*value);
                return Field();
            case LiteralType::STRING:
                return *asString<std::string>();
        }
    };

    return std::make_shared<ASTLiteral>(as_field());
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitLiteral(ClickHouseParser::LiteralContext *ctx)
{
    if (ctx->NULL_SQL())
        return AST::Literal::createNull(ctx->NULL_SQL());
    if (ctx->NUMBER_LITERAL())
        return std::static_pointer_cast<AST::Literal>(AST::Literal::createNumber(ctx->NUMBER_LITERAL()));
    if (ctx->STRING_LITERAL())
        return std::static_pointer_cast<AST::Literal>(AST::Literal::createString(ctx->STRING_LITERAL()));
    __builtin_unreachable();
}

}
