#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTLiteral.h>


namespace DB::AST
{

// static
PtrTo<Literal> Literal::createNull(antlr4::tree::TerminalNode *)
{
    // FIXME: check that it's a really Null literal.
    return PtrTo<Literal>(new Literal(LiteralType::NULL_LITERAL, String()));
}

// static
PtrTo<NumberLiteral> Literal::createNumber(antlr4::tree::TerminalNode * literal, bool minus)
{
    return std::make_shared<NumberLiteral>(literal, minus);
}

// static
PtrTo<NumberLiteral> Literal::createNumber(String&& literal)
{
    bool has_minus = literal[0] == '-';
    return std::make_shared<NumberLiteral>(has_minus ? literal.substr(1) : literal, has_minus);
}

// static
PtrTo<StringLiteral> Literal::createString(antlr4::tree::TerminalNode * literal)
{
    return std::make_shared<StringLiteral>(literal);
}

// static
PtrTo<StringLiteral> Literal::createString(String&& literal)
{
    return std::make_shared<StringLiteral>(std::move(literal));
}

Literal::Literal(LiteralType type_, String&& token_) : token(token_), type(type_)
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
            {
                const auto * number = static_cast<const NumberLiteral*>(this);

                if (auto value = number->as<Int64>()) return Field(*value);
                if (auto value = number->as<UInt64>()) return Field(*value);
                if (auto value = number->as<Float64>()) return Field(*value);

                return Field();
            }
            case LiteralType::STRING:
                return *asString<std::string>();
        }
    };

    return std::make_shared<ASTLiteral>(as_field());
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitLiteral(ClickHouseParser::LiteralContext *ctx)
{
    if (ctx->NULL_SQL())
        return Literal::createNull(ctx->NULL_SQL());
    if (ctx->STRING_LITERAL())
        return static_pointer_cast<Literal>(Literal::createString(ctx->STRING_LITERAL()));
    if (ctx->identifier())
        // TODO: store as function.
        return static_pointer_cast<Literal>(Literal::createString(ctx->identifier()->IDENTIFIER()));
    if (ctx->numberLiteral())
        return static_pointer_cast<Literal>(visit(ctx->numberLiteral()).as<PtrTo<NumberLiteral>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitNumberLiteral(ClickHouseParser::NumberLiteralContext *ctx)
{
    if (ctx->FLOATING_LITERAL())
        return Literal::createNumber(ctx->FLOATING_LITERAL(), !!ctx->DASH());
    if (ctx->DOT())
        // TODO: create floating literal instead
        return Literal::createNumber(ctx->INTEGER_LITERAL(), !!ctx->DASH());
    if (ctx->HEXADECIMAL_LITERAL())
        return Literal::createNumber(ctx->HEXADECIMAL_LITERAL(), !!ctx->DASH());
    if (ctx->INTEGER_LITERAL())
        return Literal::createNumber(ctx->INTEGER_LITERAL(), !!ctx->DASH());
    if (ctx->INF())
        return Literal::createNumber(ctx->INF(), !!ctx->DASH());
    if (ctx->NAN_SQL())
        return Literal::createNumber(ctx->NAN_SQL());
    __builtin_unreachable();
}

}
