#include <Parsers/New/AST/Literal.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<Literal> Literal::createNull()
{
    return PtrTo<Literal>(new Literal(LiteralType::NULL_LITERAL, String()));
}

// static
PtrTo<NumberLiteral> Literal::createNumber(antlr4::tree::TerminalNode * literal, bool negative)
{
    auto number = std::make_shared<NumberLiteral>(literal);
    if (negative) number->makeNegative();
    return number;
}

// static
PtrTo<NumberLiteral> Literal::createNumber(const String & literal)
{
    bool has_minus = literal[0] == '-';
    auto number = std::make_shared<NumberLiteral>(has_minus ? literal.substr(1) : literal);
    if (has_minus) number->makeNegative();
    return number;
}

// static
PtrTo<StringLiteral> Literal::createString(antlr4::tree::TerminalNode * literal)
{
    return std::make_shared<StringLiteral>(literal);
}

// static
PtrTo<StringLiteral> Literal::createString(const String & literal)
{
    return std::make_shared<StringLiteral>(literal);
}

Literal::Literal(LiteralType type_, const String & token_) : token(token_), type(type_)
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
                const auto * number = this->as<NumberLiteral>();

                if (!number->isNegative())
                    if (auto value = number->as<UInt64>()) return Field(*value);
                if (auto value = number->as<Int64>()) return Field(*value);
                if (auto value = number->as<Float64>()) return Field(*value);

                return Field();
            }
            case LiteralType::STRING:
                return asString();
        }
        __builtin_unreachable();
    };

    return std::make_shared<ASTLiteral>(as_field());
}

String Literal::toString() const
{
    WriteBufferFromOwnString wb;
    writeEscapedString(token, wb);
    return type == LiteralType::STRING ? "'" + wb.str() + "'" : wb.str();
}

NumberLiteral::NumberLiteral(antlr4::tree::TerminalNode * literal) : Literal(LiteralType::NUMBER, literal->getSymbol()->getText())
{
}

NumberLiteral::NumberLiteral(const String & literal) : Literal(LiteralType::NUMBER, literal)
{
}

String NumberLiteral::toString() const
{
    return (minus ? String("-") : String()) + Literal::toString();
}

ASTSampleRatio::Rational NumberLiteral::convertToOldRational() const
{
    UInt64 num_before = 0;
    UInt64 num_after = 0;
    Int64 exponent = 0;

    const char * pos = token.data(), * end = token.data() + token.size();
    const char * pos_after_first_num = tryReadIntText(num_before, pos, end);

    bool has_num_before_point [[maybe_unused]] = pos_after_first_num > pos;
    pos = pos_after_first_num;
    bool has_point = pos < end && *pos == '.';

    if (has_point)
        ++pos;

    assert (has_num_before_point || has_point);

    size_t number_of_digits_after_point = 0;

    if (has_point)
    {
        const char * pos_after_second_num = tryReadIntText(num_after, pos, end);
        number_of_digits_after_point = pos_after_second_num - pos;
        pos = pos_after_second_num;
    }

    bool has_exponent = pos < end && (*pos == 'e' || *pos == 'E');

    if (has_exponent)
    {
        ++pos;
        const char * pos_after_exponent [[maybe_unused]] = tryReadIntText(exponent, pos, end);
        assert (pos_after_exponent != pos);
    }

    ASTSampleRatio::Rational res;
    res.numerator = num_before * intExp10(number_of_digits_after_point) + num_after;
    res.denominator = intExp10(number_of_digits_after_point);

    if (exponent > 0)
        res.numerator *= intExp10(exponent);
    if (exponent < 0)
        res.denominator *= intExp10(-exponent);

    return res;
}

StringLiteral::StringLiteral(antlr4::tree::TerminalNode * literal) : Literal(LiteralType::STRING, literal->getSymbol()->getText())
{
    String s;
    ReadBufferFromMemory in(token.data(), token.size());

    readQuotedStringWithSQLStyle(s, in);

    assert(in.count() == token.size());
    token = s;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitFloatingLiteral(ClickHouseParser::FloatingLiteralContext * ctx)
{
    if (ctx->FLOATING_LITERAL()) return Literal::createNumber(ctx->FLOATING_LITERAL());

    const auto * dot = ctx->DOT()->getSymbol();

    if (!ctx->DECIMAL_LITERAL().empty())
    {
        // .1234
        if (dot->getTokenIndex() < ctx->DECIMAL_LITERAL(0)->getSymbol()->getTokenIndex())
            return Literal::createNumber(dot->getText() + ctx->DECIMAL_LITERAL(0)->getSymbol()->getText());
        // 1234.
        else if (ctx->DECIMAL_LITERAL().size() == 1 && !ctx->OCTAL_LITERAL())
            return Literal::createNumber(ctx->DECIMAL_LITERAL(0)->getSymbol()->getText() + dot->getText());
        // 1234.1234
        else if (ctx->DECIMAL_LITERAL().size() == 2)
            return Literal::createNumber(
                ctx->DECIMAL_LITERAL(0)->getSymbol()->getText() + dot->getText() + ctx->DECIMAL_LITERAL(1)->getSymbol()->getText());
        // 1234.0123
        else
            return Literal::createNumber(
                ctx->DECIMAL_LITERAL(0)->getSymbol()->getText() + dot->getText() + ctx->OCTAL_LITERAL()->getSymbol()->getText());
    }
    else
        // .0123
        return Literal::createNumber(dot->getText() + ctx->OCTAL_LITERAL()->getSymbol()->getText());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitLiteral(ClickHouseParser::LiteralContext * ctx)
{
    if (ctx->NULL_SQL())
        return Literal::createNull();
    if (ctx->STRING_LITERAL())
        return std::static_pointer_cast<Literal>(Literal::createString(ctx->STRING_LITERAL()));
    if (ctx->numberLiteral())
        return std::static_pointer_cast<Literal>(visit(ctx->numberLiteral()).as<PtrTo<NumberLiteral>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitNumberLiteral(ClickHouseParser::NumberLiteralContext *ctx)
{
    if (ctx->floatingLiteral())
    {
        auto number = visit(ctx->floatingLiteral()).as<PtrTo<NumberLiteral>>();
        if (ctx->DASH()) number->makeNegative();
        return number;
    }
    if (ctx->OCTAL_LITERAL()) return Literal::createNumber(ctx->OCTAL_LITERAL(), !!ctx->DASH());
    if (ctx->DECIMAL_LITERAL()) return Literal::createNumber(ctx->DECIMAL_LITERAL(), !!ctx->DASH());
    if (ctx->HEXADECIMAL_LITERAL()) return Literal::createNumber(ctx->HEXADECIMAL_LITERAL(), !!ctx->DASH());
    if (ctx->INF()) return Literal::createNumber(ctx->INF(), !!ctx->DASH());
    if (ctx->NAN_SQL()) return Literal::createNumber(ctx->NAN_SQL());
    __builtin_unreachable();
}

}
