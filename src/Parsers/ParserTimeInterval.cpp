#include <Parsers/ParserTimeInterval.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIntervalKind.h>

#include <Parsers/ASTTimeInterval.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

struct ValKind
{
    UInt64 val;
    IntervalKind kind;
    bool empty;
};

std::optional<ValKind> parseValKind(IParser::Pos & pos, Expected & expected)
{
    ASTPtr value;
    IntervalKind kind;
    if (!ParserNumber{}.parse(pos, value, expected))
        return ValKind{ .empty = true };
    if (!parseIntervalKind(pos, expected, kind))
        return {};
    UInt64 val;
    if (!value->as<ASTLiteral &>().value.tryGet(val))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Time interval must be an integer");
    return ValKind{ val, kind, false };
}

}

bool ParserTimePeriod::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto parsed = parseValKind(pos, expected);

    if (!parsed || parsed->empty || parsed->val == 0)
        return false;

    auto time_period = std::make_shared<ASTTimePeriod>();
    time_period->value = parsed->val;
    time_period->kind = parsed->kind;

    node = time_period;
    return true;
}

bool ParserTimeInterval::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto time_interval = std::make_shared<ASTTimeInterval>();

    auto parsed = parseValKind(pos, expected);
    while (parsed && !parsed->empty)
    {
        if (parsed->val == 0)
            return false;
        auto [it, inserted] = time_interval->kinds.emplace(parsed->kind, parsed->val);
        if (!inserted)
            return false;
        parsed = parseValKind(pos, expected);
    }

    if (!parsed || time_interval->kinds.empty())
        return false;
    node = time_interval;
    return true;
}

}
