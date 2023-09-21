#include <Parsers/Streaming/ParserIntervalAliasExpression.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/IntervalKind.h>

namespace DB
{
bool ParserIntervalAliasExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!pos.isValid())
        return false;

    Pos pos_begin = pos;

    //// Case: +1s -1s 1s
    /// Parse + -
    bool negative = false;
    if (pos->type == TokenType::Minus)
    {
        ++pos;
        negative = true;
    }
    else if (pos->type == TokenType::Plus) /// Leading plus is simply ignored.
        ++pos;

    /// Parse number
    Int64 x = 0;
    ReadBufferFromMemory in(pos->begin, pos->size());
    if (!tryReadIntText(x, in) || in.count() == 0 || in.count() == pos->size())
        return elem_parser ? elem_parser->parse(pos = pos_begin, node, expected) : false;

    /// Parse interval kind
    IntervalKind interval_kind;
    {
        /// pos->begin = "10s"
        /// pos->size() = 3(token size)
        /// in.count() = 2(number size)
        String kind_str(pos->begin + in.count(), pos->size() - in.count());
        if ("ns" == kind_str)
            interval_kind = IntervalKind::Nanosecond;
        else if ("us" == kind_str)
            interval_kind = IntervalKind::Microsecond;
        else if ("ms" == kind_str)
            interval_kind = IntervalKind::Millisecond;
        else if ("s" == kind_str)
            interval_kind = IntervalKind::Second;
        else if ("m" == kind_str)
            interval_kind = IntervalKind::Minute;
        else if ("h" == kind_str)
            interval_kind = IntervalKind::Hour;
        else if ("d" == kind_str)
            interval_kind = IntervalKind::Day;
        else if ("w" == kind_str)
            interval_kind = IntervalKind::Week;
        else if ("M" == kind_str)
            interval_kind = IntervalKind::Month;
        else if ("q" == kind_str)
            interval_kind = IntervalKind::Quarter;
        else if ("y" == kind_str)
            interval_kind = IntervalKind::Year;
        else
            return elem_parser ? elem_parser->parse(pos = pos_begin, node, expected) : false;
    }

    auto expr = negative ? std::make_shared<ASTLiteral>(Int64(-x)) : std::make_shared<ASTLiteral>(UInt64(x));
    expr->begin = pos_begin;
    expr->end = ++pos;

    /// The function corresponding to the operator
    auto function = std::make_shared<ASTFunction>();

    /// Function arguments
    auto exp_list = std::make_shared<ASTExpressionList>();

    /// The first argument of the function is the previous element, the second is the next one
    function->name = interval_kind.toNameOfFunctionToIntervalDataType();
    function->arguments = exp_list;
    function->children.push_back(exp_list);

    /// Set code name: '+1s' '-10m' '1h' ...
    for (auto iter = pos_begin; iter != pos; ++iter)
        function->code_name.append(iter->begin, iter->size());

    exp_list->children.push_back(expr);

    node = function;
    return true;
}
}
