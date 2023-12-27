#include <Interpreters/Streaming/WindowCommon.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/FunctionHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_CONVERT_TYPE;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Streaming
{
namespace
{
std::optional<IntervalKind> mapIntervalKind(const String & func_name)
{
    if (func_name == "toIntervalNanosecond")
        return IntervalKind::Nanosecond;
    else if (func_name == "toIntervalMicrosecond")
        return IntervalKind::Microsecond;
    else if (func_name == "toIntervalMillisecond")
        return IntervalKind::Millisecond;
    else if (func_name == "toIntervalSecond")
        return IntervalKind::Second;
    else if (func_name == "toIntervalMinute")
        return IntervalKind::Minute;
    else if (func_name == "toIntervalHour")
        return IntervalKind::Hour;
    else if (func_name == "toIntervalDay")
        return IntervalKind::Day;
    else if (func_name == "toIntervalWeek")
        return IntervalKind::Week;
    else if (func_name == "toIntervalMonth")
        return IntervalKind::Month;
    else if (func_name == "toIntervalQuarter")
        return IntervalKind::Quarter;
    else if (func_name == "toIntervalYear")
        return IntervalKind::Year;
    else
        return {};
}
}

void checkIntervalAST(const ASTPtr & ast, const String & msg)
{
    assert(ast);
    auto * func_node = ast->as<ASTFunction>();
    assert(func_node);

    auto kind = mapIntervalKind(func_node->name);
    if (kind)
    {
        if (*kind <= IntervalKind::Day)
            return;
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}: the max interval kind supported is DAY.", msg);
    }
}

WindowInterval extractInterval(const ASTPtr & ast, const ContextPtr & context)
{
    auto [field, type] = evaluateConstantExpression(ast, context);

    if (const auto * type_interval = typeid_cast<const DataTypeInterval *>(type.get()))
        return WindowInterval {
            .interval = field.get<Int64>(),
            .unit = type_interval->getKind()
        };

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of EMIT expression, must be constant interval function", type->getName());
}

Int64 BaseScaleInterval::toIntervalKind(IntervalKind::Kind to_kind) const
{
    if (scale == to_kind)
        return num_units;

    const auto & bs = toBaseScale(1, to_kind);
    if (scale != bs.scale)
        throw Exception(
            ErrorCodes::CANNOT_CONVERT_TYPE,
            "Scale conversion is not possible between '{}' and '{}'",
            IntervalKind(src_kind).toString(),
            IntervalKind(to_kind).toString());

    if (num_units < bs.num_units)
        return 1;

    return num_units / bs.num_units;
}

String BaseScaleInterval::toString() const
{
    return fmt::format("{}{}", num_units, (scale == SCALE_NANOSECOND ? "ns" : "M"));
}

}
}
