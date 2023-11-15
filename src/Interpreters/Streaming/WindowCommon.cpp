#include <Interpreters/Streaming/WindowCommon.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/FunctionHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

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
    if (func_node)
    {
        auto kind = mapIntervalKind(func_node->name);
        if (kind)
        {
            if (*kind <= IntervalKind::Day)
                return;
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}: the max interval kind supported is DAY.", msg);
        }
    }
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}", msg);
}

void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind)
{
    assert(ast);

    if (auto opt_kind = mapIntervalKind(ast->name); opt_kind)
        kind = opt_kind.value();
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid interval function");

    const auto * val = ast->arguments ? ast->arguments->children.front()->as<ASTLiteral>() : nullptr;
    if (!val)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid interval argument");

    if (val->value.getType() == Field::Types::UInt64)
    {
        interval = val->value.safeGet<UInt64>();
    }
    else if (val->value.getType() == Field::Types::Int64)
    {
        interval = val->value.safeGet<Int64>();
    }
    else if (val->value.getType() == Field::Types::String)
    {
        interval = std::stoi(val->value.safeGet<String>());
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid interval argument");
}

WindowInterval extractInterval(const ASTFunction * ast)
{
    WindowInterval window_interval;
    extractInterval(ast, window_interval.interval, window_interval.unit);
    return window_interval;
}

WindowInterval extractInterval(const ColumnWithTypeAndName & interval_column)
{
    const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
    assert(interval_type);
    const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
    assert(interval_column_const_int64);
    return {interval_column_const_int64->getValue<Int64>(), interval_type->getKind()};
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
