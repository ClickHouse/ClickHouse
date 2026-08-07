#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/Kusto/KQLLexer.h>
#include <base/arithmeticOverflow.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** `kqlToTimespan(x)` - Kusto's `totimespan()` cast.
  *
  * Unlike the `timespan(...)` literal, `totimespan()` takes an arbitrary expression, so the
  * conversion has to dispatch on the argument *type* at analysis time rather than on how the
  * argument was spelled:
  *  - a timespan passes through unchanged (`totimespan(x * 1h)`);
  *  - a number counts days, the unit Kusto uses when converting numbers to timespans;
  *  - a string is read per row as `[-][d.]hh:mm:ss[.fffffff]`, the way Kusto prints a
  *    timespan, and an unreadable string converts to null rather than an error - it is a
  *    cast, not a literal.
  */
class FunctionKQLToTimespan final : public IFunction, WithContext
{
public:
    static constexpr auto name = "kqlToTimespan";

    explicit FunctionKQLToTimespan(ContextPtr context_) : WithContext(context_) { }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionKQLToTimespan>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & type = arguments[0];
        if (isInterval(type))
            return type;
        if (isNumber(type))
            return std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond);
        if (isStringOrFixedString(type))
            return makeNullable(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond));
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function {} converts a timespan, a number of days or a string, but the argument has type {}",
            getName(),
            type->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & argument = arguments[0];

        if (isInterval(argument.type))
            return argument.column;

        if (isNumber(argument.type))
        {
            /// Days to nanoseconds, delegated so every numeric type divides out the same way.
            const DataTypePtr constant_type = std::make_shared<DataTypeInt64>();
            const ColumnWithTypeAndName per_day{
                constant_type->createColumnConst(input_rows_count, Field(Int64(86'400'000'000'000))), constant_type, ""};

            ColumnsWithTypeAndName multiply_arguments{argument, per_day};
            auto multiply = FunctionFactory::instance().get("multiply", getContext())->build(multiply_arguments);
            const ColumnWithTypeAndName product{
                multiply->execute(multiply_arguments, multiply->getResultType(), input_rows_count, /*dry_run=*/false),
                multiply->getResultType(),
                ""};

            ColumnsWithTypeAndName conversion_arguments{product};
            auto to_interval = FunctionFactory::instance().get("toIntervalNanosecond", getContext())->build(conversion_arguments);
            return to_interval->execute(conversion_arguments, to_interval->getResultType(), input_rows_count, /*dry_run=*/false);
        }

        auto values = ColumnInt64::create(input_rows_count);
        auto null_map = ColumnUInt8::create(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view text = argument.column->getDataAt(i);
            /// A `FixedString` pads with zero bytes, which are not part of the value.
            while (!text.empty() && text.back() == '\0')
                text.remove_suffix(1);
            const std::optional<Int64> ticks = kqlParseTimespanText(text);
            Int64 nanoseconds = 0;
            const bool bad = !ticks || common::mulOverflow<Int64>(*ticks, 100, nanoseconds);
            values->getData()[i] = bad ? 0 : nanoseconds;
            null_map->getData()[i] = bad;
        }
        return ColumnNullable::create(std::move(values), std::move(null_map));
    }
};

}

REGISTER_FUNCTION(KQLToTimespan)
{
    FunctionDocumentation documentation{
        .description = R"(
Converts a value to a timespan (an `Interval` in nanoseconds) as the Kusto Query Language's
`totimespan()` does: a timespan passes through, a number counts days, and a string is read as
`[-][d.]hh:mm:ss[.fffffff]` - converting to `NULL` when it does not parse.

This function backs `totimespan()` when `dialect = 'kusto'`. It is not meant to be called
directly from SQL.
)",
        .syntax = "kqlToTimespan(x)",
        .arguments = {{"x", "A timespan, a number of days, or a string spelling a timespan."}},
        .returned_value = {"The value as an interval of nanoseconds; `NULL` for a string that does not spell a timespan."},
        .examples
        = {{"string", "SELECT kqlToTimespan('0.00:01:00')", "60000000000"},
           {"days", "SELECT kqlToTimespan(2)", "172800000000000"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::TypeConversion,
    };

    factory.registerFunction<FunctionKQLToTimespan>(documentation);
}

}
