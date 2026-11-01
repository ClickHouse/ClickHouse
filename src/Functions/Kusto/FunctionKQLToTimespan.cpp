#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLExactArithmetic.h>
#include <Interpreters/Context.h>
#include <Parsers/Kusto/KQLLexer.h>
#include <base/arithmeticOverflow.h>

#include <cmath>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
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

    explicit FunctionKQLToTimespan(ContextPtr context_)
        : WithContext(context_)
    {
    }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionKQLToTimespan>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & type = arguments[0];
        const DataTypePtr nested = removeNullable(type);
        if (isNothing(nested))
            return makeNullable(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond));
        if (isInterval(nested))
            return type;
        if (isNumber(nested))
        {
            const DataTypePtr result = std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond);
            return type->isNullable() ? makeNullable(result) : result;
        }
        if (isStringOrFixedString(nested))
            return makeNullable(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond));
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function {} converts a timespan, a number of days or a string, but the argument has type {}",
            getName(),
            type->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & argument = arguments[0];

        const DataTypePtr nested = removeNullable(argument.type);
        if (isNothing(nested))
            return result_type->createColumnConstWithDefaultValue(input_rows_count);
        if (isInterval(nested))
            return argument.column;

        if (isNumber(nested))
        {
            ColumnPtr full = argument.column->convertToFullColumnIfConst();
            const IColumn * source = full.get();
            const NullMap * source_nulls = nullptr;
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(source))
            {
                source_nulls = &nullable->getNullMapData();
                source = &nullable->getNestedColumn();
            }

            auto values = ColumnInt64::create(input_rows_count);
            auto null_map = ColumnUInt8::create(input_rows_count);
            constexpr Int64 nanoseconds_per_day_exact = 86'400'000'000'000;
            constexpr long double nanoseconds_per_day = 86'400'000'000'000.L;
            constexpr long double limit = static_cast<long double>(std::numeric_limits<Int64>::max()) + 1;
            /// An integer or a decimal count of days converts exactly; only a float has to
            /// go through `Float64`, which cannot spell every day count to the nanosecond.
            const bool source_exact = KQLExact::isExactNumber(*nested);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (source_nulls && (*source_nulls)[i])
                {
                    values->getData()[i] = 0;
                    null_map->getData()[i] = 1;
                    continue;
                }

                if (source_exact)
                {
                    values->getData()[i] = KQLExact::scaledTicks(nanoseconds_per_day_exact, *source, *nested, i, getName());
                    null_map->getData()[i] = 0;
                    continue;
                }

                const long double nanoseconds = static_cast<long double>(source->getFloat64(i)) * nanoseconds_per_day;
                if (!std::isfinite(nanoseconds) || nanoseconds < -limit || nanoseconds >= limit)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} result does not fit a timespan", getName());

                values->getData()[i] = static_cast<Int64>(std::trunc(nanoseconds));
                null_map->getData()[i] = 0;
            }

            if (source_nulls)
                return ColumnNullable::create(std::move(values), std::move(null_map));
            return values;
        }

        ColumnPtr full = argument.column->convertToFullColumnIfConst();
        const IColumn * source = full.get();
        const NullMap * source_nulls = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(source))
        {
            source_nulls = &nullable->getNullMapData();
            source = &nullable->getNestedColumn();
        }

        auto values = ColumnInt64::create(input_rows_count);
        auto null_map = ColumnUInt8::create(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (source_nulls && (*source_nulls)[i])
            {
                values->getData()[i] = 0;
                null_map->getData()[i] = 1;
                continue;
            }

            std::string_view text = source->getDataAt(i);
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
        = {{"string", "SELECT kqlToTimespan('0.00:01:00')", "60000000000"}, {"days", "SELECT kqlToTimespan(2)", "172800000000000"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::TypeConversion,
    };

    factory.registerFunction<FunctionKQLToTimespan>(documentation);
}

}
