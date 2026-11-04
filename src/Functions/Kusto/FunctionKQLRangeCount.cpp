#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLExactArithmetic.h>
#include <Common/assert_cast.h>
#include <base/arithmeticOverflow.h>

#include <cmath>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** `kqlRangeCount(from, to, step)` - the number of rows the KQL `range` source produces.
  *
  * The count is `floor((to - from) / step) + 1`, and never less than zero. Which arithmetic
  * that formula needs depends on the argument *types*, which the parser translating a `range`
  * source cannot see: numbers divide as numbers, but a KQL timespan is an `Interval` here, and
  * the span of two datetimes divided by a timespan has no single ClickHouse operator at all.
  * Datetimes and timespans are therefore counted in integer nanoseconds.
  */
class FunctionKQLRangeCount final : public IFunction
{
public:
    static constexpr auto name = "kqlRangeCount";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionKQLRangeCount>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    /// A null bound is reported as an error rather than propagated: the count feeds the
    /// `numbers` table function, which takes no NULL.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        classify(removeNullable(arguments[0]), removeNullable(arguments[1]), removeNullable(arguments[2]));
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        struct Operand
        {
            ColumnPtr full_column; /// Owns the column `values` and `nulls` point into.
            const IColumn * values = nullptr;
            DataTypePtr type;
            const NullMap * nulls = nullptr;
        };

        std::array<Operand, 3> operands;
        for (size_t i = 0; i < 3; ++i)
        {
            operands[i].full_column = arguments[i].column->convertToFullColumnIfConst();
            operands[i].values = operands[i].full_column.get();
            operands[i].type = removeNullable(arguments[i].type);
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(operands[i].values))
            {
                operands[i].nulls = &nullable->getNullMapData();
                operands[i].values = &nullable->getNestedColumn();
            }
        }

        const Domain domain = classify(operands[0].type, operands[1].type, operands[2].type);

        /// The exact domain compares all three operands at one common scale.
        std::array<Int256, 3> rescale{1, 1, 1};
        if (domain == Domain::Exact)
        {
            UInt32 common_scale = 0;
            for (const Operand & operand : operands)
                common_scale = std::max(common_scale, KQLExact::scaleOf(*operand.type));
            for (size_t i = 0; i < 3; ++i)
                rescale[i] = KQLExact::powerOfTen(common_scale - KQLExact::scaleOf(*operands[i].type));
        }

        auto result = ColumnUInt64::create(input_rows_count);
        auto & counts = result->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            for (const Operand & operand : operands)
                if (operand.nulls && (*operand.nulls)[row])
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The bounds and the step of a 'range' may not be null");

            if (domain == Domain::Numeric)
            {
                const Float64 from = operands[0].values->getFloat64(row);
                const Float64 to = operands[1].values->getFloat64(row);
                const Float64 step = operands[2].values->getFloat64(row);
                if (step == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The step of a 'range' must not be zero");

                const Float64 steps = std::floor((to - from) / step);
                if (!std::isfinite(steps))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The bounds and the step of a 'range' must be finite");
                if (steps >= static_cast<Float64>(std::numeric_limits<UInt64>::max()))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'range' has too many rows to count");
                counts[row] = steps < 0 ? 0 : static_cast<UInt64>(steps) + 1;
            }
            else if (domain == Domain::Exact)
            {
                /// Exact rational arithmetic: every operand as an integer at the common scale.
                const auto scaled = [&](size_t i) -> Int256
                {
                    Int256 value;
                    if (common::mulOverflow(KQLExact::unscaledValue(*operands[i].values, *operands[i].type, row), rescale[i], value))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The bounds and the step of a 'range' are too large to count");
                    return value;
                };
                Int256 span;
                if (common::subOverflow(scaled(1), scaled(0), span))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The bounds and the step of a 'range' are too large to count");
                counts[row] = flooredCount(span, scaled(2));
            }
            else
            {
                const Int128 span
                    = nanosecondsAt(*operands[1].values, *operands[1].type, row) - nanosecondsAt(*operands[0].values, *operands[0].type, row);
                const Int128 step = nanosecondsAt(*operands[2].values, *operands[2].type, row);
                counts[row] = flooredCount(span, step);
            }
        }

        return result;
    }

private:
    enum class Domain : uint8_t
    {
        Numeric,
        Exact,
        Temporal,
    };

    Domain classify(const DataTypePtr & from, const DataTypePtr & to, const DataTypePtr & step) const
    {
        /// Integers and decimals count exactly, in `Int256` over their unscaled values: a
        /// `Float64` cannot tell integers above 2^53 apart, and `(0.3 - 0.1) / 0.1` as a
        /// `Float64` is 1.9999..., which floors one row short of the exact decimal sequence.
        const auto is_exact = [](const DataTypePtr & type) { return KQLExact::isExactNumber(*type); };
        if (is_exact(from) && is_exact(to) && is_exact(step))
            return Domain::Exact;

        if (isNumber(from) && isNumber(to) && isNumber(step))
            return Domain::Numeric;

        const bool datetime_bounds = isDateTimeOrDateTime64(from) && isDateTimeOrDateTime64(to);
        if ((datetime_bounds || (isInterval(from) && isInterval(to))) && isInterval(step))
        {
            const auto step_kind = assert_cast<const DataTypeInterval &>(*step).getKind();
            if (!step_kind.isFixedLength())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "A 'range' cannot step by {}, which has no fixed length",
                    step_kind.toString());
            return Domain::Temporal;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "A 'range' goes from a number by a number, from a datetime by a timespan, or from a timespan by a timespan, "
            "but the bounds and the step have types {}, {} and {}",
            from->getName(),
            to->getName(),
            step->getName());
    }

    /// `floor(span / step) + 1`, and never less than zero. Integer division truncates
    /// toward zero, so an inexact quotient of differing signs is one too high.
    template <typename T>
    static UInt64 flooredCount(T span, T step)
    {
        if (step == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The step of a 'range' must not be zero");

        T steps = span / step;
        if (span % step != 0 && (span < 0) != (step < 0))
            --steps;
        if (steps >= T(std::numeric_limits<UInt64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'range' has too many rows to count");
        return steps < 0 ? 0 : static_cast<UInt64>(steps) + 1;
    }

    /// A datetime, or an interval of a fixed-length kind, as integer nanoseconds.
    static Int128 nanosecondsAt(const IColumn & column, const IDataType & type, size_t row)
    {
        if (const auto * datetime64 = typeid_cast<const DataTypeDateTime64 *>(&type))
        {
            const Int128 value = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData()[row].value;
            const UInt32 scale = datetime64->getScale();
            Int128 factor = 1;
            for (UInt32 i = std::min(scale, 9u); i < std::max(scale, 9u); ++i)
                factor *= 10;
            /// Ticks finer than a nanosecond round toward zero.
            return scale > 9 ? value / factor : value * factor;
        }

        if (isDateTime(type))
            return Int128(assert_cast<const ColumnUInt32 &>(column).getData()[row]) * 1'000'000'000;

        const auto kind = assert_cast<const DataTypeInterval &>(type).getKind();
        return Int128(assert_cast<const ColumnInt64 &>(column).getData()[row]) * kind.toAvgNanoseconds();
    }
};

}

REGISTER_FUNCTION(KQLRangeCount)
{
    FunctionDocumentation documentation{
        .description = R"(
The number of rows the `range` source of the Kusto Query Language produces: `floor((to - from)
/ step) + 1`, and never less than zero. The bounds and the step are numbers, or datetimes
stepped by a timespan (an `Interval`), or timespans; the temporal forms are counted in integer
nanoseconds, which no single ClickHouse division expresses. Integers and decimals are counted
exactly, not through `Float64`.

This function backs the `range` source when `dialect = 'kusto'`. It is not meant to be called
directly from SQL.
)",
        .syntax = "kqlRangeCount(from, to, step)",
        .arguments
        = {{"from", "The first value of the range."},
           {"to", "The value the range does not go past."},
           {"step", "The difference between two consecutive values."}},
        .returned_value = {"The number of values in the range."},
        .examples
        = {{"numbers", "SELECT kqlRangeCount(1, 7, 2)", "4"},
           {"datetimes",
            "SELECT kqlRangeCount(toDateTime64('2026-08-01 00:00:00', 7, 'UTC'), toDateTime64('2026-08-01 12:00:00', 7, 'UTC'), toIntervalHour(5))",
            "3"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction<FunctionKQLRangeCount>(documentation);
}

}
