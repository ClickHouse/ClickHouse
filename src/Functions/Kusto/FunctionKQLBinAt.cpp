#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLPlan.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

/// Fixed-point datetime arithmetic is deliberately kept out of a `dateDiff` plan. A
/// `DateTime64` tick is an integer and can exceed the `Int64` nanosecond range; widening it
/// before subtracting also makes the calculation independent of the display time zone.
class FunctionKQLDateTimeBinAt final : public IFunction
{
public:
    static constexpr auto name = "kqlDateTimeBinAt";

    explicit FunctionKQLDateTimeBinAt(ContextPtr context_)
        : overflow_behavior(getDateTimeOverflowBehavior(context_))
    {
    }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionKQLDateTimeBinAt>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isDateTime64(removeNullable(arguments[0])) || !isInterval(removeNullable(arguments[1]))
            || !isDateTime64(removeNullable(arguments[2])))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} expects a datetime, a timespan and a datetime", getName());

        /// KQL datetimes have 100 ns precision. Preserve a more precise physical input, but
        /// never truncate a valid bin merely because the source column uses a coarser carrier.
        const auto & value_type = assert_cast<const DataTypeDateTime64 &>(*removeNullable(arguments[0]));
        const UInt32 result_scale = std::max<UInt32>(value_type.getScale(), 7);
        return makeNullable(std::make_shared<DataTypeDateTime64>(result_scale, value_type));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t rows) const override
    {
        struct Operand
        {
            ColumnPtr full;
            const IColumn * values = nullptr;
            const NullMap * nulls = nullptr;
        };
        std::array<Operand, 3> operands;
        for (size_t i = 0; i < operands.size(); ++i)
        {
            operands[i].full = arguments[i].column->convertToFullColumnIfConst();
            operands[i].values = operands[i].full.get();
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(operands[i].values))
            {
                operands[i].nulls = &nullable->getNullMapData();
                operands[i].values = &nullable->getNestedColumn();
            }
        }

        const auto & value_type = *removeNullable(arguments[0].type);
        const auto & fixed_type = *removeNullable(arguments[2].type);
        const auto kind = assert_cast<const DataTypeInterval &>(*removeNullable(arguments[1].type)).getKind();
        const auto & result_datetime_type = *removeNullable(result_type);
        auto nested = result_datetime_type.createColumn();
        auto nulls = ColumnUInt8::create();
        auto & null_map = nulls->getData();

        for (size_t row = 0; row < rows; ++row)
        {
            if ((operands[0].nulls && (*operands[0].nulls)[row]) || (operands[1].nulls && (*operands[1].nulls)[row])
                || (operands[2].nulls && (*operands[2].nulls)[row]))
            {
                nested->insertDefault();
                null_map.push_back(UInt8(1));
                continue;
            }

            const Int128 bin = Int128(assert_cast<const ColumnInt64 &>(*operands[1].values).getData()[row]) * kind.toAvgNanoseconds();
            if (bin <= 0)
            {
                nested->insertDefault();
                null_map.push_back(UInt8(1));
                continue;
            }

            const Int128 fixed = nanosecondsAt(*operands[2].values, fixed_type, row);
            const Int128 span = nanosecondsAt(*operands[0].values, value_type, row) - fixed;
            Int128 quotient = span / bin;
            if (span % bin != 0 && span < 0)
                --quotient;
            insertNanoseconds(*nested, result_datetime_type, fixed + quotient * bin);
            null_map.push_back(UInt8(0));
        }
        return ColumnNullable::create(std::move(nested), std::move(nulls));
    }

private:
    const FormatSettings::DateTimeOverflowBehavior overflow_behavior;

    static Int128 nanosecondsAt(const IColumn & column, const IDataType & type, size_t row)
    {
        if (const auto * datetime64 = typeid_cast<const DataTypeDateTime64 *>(&type))
        {
            Int128 value = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData()[row].value;
            for (UInt32 scale = datetime64->getScale(); scale < 9; ++scale)
                value *= 10;
            for (UInt32 scale = 9; scale < datetime64->getScale(); ++scale)
                value /= 10;
            return value;
        }
        return Int128(assert_cast<const ColumnUInt32 &>(column).getData()[row]) * 1'000'000'000;
    }

    void insertNanoseconds(IColumn & column, const IDataType & type, Int128 value) const
    {
        if (const auto * datetime64 = typeid_cast<const DataTypeDateTime64 *>(&type))
        {
            for (UInt32 scale = datetime64->getScale(); scale < 9; ++scale)
                value /= 10;
            for (UInt32 scale = 9; scale < datetime64->getScale(); ++scale)
                value *= 10;
            const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(datetime64->getScale());
            const Int64 min = (MIN_DATETIME64_TIMESTAMP >= std::numeric_limits<Int64>::min() / scale_multiplier)
                ? MIN_DATETIME64_TIMESTAMP * scale_multiplier
                : std::numeric_limits<Int64>::min();
            const Int64 max = (MAX_DATETIME64_TIMESTAMP <= (std::numeric_limits<Int64>::max() - (scale_multiplier - 1)) / scale_multiplier)
                ? MAX_DATETIME64_TIMESTAMP * scale_multiplier + scale_multiplier - 1
                : std::numeric_limits<Int64>::max();

            if (value < min || value > max)
            {
                if (overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "The result of {} is out of bounds of type DateTime64({})",
                        getName(),
                        datetime64->getScale());
                if (overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
                    value = std::clamp<Int128>(value, min, max);
            }

            assert_cast<ColumnDecimal<DateTime64> &>(column).getData().push_back(DateTime64(static_cast<Int64>(value)));
            return;
        }
        assert_cast<ColumnUInt32 &>(column).getData().push_back(UInt32(value / 1'000'000'000));
    }
};

/** `kqlBinAt(value, binSize, fixedPoint)` - Kusto's `bin_at()`, rounding a value down to a
  * multiple of `binSize` counted from `fixedPoint` rather than from zero.
  *
  * Every form computes `fixedPoint + bin(value - fixedPoint, binSize)`: numbers directly,
  * timespans over their integer ticks, and datetimes over the nanosecond span between the
  * fixed point and the value - Kusto lets the bins align before *or after* the fixed point,
  * which rules out `toStartOfInterval`, whose origin must not be past the value. Only the
  * argument types say which applies, so the decision is made here, during analysis, like
  * `kqlBin` does.
  */
class FunctionKQLBinAtOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlBinAt";

    explicit FunctionKQLBinAtOverloadResolver(ContextPtr context_) : WithContext(context_) { }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLBinAtOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return delegate(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return delegate(arguments)->getResultType();
    }

private:
    /// A function over the given arguments that never reads them and returns a constant NULL.
    /// The argument types the analyzer sees stay the real ones.
    FunctionBasePtr buildNullResult(const ColumnsWithTypeAndName & arguments) const
    {
        KQLPlanBuilder plan(getContext());
        for (const auto & argument : arguments)
            plan.argument(argument.type);
        const size_t null_literal = plan.constant(makeNullable(std::make_shared<DataTypeNothing>()), Field());
        plan.step("identity", {null_literal});
        return std::move(plan).finish(name, arguments);
    }

    static DataTypePtr retypedAsTicks(const DataTypePtr & original)
    {
        const DataTypePtr ticks = std::make_shared<DataTypeInt64>();
        return original->isNullable() ? makeNullable(ticks) : ticks;
    }

    FunctionBasePtr delegate(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 3 arguments", getName());

        const DataTypePtr value_type = removeNullable(arguments[0].type);
        const DataTypePtr bin_type = removeNullable(arguments[1].type);
        const DataTypePtr fixed_type = removeNullable(arguments[2].type);

        /// A NULL literal argument makes the whole result a NULL literal; the numeric chain
        /// below short-circuits it, so it must get the query regardless of the other types.
        const bool value_is_null_literal = isNothing(value_type) || isNothing(bin_type) || isNothing(fixed_type);

        /// KQL datetime values are `DateTime64`. The other ClickHouse date carriers cannot
        /// faithfully represent the full KQL result range: in particular, `DateTime` is an
        /// unsigned epoch-second type, while a valid `bin_at` result may precede 1970.
        if (!value_is_null_literal && isDateTime64(value_type))
        {
            if (!isInterval(bin_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime by a timespan, but the second argument has type {}",
                    getName(),
                    arguments[1].type->getName());
            if (!isDateTime64(fixed_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime from a datetime fixed point, but the third argument has type {}",
                    getName(),
                    arguments[2].type->getName());

            const IntervalKind bin_kind = assert_cast<const DataTypeInterval &>(*bin_type).getKind();

            /// A calendar-length bin (a month has no fixed number of nanoseconds) cannot take
            /// the span formula; `toStartOfInterval` handles it, with its restriction that the
            /// fixed point not be past the value. The KQL dialect itself never produces such a
            /// bin - a timespan is always fixed-length. Kusto returns null for a negative bin
            /// size; `toStartOfInterval` would throw, and it only takes a constant interval,
            /// so the sign is known here.
            if (!bin_kind.isFixedLength())
            {
                if (arguments[1].column && isColumnConst(*arguments[1].column))
                {
                    const Field interval = assert_cast<const ColumnConst &>(*arguments[1].column).getField();
                    if (!interval.isNull() && interval.safeGet<Int64>() < 0)
                        return buildNullResult(arguments);
                }
                return FunctionFactory::instance().get("toStartOfInterval", getContext())->build(arguments);
            }

            return FunctionFactory::instance().get(FunctionKQLDateTimeBinAt::name, getContext())->build(arguments);
        }

        if (!value_is_null_literal && isInterval(value_type))
        {
            if (!isInterval(bin_type) || !isInterval(fixed_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a timespan by a timespan from a timespan fixed point, got {}, {} and {}",
                    getName(),
                    arguments[0].type->getName(),
                    arguments[1].type->getName(),
                    arguments[2].type->getName());

            const IntervalKind value_kind = assert_cast<const DataTypeInterval &>(*value_type).getKind();
            const IntervalKind bin_kind = assert_cast<const DataTypeInterval &>(*bin_type).getKind();
            const IntervalKind fixed_kind = assert_cast<const DataTypeInterval &>(*fixed_type).getKind();

            /// Integer arithmetic over the intervals' ticks, like `kqlBin`: equal kinds count
            /// in their own unit; unequal fixed-length kinds are normalized to nanoseconds.
            KQLPlanBuilder plan(getContext());
            size_t value_slot = plan.argument(retypedAsTicks(arguments[0].type));
            size_t bin_slot = plan.argument(retypedAsTicks(arguments[1].type));
            size_t fixed_slot = plan.argument(retypedAsTicks(arguments[2].type));

            IntervalKind result_kind = value_kind;
            if (value_kind != bin_kind || value_kind != fixed_kind)
            {
                if (!value_kind.isFixedLength() || !bin_kind.isFixedLength() || !fixed_kind.isFixedLength())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} cannot round {} by {} from {}: the kinds differ and not all have a fixed length",
                        getName(),
                        arguments[0].type->getName(),
                        arguments[1].type->getName(),
                        arguments[2].type->getName());
                const auto in_nanoseconds = [&](size_t slot, const IntervalKind & kind)
                {
                    if (kind.toAvgNanoseconds() == 1)
                        return slot;
                    const size_t ticks = plan.constant(std::make_shared<DataTypeInt64>(), Field(kind.toAvgNanoseconds()));
                    return plan.step("multiply", {slot, ticks});
                };
                value_slot = in_nanoseconds(value_slot, value_kind);
                bin_slot = in_nanoseconds(bin_slot, bin_kind);
                fixed_slot = in_nanoseconds(fixed_slot, fixed_kind);
                result_kind = IntervalKind::Kind::Nanosecond;
            }

            const size_t difference = plan.step("minus", {value_slot, fixed_slot});
            const size_t rounded = plan.step("kqlBin", {difference, bin_slot});
            const size_t sum = plan.step("plus", {fixed_slot, rounded});
            plan.step(result_kind.toNameOfFunctionToIntervalDataType(), {sum});
            return std::move(plan).finish(name, arguments);
        }

        if (!value_is_null_literal && (!isNumber(value_type) || !isNumber(bin_type) || !isNumber(fixed_type)))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects a number rounded by a number from a number, a timespan rounded by a timespan "
                "from a timespan, or a datetime rounded by a timespan from a datetime, got {}, {} and {}",
                getName(),
                arguments[0].type->getName(),
                arguments[1].type->getName(),
                arguments[2].type->getName());

        /// `fixedPoint + bin(value - fixedPoint, binSize)`. Each delegated function keeps its
        /// own null handling, and a NULL literal argument short-circuits through the chain.
        KQLPlanBuilder plan(getContext());
        const size_t value_slot = plan.argument(arguments[0].type);
        const size_t bin_slot = plan.argument(arguments[1].type);
        const size_t fixed_slot = plan.argument(arguments[2].type);

        const size_t difference = plan.step("minus", {value_slot, fixed_slot});
        const size_t rounded = plan.step("kqlBin", {difference, bin_slot});
        plan.step("plus", {fixed_slot, rounded});
        return std::move(plan).finish(name, arguments);
    }
};

}

REGISTER_FUNCTION(KQLBinAt)
{
    FunctionDocumentation bin_at_documentation{
        .description = R"(
Rounds a value down to a multiple of `binSize` counted from `fixedPoint`, as the Kusto Query
Language's `bin_at()` does. The bins may align before or after the fixed point.

The rule depends on the argument types: a number is rounded arithmetically, a timespan (which
is an `Interval`) is rounded by a timespan from a timespan, and a datetime is rounded by a
timespan counted from a datetime fixed point. A KQL datetime is a `DateTime64`; the narrower
`DateTime` and `Date` carriers are rejected, because they cannot represent every bin a KQL
datetime can produce.

This function backs `bin_at()` when `dialect = 'kusto'`. It is not meant to be called directly
from SQL.
)",
        .syntax = "kqlBinAt(value, binSize, fixedPoint)",
        .arguments
        = {{"value", "A number, a timespan, or a datetime (a `DateTime64`)."},
           {"binSize", "The bin size."},
           {"fixedPoint", "The point the bins are counted from."}},
        .returned_value = {"`value` rounded down to the nearest multiple of `binSize` counted from `fixedPoint`."},
        .examples
        = {{"number", "SELECT kqlBinAt(6.5, 2.5, -0.5)", "4.5"},
           {"datetime",
            "SELECT kqlBinAt(toDateTime64('2026-08-01 12:34:56', 7, 'UTC'), toIntervalHour(1), toDateTime64('2026-08-01 00:30:00', 7, 'UTC'))",
            "2026-08-01 12:30:00.0000000"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction<FunctionKQLDateTimeBinAt>(FunctionDocumentation{
        .description = "Rounds a datetime down to a timespan multiple counted from a datetime fixed point.",
        .syntax = "kqlDateTimeBinAt(value, binSize, fixedPoint)",
        .arguments = {{"value", "The datetime to round."}, {"binSize", "The timespan bin size."}, {"fixedPoint", "The datetime fixed point."}},
        .returned_value = {"The rounded datetime."},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    });

    factory.registerFunction(
        FunctionKQLBinAtOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLBinAtOverloadResolver::create(std::move(context)); },
        bin_at_documentation);
}

}
