#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLPlan.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

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

        if (!value_is_null_literal && isDateOrDate32OrDateTimeOrDateTime64(value_type))
        {
            if (!isInterval(bin_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime by a timespan, but the second argument has type {}",
                    getName(),
                    arguments[1].type->getName());
            if (!isDateOrDate32OrDateTimeOrDateTime64(fixed_type))
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

            /// `fixedPoint + bin(value - fixedPoint, binSize)` over integer nanoseconds. The
            /// bins may align before or after the fixed point, and `kqlBin` turns a negative
            /// bin size into a null per row.
            KQLPlanBuilder plan(getContext());
            const size_t value_slot = plan.argument(arguments[0].type);
            size_t bin_slot = plan.argument(retypedAsTicks(arguments[1].type));
            const size_t fixed_slot = plan.argument(arguments[2].type);

            if (bin_kind.toAvgNanoseconds() != 1)
            {
                const size_t ticks = plan.constant(std::make_shared<DataTypeInt64>(), Field(bin_kind.toAvgNanoseconds()));
                bin_slot = plan.step("multiply", {bin_slot, ticks});
            }

            const size_t unit = plan.constant(std::make_shared<DataTypeString>(), Field("nanosecond"));
            const size_t difference = plan.step("dateDiff", {unit, fixed_slot, value_slot});
            const size_t rounded = plan.step("kqlBin", {difference, bin_slot});
            const size_t shift = plan.step("toIntervalNanosecond", {rounded});
            plan.step("plus", {fixed_slot, shift});
            return std::move(plan).finish(name, arguments);
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
timespan counted from a datetime fixed point.

This function backs `bin_at()` when `dialect = 'kusto'`. It is not meant to be called directly
from SQL.
)",
        .syntax = "kqlBinAt(value, binSize, fixedPoint)",
        .arguments
        = {{"value", "A number, a timespan, or a datetime."},
           {"binSize", "The bin size."},
           {"fixedPoint", "The point the bins are counted from."}},
        .returned_value = {"`value` rounded down to the nearest multiple of `binSize` counted from `fixedPoint`."},
        .examples
        = {{"number", "SELECT kqlBinAt(6.5, 2.5, -0.5)", "4.5"},
           {"datetime",
            "SELECT kqlBinAt(toDateTime('2026-08-01 12:34:56'), toIntervalHour(1), toDateTime('2026-08-01 00:30:00'))",
            "2026-08-01 12:30:00"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLBinAtOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLBinAtOverloadResolver::create(std::move(context)); },
        bin_at_documentation);
}

}
