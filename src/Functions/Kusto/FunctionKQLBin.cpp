#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeDateTime.h>
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

/** `kqlBin(value, roundTo)` - Kusto's `bin()`, rounding a value down to a multiple of `roundTo`.
  *
  * The rule reads the same in every case (`floor(value / roundTo) * roundTo`) but the way to
  * compute it is not: numbers divide, a timespan is an `Interval` counted in integer ticks,
  * and a datetime has to be rounded by an interval. Only the argument types say which
  * applies, so the decision is made here rather than guessed from how the argument was
  * spelled - the previous KQL implementation compared the first *token* against the text
  * "datetime", so `bin(Timestamp, 1d)` over a datetime column took the numeric branch and
  * emitted `toFloat64(Timestamp)`.
  *
  * Being a resolver rather than a function means that dispatch happens once, during analysis.
  */
class FunctionKQLBinOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlBin";

    explicit FunctionKQLBinOverloadResolver(ContextPtr context_)
        : WithContext(context_)
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLBinOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override { return delegate(arguments); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override { return delegate(arguments)->getResultType(); }

private:
    /// A function over the given arguments that never reads them and returns a constant NULL,
    /// as a plan: the argument slots stay unused and the only step passes a NULL constant
    /// through, so the argument types the analyzer sees stay the real ones.
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
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 2 arguments", getName());

        const DataTypePtr value_type = removeNullable(arguments[0].type);
        const DataTypePtr bin_type = removeNullable(arguments[1].type);

        /// A NULL literal argument makes the whole result a NULL literal. `divide` short-circuits
        /// that itself, so delegating to it wholesale beats teaching the chain below about Nothing.
        /// The short circuit fires on a *column* of nulls, which a synthetic argument built from a
        /// type alone does not have - so materialize one.
        if (isNothing(value_type) || isNothing(bin_type))
        {
            ColumnsWithTypeAndName null_arguments = arguments;
            for (auto & argument : null_arguments)
                if (!argument.column && isNothing(removeNullable(argument.type)))
                    argument.column = argument.type->createColumnConstWithDefaultValue(1);
            return FunctionFactory::instance().get("divide", getContext())->build(null_arguments);
        }

        /// A datetime is rounded by an interval. The bin size may be a column, so the rounding
        /// cannot delegate to `toStartOfInterval`, which only takes a constant interval.
        /// KQL datetime values are `DateTime64`. Do not accept ClickHouse's narrower date
        /// carriers here: for example, `DateTime` cannot represent a bin before the Unix
        /// epoch, whereas KQL datetime arithmetic can produce one.
        if (isDateTime64(value_type))
        {
            if (!isInterval(bin_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime by a timespan, but the second argument has type {}",
                    getName(),
                    arguments[1].type->getName());

            const IntervalKind interval_kind = assert_cast<const DataTypeInterval &>(*bin_type).getKind();

            /// A calendar-length bin (a month has no fixed number of nanoseconds) cannot take
            /// the span formula below; `toStartOfInterval` handles it. The KQL dialect itself
            /// never produces such a bin - a timespan is always fixed-length. Kusto returns
            /// null for a negative bin size; `toStartOfInterval` would throw, and it only
            /// takes a constant interval, so the sign is known here.
            if (!interval_kind.isFixedLength())
            {
                if (arguments[1].column && isColumnConst(*arguments[1].column))
                {
                    const Field interval = assert_cast<const ColumnConst &>(*arguments[1].column).getField();
                    if (!interval.isNull() && interval.safeGet<Int64>() < 0)
                        return buildNullResult(arguments);
                }
                return FunctionFactory::instance().get("toStartOfInterval", getContext())->build(arguments);
            }

            /// Delegate the fixed-point arithmetic to `kqlDateTimeBinAt`. It works over widened
            /// physical ticks, avoiding both timezone-sensitive `dateDiff` and `Int64`
            /// nanosecond overflow for valid `DateTime64` values.
            KQLPlanBuilder plan(getContext());
            const size_t value_slot = plan.argument(arguments[0].type);
            const size_t bin_slot = plan.argument(arguments[1].type);
            const size_t epoch = plan.constant(arguments[0].type, Field(DateTime64(0)));
            plan.step("kqlDateTimeBinAt", {value_slot, bin_slot, epoch});
            return std::move(plan).finish(name, arguments);
        }

        /// A timespan rounded by a timespan is integer arithmetic over the intervals' ticks:
        /// an `Interval` column is a plain `Int64` column, so retyping the argument slots
        /// turns the case into the signed-integer chain below, and the last step converts
        /// the rounded tick count back into an interval.
        const auto * value_interval = typeid_cast<const DataTypeInterval *>(value_type.get());
        const auto * bin_interval = typeid_cast<const DataTypeInterval *>(bin_type.get());
        const bool intervals = value_interval && bin_interval;

        if (!intervals && (!isNumber(value_type) || !isNumber(bin_type)))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects a number rounded by a number, a timespan rounded by a timespan, "
                "or a datetime rounded by a timespan, got {} and {}",
                getName(),
                arguments[0].type->getName(),
                arguments[1].type->getName());

        KQLPlanBuilder plan(getContext());

        size_t value_slot = plan.argument(intervals ? retypedAsTicks(arguments[0].type) : arguments[0].type);
        size_t bin_slot = plan.argument(intervals ? retypedAsTicks(arguments[1].type) : arguments[1].type);
        const size_t zero = plan.constant(std::make_shared<DataTypeUInt8>(), Field(UInt64(0)));

        /// The KQL dialect makes every timespan an `IntervalNanosecond`, so equal kinds count
        /// in their own unit; unequal fixed-length kinds are normalized to nanoseconds first.
        IntervalKind result_kind = IntervalKind::Kind::Nanosecond;
        if (intervals)
        {
            const IntervalKind value_kind = value_interval->getKind();
            const IntervalKind bin_kind = bin_interval->getKind();
            if (value_kind == bin_kind)
            {
                result_kind = value_kind;
            }
            else
            {
                if (!value_kind.isFixedLength() || !bin_kind.isFixedLength())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} cannot round {} by {}: the kinds differ and not both have a fixed length",
                        getName(),
                        arguments[0].type->getName(),
                        arguments[1].type->getName());
                const auto in_nanoseconds = [&](size_t slot, const IntervalKind & kind)
                {
                    if (kind.toAvgNanoseconds() == 1)
                        return slot;
                    const size_t ticks = plan.constant(std::make_shared<DataTypeInt64>(), Field(kind.toAvgNanoseconds()));
                    return plan.step("multiply", {slot, ticks});
                };
                value_slot = in_nanoseconds(value_slot, value_kind);
                bin_slot = in_nanoseconds(bin_slot, bin_kind);
            }
        }

        /// Kusto's contract: a negative bin size makes the result null, whatever the value. The
        /// bin size may be a column, so the check is per row: the bin size is routed through
        /// `if(roundTo < 0, NULL, roundTo)` up front, and the chain below inherits the null
        /// instead of every step guarding. An unsigned bin size cannot be negative, so it skips
        /// the detour (which would also signed-flip the unsigned-only branch below).
        if (intervals || !WhichDataType(bin_type).isUInt())
        {
            const size_t null_literal = plan.constant(makeNullable(std::make_shared<DataTypeNothing>()), Field());
            const size_t bin_negative = plan.step("less", {bin_slot, zero});
            bin_slot = plan.step("if", {bin_negative, null_literal, bin_slot});
        }

        size_t rounded = 0;
        if (!intervals && WhichDataType(value_type).isUInt() && WhichDataType(bin_type).isUInt())
        {
            /// Unsigned operands never need the floor adjustment below, and must not take it:
            /// its `minus` would flip the result to a signed type.
            const size_t quotient = plan.step("intDiv", {value_slot, bin_slot});
            rounded = plan.step("multiply", {quotient, bin_slot});
        }
        else if (intervals || (isInteger(value_type) && isInteger(bin_type)))
        {
            /// `intDiv` truncates toward zero, so when the division is inexact and the operands'
            /// signs differ, the truncated quotient sits one bin above the floor.
            const size_t quotient = plan.step("intDiv", {value_slot, bin_slot});
            const size_t remainder = plan.step("modulo", {value_slot, bin_slot});
            const size_t inexact = plan.step("notEquals", {remainder, zero});
            const size_t value_negative = plan.step("less", {value_slot, zero});
            const size_t bin_negative = plan.step("less", {bin_slot, zero});
            const size_t signs_differ = plan.step("notEquals", {value_negative, bin_negative});
            const size_t adjust = plan.step("and", {inexact, signs_differ});
            const size_t floored = plan.step("minus", {quotient, adjust});
            rounded = plan.step("multiply", {floored, bin_slot});
        }
        else
        {
            const size_t quotient = plan.step("divide", {value_slot, bin_slot});
            const size_t floored = plan.step("floor", {quotient});
            rounded = plan.step("multiply", {floored, bin_slot});
        }

        if (intervals)
            plan.step(result_kind.toNameOfFunctionToIntervalDataType(), {rounded});

        return std::move(plan).finish(name, arguments);
    }
};

}

REGISTER_FUNCTION(KQLBin)
{
    FunctionDocumentation bin_documentation{
        .description = R"(
Rounds a value down to a multiple of `roundTo`, as the Kusto Query Language's `bin()` does.

The rule depends on the argument types: a number is rounded arithmetically, a timespan (which
is an `Interval`) is rounded by a timespan, and a datetime is rounded by a timespan. A KQL
datetime is a `DateTime64`; the narrower `DateTime` and `Date` carriers are rejected, because
they cannot represent every bin a KQL datetime can produce.

This function backs `bin()` when `dialect = 'kusto'`. It is not meant to be called directly
from SQL.
)",
        .syntax = "kqlBin(value, roundTo)",
        .arguments = {{"value", "A number, a timespan, or a datetime (a `DateTime64`)."}, {"roundTo", "The bin size."}},
        .returned_value = {"`value` rounded down to the nearest multiple of `roundTo`."},
        .examples
        = {{"number", "SELECT kqlBin(4.5, 1)", "4"},
           {"timespan", "SELECT kqlBin(toIntervalNanosecond(16 * 86400000000000), toIntervalNanosecond(7 * 86400000000000))", "1209600000000000"},
           {"datetime", "SELECT kqlBin(toDateTime64('2026-08-01 12:34:56', 7, 'UTC'), toIntervalHour(1))", "2026-08-01 12:00:00.0000000"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLBinOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLBinOverloadResolver::create(std::move(context)); },
        bin_documentation);
}

}
