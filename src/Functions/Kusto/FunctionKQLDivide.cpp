#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLPlan.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** `kqlDivide(x, y)` - the `/` operator of the Kusto dialect.
  *
  * KQL divides two integers as integers: `7 / 2` is 3, not 3.5. Which rule applies depends on
  * the operand *types*, which a parser translating text to text cannot see - the previous KQL
  * implementation emitted a plain `divide` and silently answered 3.5.
  *
  * This is an overload *resolver* rather than a function: the choice depends only on the
  * argument types, so it is made once while the query is analysed, and the chosen function
  * then runs with no further dispatch.
  */
class FunctionKQLDivideOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlDivide";

    explicit FunctionKQLDivideOverloadResolver(ContextPtr context_) : WithContext(context_) { }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLDivideOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
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
    /// Integer over integer keeps the KQL rule; a timespan over a timespan is a ratio;
    /// anything else divides as usual.
    FunctionBasePtr delegate(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 2 arguments", getName());

        const auto * value_interval = typeid_cast<const DataTypeInterval *>(removeNullable(arguments[0].type).get());
        const auto * divisor_interval = typeid_cast<const DataTypeInterval *>(removeNullable(arguments[1].type).get());
        if (value_interval && divisor_interval)
            return divideIntervals(arguments, value_interval->getKind(), divisor_interval->getKind());

        const bool both_integral
            = isInteger(removeNullable(arguments[0].type)) && isInteger(removeNullable(arguments[1].type));
        return FunctionFactory::instance().get(both_integral ? "intDiv" : "divide", getContext())->build(arguments);
    }

    /// A timespan divided by a timespan is their real-valued ratio (`15ms / 10ms` is `1.5`):
    /// an `Interval` column is a plain `Int64` column, so retyping the argument slots turns
    /// the case into `divide` over the intervals' ticks. Equal kinds cancel out as they are;
    /// unequal fixed-length kinds are normalized to nanoseconds first.
    FunctionBasePtr divideIntervals(const ColumnsWithTypeAndName & arguments, IntervalKind value_kind, IntervalKind divisor_kind) const
    {
        KQLPlanBuilder plan(getContext());

        const auto retyped_as_ticks = [](const DataTypePtr & original) -> DataTypePtr
        {
            const DataTypePtr ticks = std::make_shared<DataTypeInt64>();
            return original->isNullable() ? makeNullable(ticks) : ticks;
        };

        size_t value_slot = plan.argument(retyped_as_ticks(arguments[0].type));
        size_t divisor_slot = plan.argument(retyped_as_ticks(arguments[1].type));

        if (value_kind != divisor_kind)
        {
            if (!value_kind.isFixedLength() || !divisor_kind.isFixedLength())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} cannot divide {} by {}: the kinds differ and not both have a fixed length",
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
            divisor_slot = in_nanoseconds(divisor_slot, divisor_kind);
        }

        plan.step("divide", {value_slot, divisor_slot});
        return std::move(plan).finish(name, arguments);
    }
};

}

REGISTER_FUNCTION(KQLDivide)
{
    FunctionDocumentation documentation{
        .description = R"(
Division as the Kusto Query Language defines it: two integer operands divide to an integer,
so `7 / 2` is `3`, and two timespan operands (which are `Interval` values) divide to their
real-valued ratio, so `15ms / 10ms` is `1.5`. Any other combination of operand types divides
as [`divide`](#divide) does.

This function backs the `/` operator when `dialect = 'kusto'`. It is not meant to be called
directly from SQL.
)",
        .syntax = "kqlDivide(x, y)",
        .arguments = {{"x", "The dividend."}, {"y", "The divisor."}},
        .returned_value
        = {"`intDiv(x, y)` when both arguments are integers, the ratio of the intervals' ticks when both are intervals, "
           "`divide(x, y)` otherwise."},
        .examples
        = {{"integers", "SELECT kqlDivide(7, 2)", "3"},
           {"reals", "SELECT kqlDivide(7.0, 2)", "3.5"},
           {"timespans", "SELECT kqlDivide(toIntervalNanosecond(15000000), toIntervalNanosecond(10000000))", "1.5"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLDivideOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLDivideOverloadResolver::create(std::move(context)); },
        documentation);
}

}
