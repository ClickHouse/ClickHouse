#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Runs the numeric case as a short, pre-built plan over already-resolved functions: each step
/// executes one function over earlier slots and appends its result as a new slot. Holding the
/// already-built functions keeps the per-block work down to executing them.
class FunctionKQLBinNumeric final : public IFunctionBase
{
public:
    /// A slot is one intermediate value: an argument, a constant, or a step's result.
    struct Slot
    {
        DataTypePtr type;
        /// Set for constant slots only; the column materializes at execution time.
        std::optional<Field> constant;
    };

    struct Step
    {
        FunctionBasePtr function;
        VectorWithMemoryTracking<size_t> arguments;
        size_t result;
    };

    FunctionKQLBinNumeric(VectorWithMemoryTracking<Slot> slots_, VectorWithMemoryTracking<Step> steps_, DataTypes argument_types_)
        : slots(std::move(slots_))
        , steps(std::move(steps_))
        , argument_types(std::move(argument_types_))
    {
    }

    String getName() const override { return "kqlBin"; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return slots[steps.back().result].type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return steps.front().function->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override { return std::make_unique<Executable>(slots, steps); }

private:
    class Executable final : public IExecutableFunction
    {
    public:
        Executable(VectorWithMemoryTracking<Slot> slots_, VectorWithMemoryTracking<Step> steps_)
            : slots(std::move(slots_))
            , steps(std::move(steps_))
        {
        }

        String getName() const override { return "kqlBin"; }

    protected:
        /// The delegated functions were built over the original argument types, `Nullable` and
        /// all, and each handles its own nulls. Stripping `Nullable` here would hand them columns
        /// that no longer match what they were built for.
        bool useDefaultImplementationForNulls() const override { return false; }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            ColumnsWithTypeAndName columns(slots.size());
            for (size_t i = 0; i < slots.size(); ++i)
            {
                if (i < arguments.size())
                    columns[i] = arguments[i];
                else if (slots[i].constant)
                    columns[i]
                        = ColumnWithTypeAndName{slots[i].type->createColumnConst(input_rows_count, *slots[i].constant), slots[i].type, ""};
            }

            for (const auto & step : steps)
            {
                ColumnsWithTypeAndName step_arguments;
                for (size_t argument : step.arguments)
                    step_arguments.push_back(columns[argument]);
                const DataTypePtr & step_type = &step == &steps.back() ? result_type : slots[step.result].type;
                columns[step.result] = ColumnWithTypeAndName{
                    step.function->execute(step_arguments, step_type, input_rows_count, /*dry_run=*/false), step_type, ""};
            }

            return columns[steps.back().result].column;
        }

    private:
        VectorWithMemoryTracking<Slot> slots;
        VectorWithMemoryTracking<Step> steps;
    };

    VectorWithMemoryTracking<Slot> slots;
    VectorWithMemoryTracking<Step> steps;
    DataTypes argument_types;
};

/** `kqlBin(value, roundTo)` - Kusto's `bin()`, rounding a value down to a multiple of `roundTo`.
  *
  * The rule reads the same in every case (`floor(value / roundTo) * roundTo`) but the way to
  * compute it is not: numbers divide, and a datetime has to be rounded by an interval. Only
  * the argument types say which applies, so the decision is made here rather than guessed
  * from how the argument was spelled - the previous KQL implementation compared the first
  * *token* against the text "datetime", so `bin(Timestamp, 1d)` over a datetime column took
  * the numeric branch and emitted `toFloat64(Timestamp)`.
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

        /// A datetime rounded by an interval is exactly `toStartOfInterval`.
        if (isDateOrDate32OrDateTimeOrDateTime64(value_type))
        {
            if (!isInterval(bin_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime by a timespan, but the second argument has type {}",
                    getName(),
                    arguments[1].type->getName());
            return FunctionFactory::instance().get("toStartOfInterval", getContext())->build(arguments);
        }

        if (!isNumber(value_type) || !isNumber(bin_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects a number rounded by a number, or a datetime rounded by a timespan, got {} and {}",
                getName(),
                arguments[0].type->getName(),
                arguments[1].type->getName());

        /// Numbers: `floor(value / roundTo) * roundTo`. Integers divide exactly, so they skip
        /// the detour through floating point.
        VectorWithMemoryTracking<FunctionKQLBinNumeric::Slot> slots{{arguments[0].type, {}}, {arguments[1].type, {}}};
        VectorWithMemoryTracking<FunctionKQLBinNumeric::Step> steps;

        const auto add_step = [&](const String & function_name, VectorWithMemoryTracking<size_t> step_arguments)
        {
            ColumnsWithTypeAndName built_over;
            for (size_t argument : step_arguments)
                built_over.push_back(ColumnWithTypeAndName{nullptr, slots[argument].type, ""});
            auto function = FunctionFactory::instance().get(function_name, getContext())->build(built_over);
            slots.push_back({function->getResultType(), {}});
            steps.push_back({std::move(function), std::move(step_arguments), slots.size() - 1});
            return slots.size() - 1;
        };
        const auto add_constant = [&](const DataTypePtr & type, Field constant)
        {
            slots.push_back({type, std::move(constant)});
            return slots.size() - 1;
        };

        constexpr size_t value_slot = 0;
        constexpr size_t bin_slot = 1;
        if (WhichDataType(value_type).isUInt() && WhichDataType(bin_type).isUInt())
        {
            /// Unsigned operands never need the floor adjustment below, and must not take it:
            /// its `minus` would flip the result to a signed type.
            const size_t quotient = add_step("intDiv", {value_slot, bin_slot});
            add_step("multiply", {quotient, bin_slot});
        }
        else if (isInteger(value_type) && isInteger(bin_type))
        {
            /// `intDiv` truncates toward zero, so when the division is inexact and the operands'
            /// signs differ, the truncated quotient sits one bin above the floor.
            const size_t zero = add_constant(std::make_shared<DataTypeUInt8>(), Field(UInt64(0)));
            const size_t quotient = add_step("intDiv", {value_slot, bin_slot});
            const size_t remainder = add_step("modulo", {value_slot, bin_slot});
            const size_t inexact = add_step("notEquals", {remainder, zero});
            const size_t value_negative = add_step("less", {value_slot, zero});
            const size_t bin_negative = add_step("less", {bin_slot, zero});
            const size_t signs_differ = add_step("notEquals", {value_negative, bin_negative});
            const size_t adjust = add_step("and", {inexact, signs_differ});
            const size_t rounded = add_step("minus", {quotient, adjust});
            add_step("multiply", {rounded, bin_slot});
        }
        else
        {
            const size_t quotient = add_step("divide", {value_slot, bin_slot});
            const size_t rounded = add_step("floor", {quotient});
            add_step("multiply", {rounded, bin_slot});
        }

        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        return std::make_shared<FunctionKQLBinNumeric>(std::move(slots), std::move(steps), std::move(argument_types));
    }
};

}

REGISTER_FUNCTION(KQLBin)
{
    FunctionDocumentation bin_documentation{
        .description = R"(
Rounds a value down to a multiple of `roundTo`, as the Kusto Query Language's `bin()` does.

The rule depends on the argument types: a number is rounded arithmetically, and a datetime is
rounded by a timespan (which is an `Interval`).

This function backs `bin()` when `dialect = 'kusto'`. It is not meant to be called directly
from SQL.
)",
        .syntax = "kqlBin(value, roundTo)",
        .arguments = {{"value", "A number or a datetime."}, {"roundTo", "The bin size."}},
        .returned_value = {"`value` rounded down to the nearest multiple of `roundTo`."},
        .examples
        = {{"number", "SELECT kqlBin(4.5, 1)", "4"},
           {"datetime", "SELECT kqlBin(toDateTime('2026-08-01 12:34:56'), toIntervalHour(1))", "2026-08-01 12:00:00"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLBinOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLBinOverloadResolver::create(std::move(context)); },
        bin_documentation);
}

}
