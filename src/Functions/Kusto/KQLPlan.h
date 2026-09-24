#pragma once

#include <Columns/ColumnConst.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/** Runs a KQL scalar function as a short, pre-built plan over already-resolved functions: each
  * step executes one function over earlier slots and appends its result as a new slot. Holding
  * the already-built functions keeps the per-block work down to executing them.
  *
  * An argument slot may carry a different type than the argument feeding it, as long as the
  * physical column matches: a KQL timespan is an `Interval` stored as a plain `Int64` column,
  * so retyping its slot as `Int64` turns interval arithmetic into the integer arithmetic the
  * delegated functions know.
  */
class FunctionKQLPlan final : public IFunctionBase
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

    FunctionKQLPlan(String name_, VectorWithMemoryTracking<Slot> slots_, VectorWithMemoryTracking<Step> steps_, DataTypes argument_types_)
        : name(std::move(name_))
        , slots(std::move(slots_))
        , steps(std::move(steps_))
        , argument_types(std::move(argument_types_))
    {
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return slots[steps.back().result].type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return steps.front().function->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override { return std::make_unique<Executable>(name, slots, steps); }

private:
    class Executable final : public IExecutableFunction
    {
    public:
        Executable(String name_, VectorWithMemoryTracking<Slot> slots_, VectorWithMemoryTracking<Step> steps_)
            : name(std::move(name_))
            , slots(std::move(slots_))
            , steps(std::move(steps_))
        {
        }

        String getName() const override { return name; }

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
                /// An argument column flows in under the slot's type, which may retype it.
                if (i < arguments.size())
                    columns[i] = ColumnWithTypeAndName{arguments[i].column, slots[i].type, arguments[i].name};
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
        String name;
        VectorWithMemoryTracking<Slot> slots;
        VectorWithMemoryTracking<Step> steps;
    };

    String name;
    VectorWithMemoryTracking<Slot> slots;
    VectorWithMemoryTracking<Step> steps;
    DataTypes argument_types;
};

/// Assembles a `FunctionKQLPlan`. The arguments become the first slots (retyped if asked),
/// `constant` and `step` append further ones, and the last step's result is the plan's result.
class KQLPlanBuilder
{
public:
    explicit KQLPlanBuilder(ContextPtr context_)
        : context(std::move(context_))
    {
    }

    size_t argument(DataTypePtr type)
    {
        slots.push_back({std::move(type), {}});
        return slots.size() - 1;
    }

    size_t constant(DataTypePtr type, Field value)
    {
        slots.push_back({std::move(type), std::move(value)});
        return slots.size() - 1;
    }

    size_t step(const String & function_name, VectorWithMemoryTracking<size_t> step_arguments)
    {
        ColumnsWithTypeAndName built_over;
        for (size_t argument : step_arguments)
        {
            const auto & slot = slots[argument];
            ColumnPtr column;
            /// A constant is visible at build time - some delegates require that (an interval
            /// unit, a `NULL` literal short-circuit) - and a synthetic `Nullable(Nothing)`
            /// intermediate needs a materialized column for the short circuit to fire.
            if (slot.constant)
                column = slot.type->createColumnConst(1, *slot.constant);
            else if (isNothing(removeNullable(slot.type)))
                column = slot.type->createColumnConstWithDefaultValue(1);
            built_over.push_back(ColumnWithTypeAndName{column, slot.type, ""});
        }
        auto function = FunctionFactory::instance().get(function_name, context)->build(built_over);
        slots.push_back({function->getResultType(), {}});
        steps.push_back({std::move(function), std::move(step_arguments), slots.size() - 1});
        return slots.size() - 1;
    }

    const DataTypePtr & slotType(size_t slot) const { return slots[slot].type; }

    FunctionBasePtr finish(String name, const ColumnsWithTypeAndName & arguments) &&
    {
        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);
        return std::make_shared<FunctionKQLPlan>(std::move(name), std::move(slots), std::move(steps), std::move(argument_types));
    }

private:
    ContextPtr context;
    VectorWithMemoryTracking<FunctionKQLPlan::Slot> slots;
    VectorWithMemoryTracking<FunctionKQLPlan::Step> steps;
};

}
