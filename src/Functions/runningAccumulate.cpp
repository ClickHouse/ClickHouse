#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/scope_guard_safe.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_error_prone_window_functions;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int DEPRECATED_FUNCTION;
}

namespace
{

/** runningAccumulate(agg_state) - takes the states of the aggregate function and returns a column with values,
  * are the result of the accumulation of these states for a set of columns lines, from the first to the current line.
  *
  * Quite unusual function.
  * Takes state of aggregate function (example runningAccumulate(uniqState(UserID))),
  *  and for each row of columns, return result of aggregate function on merge of states of all previous rows and current row.
  *
  * So, result of function depends on partition of data to columns and on order of data in columns.
  */
class FunctionRunningAccumulate : public IFunction
{
public:
    static constexpr auto name = "runningAccumulate";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_deprecated_error_prone_window_functions])
            throw Exception(
                ErrorCodes::DEPRECATED_FUNCTION,
                "Function {} is deprecated since its usage is error-prone (see docs)."
                "Please use proper window function or set `allow_deprecated_error_prone_window_functions` setting to enable it",
                name);

        return std::make_shared<FunctionRunningAccumulate>();
    }

    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments of function {}. Must be 1 or 2.", getName());

        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument for function {} must have type AggregateFunction - state "
                            "of aggregate function.", getName());

        return type->getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnAggregateFunction * column_with_states
            = typeid_cast<const ColumnAggregateFunction *>(&*arguments.at(0).column);

        if (!column_with_states)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    arguments.at(0).column->getName(), getName());

        ColumnPtr column_with_groups;

        if (arguments.size() == 2)
            column_with_groups = arguments[1].column;

        AggregateFunctionPtr aggregate_function_ptr = column_with_states->getAggregateFunction();
        const IAggregateFunction & agg_func = *aggregate_function_ptr;

        AlignedBuffer place(agg_func.sizeOfData(), agg_func.alignOfData());

        /// If the aggregate function returns its own state (i.e. it is wrapped in
        /// `-State`, possibly through `-OrDefault`/`-OrNull`/`-If`/`-ForEach`/etc.),
        /// `AggregateFunctionState::insertResultInto` (called from below or from a
        /// wrapping combinator) pushes raw state pointers into a
        /// `ColumnAggregateFunction`. The accumulator `place` lives on the stack,
        /// so we cannot push pointers into it (use-after-free at result-column
        /// destruction time) — and even if it survived, every row would alias the
        /// *final* accumulator. To support this case we allocate a fresh per-row
        /// state slot inside an `Arena`, deep-copy the running accumulator into
        /// the slot, and call `insertResultInto` on the slot. The arena is
        /// attached to every `ColumnAggregateFunction` reachable inside the
        /// result column tree (e.g. through `ColumnArray` for the `-ForEach`
        /// case) via `addArena`, so its lifetime extends until those columns
        /// are destroyed.
        const bool returns_state = agg_func.isState();

        /// Will pass empty arena if agg_func does not allocate memory in arena
        /// and does not return a state. When it returns a state, we also keep
        /// the arena around for the same lifetime reason — auxiliary allocations
        /// inside merged states (e.g. arena-allocated strings) would otherwise
        /// dangle.
        ArenaPtr arena = (returns_state || agg_func.allocatesMemoryInArena()) ? std::make_shared<Arena>() : nullptr;

        auto result_column_ptr = agg_func.getResultType()->createColumn();
        IColumn & result_column = *result_column_ptr;
        result_column.reserve(column_with_states->size());

        if (returns_state)
        {
            /// The result column for state-returning aggregates can be either
            /// a `ColumnAggregateFunction` directly (plain `-State`) or contain
            /// one inside (`-ForEach`/`-Array`/etc. wrap it in a `ColumnArray`,
            /// other combinators may wrap further). Attach the arena to every
            /// `ColumnAggregateFunction` we find so that all paths that own
            /// pushed state pointers keep their backing storage alive.
            auto attach_arena = [&arena](IColumn & col)
            {
                if (auto * agg_col = typeid_cast<ColumnAggregateFunction *>(&col))
                    agg_col->addArena(arena);
            };
            attach_arena(result_column);
            result_column.forEachMutableSubcolumnRecursively(attach_arena);
        }

        const auto & states = column_with_states->getData();

        bool state_created = false;
        SCOPE_EXIT_MEMORY_SAFE({
            if (state_created)
                agg_func.destroy(place.data());
        });

        size_t row_number = 0;
        for (const auto & state_to_add : states)
        {
            if (row_number == 0 || (column_with_groups && column_with_groups->compareAt(row_number, row_number - 1, *column_with_groups, 1) != 0))
            {
                if (state_created)
                {
                    agg_func.destroy(place.data());
                    state_created = false;
                }

                agg_func.create(place.data()); /// This function can throw.
                state_created = true;
            }

            agg_func.merge(place.data(), state_to_add, arena.get());

            if (returns_state)
            {
                /// Snapshot the running accumulator into a fresh arena-backed
                /// slot, then ask `agg_func` to materialise the result from it.
                /// After a successful `insertResultInto` the result column tree
                /// owns whatever was pushed (either `row_state` itself for plain
                /// `-State`, or sub-elements of `row_state` for `-ForEach`), so
                /// we do not destroy `row_state` on the success path.
                AggregateDataPtr row_state = arena->alignedAlloc(
                    agg_func.sizeOfData(), agg_func.alignOfData());
                agg_func.create(row_state); /// This function can throw.
                try
                {
                    agg_func.merge(row_state, place.data(), arena.get());
                    agg_func.insertResultInto(row_state, result_column, arena.get());
                }
                catch (...)
                {
                    agg_func.destroy(row_state);
                    throw;
                }
            }
            else
            {
                agg_func.insertResultInto(place.data(), result_column, arena.get());
            }

            ++row_number;
        }

        return result_column_ptr;
    }
};

}

REGISTER_FUNCTION(RunningAccumulate)
{
    FunctionDocumentation::Description description = R"(
Accumulates the states of an aggregate function for each row of a data block.

:::warning Deprecated
The state is reset for each new block of data.
Due to this error-prone behavior the function has been deprecated, and you are advised to use [window functions](/sql-reference/window-functions) instead.
You can use setting [`allow_deprecated_error_prone_window_functions`](/operations/settings/settings#allow_deprecated_error_prone_window_functions) to allow usage of this function.
:::
)";
    FunctionDocumentation::Syntax syntax = "runningAccumulate(agg_state[, grouping])";
    FunctionDocumentation::Arguments arguments = {
        {"agg_state", "State of the aggregate function.", {"AggregateFunction"}},
        {"grouping", "Optional. Grouping key. The state of the function is reset if the `grouping` value is changed. It can be any of the supported data types for which the equality operator is defined.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the accumulated result for each row.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example with initializeAggregation",
        R"(
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(5);
        )",
        R"(
┌─number─┬─one_row_sum─┬─cumulative_sum─┐
│      0 │           0 │              0 │
│      1 │           1 │              1 │
│      2 │           2 │              3 │
│      3 │           3 │              6 │
│      4 │           4 │             10 │
└────────┴─────────────┴────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRunningAccumulate>(documentation);
}

}
