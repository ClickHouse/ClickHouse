#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/scope_guard_safe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
  * So, result of function depends on partition of data to columnss and on order of data in columns.
  */
class FunctionRunningAccumulate : public IFunction
{
public:
    static constexpr auto name = "runningAccumulate";
    static FunctionPtr create(ContextPtr)
    {
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

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception("Incorrect number of arguments of function " + getName() + ". Must be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnAggregateFunction * column_with_states
            = typeid_cast<const ColumnAggregateFunction *>(&*arguments.at(0).column);

        if (!column_with_states)
            throw Exception("Illegal column " + arguments.at(0).column->getName()
                    + " of first argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        ColumnPtr column_with_groups;

        if (arguments.size() == 2)
            column_with_groups = arguments[1].column;

        AggregateFunctionPtr aggregate_function_ptr = column_with_states->getAggregateFunction();
        const IAggregateFunction & agg_func = *aggregate_function_ptr;

        AlignedBuffer place(agg_func.sizeOfData(), agg_func.alignOfData());

        /// Will pass empty arena if agg_func does not allocate memory in arena
        std::unique_ptr<Arena> arena = agg_func.allocatesMemoryInArena() ? std::make_unique<Arena>() : nullptr;

        auto result_column_ptr = agg_func.getReturnType()->createColumn();
        IColumn & result_column = *result_column_ptr;
        result_column.reserve(column_with_states->size());

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
                state_created = true; //-V519
            }

            agg_func.merge(place.data(), state_to_add, arena.get());
            agg_func.insertResultInto(place.data(), result_column, arena.get());

            ++row_number;
        }

        return result_column_ptr;
    }
};

}

REGISTER_FUNCTION(RunningAccumulate)
{
    factory.registerFunction<FunctionRunningAccumulate>();
}

}
