#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** runningAccumulate(agg_state) - takes the states of the aggregate function and returns a column with values,
  * are the result of the accumulation of these states for a set of block lines, from the first to the current line.
  *
  * Quite unusual function.
  * Takes state of aggregate function (example runningAccumulate(uniqState(UserID))),
  *  and for each row of block, return result of aggregate function on merge of states of all previous rows and current row.
  *
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
class FunctionRunningAccumulate : public IFunction
{
public:
    static constexpr auto name = "runningAccumulate";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRunningAccumulate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnAggregateFunction * column_with_states
            = typeid_cast<const ColumnAggregateFunction *>(&*block.getByPosition(arguments.at(0)).column);
        if (!column_with_states)
            throw Exception("Illegal column " + block.getByPosition(arguments.at(0)).column->getName()
                    + " of first argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        AggregateFunctionPtr aggregate_function_ptr = column_with_states->getAggregateFunction();
        const IAggregateFunction & agg_func = *aggregate_function_ptr;

        AlignedBuffer place(agg_func.sizeOfData(), agg_func.alignOfData());
        agg_func.create(place.data());
        SCOPE_EXIT(agg_func.destroy(place.data()));

        std::unique_ptr<Arena> arena = agg_func.allocatesMemoryInArena() ? std::make_unique<Arena>() : nullptr;

        auto result_column_ptr = agg_func.getReturnType()->createColumn();
        IColumn & result_column = *result_column_ptr;
        result_column.reserve(column_with_states->size());

        const auto & states = column_with_states->getData();
        for (const auto & state_to_add : states)
        {
            /// Will pass empty arena if agg_func does not allocate memory in arena
            agg_func.merge(place.data(), state_to_add, arena.get());
            agg_func.insertResultInto(place.data(), result_column);
        }

        block.getByPosition(result).column = std::move(result_column_ptr);
    }
};


void registerFunctionRunningAccumulate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRunningAccumulate>();
}

}
