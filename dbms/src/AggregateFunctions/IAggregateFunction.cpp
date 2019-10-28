#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

std::shared_ptr<IAggregateFunction> IAggregateFunction::getInstance() const
{
    return AggregateFunctionFactory::instance().get(getName(), argument_types, parameters);
}

String IAggregateFunction::getNameWithState() const
{
    auto name = getName();
    if (is_state)
        name += "State";
    return name;
}

DataTypePtr IAggregateFunction::getReturnTypeWithState() const
{
    auto return_type = getReturnType();
    if (!is_state)
        return return_type;

    auto func_without_state = getInstance();
    auto ptr = std::make_shared<DataTypeAggregateFunction>(func_without_state, argument_types, parameters);

    /// Special case: it is -MergeState combinator.
    /// We must return AggregateFunction(agg, ...) instead of AggregateFunction(aggMerge, ...)
    if (typeid_cast<const AggregateFunctionMerge *>(ptr->getFunction().get()))
    {
        if (this->argument_types.size() != 1)
            throw Exception("Combinator -MergeState expects only one argument", ErrorCodes::BAD_ARGUMENTS);

        if (!typeid_cast<const DataTypeAggregateFunction *>(this->argument_types[0].get()))
            throw Exception("Combinator -MergeState expects argument with AggregateFunction type", ErrorCodes::BAD_ARGUMENTS);

        return this->argument_types[0];
    }
    if (this->argument_types.size() > 0)
    {
        DataTypePtr argument_type_ptr = this->argument_types[0];
        WhichDataType which(*argument_type_ptr);
        if (which.idx == TypeIndex::AggregateFunction)
        {
            if (this->argument_types.size() != 1)
                throw Exception("Nested aggregation expects only one argument", ErrorCodes::BAD_ARGUMENTS);
            return this->argument_types[0];
        }
    }

    return ptr;
}

void IAggregateFunction::insertResultIntoWithState(ConstAggregateDataPtr place, IColumn & to) const
{
    if (is_state)
        assert_cast<ColumnAggregateFunction &>(to).getData().push_back(const_cast<AggregateDataPtr>(place));
    else
        insertResultInto(place, to);
}

}
