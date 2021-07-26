#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

DataTypePtr IAggregateFunction::getStateType() const
{
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), argument_types, parameters);
}

}
