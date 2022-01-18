#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

DataTypePtr IAggregateFunction::getStateType() const
{
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), argument_types, parameters);
}

String IAggregateFunction::getDescription() const
{
    String description;

    description += getName();

    description += '(';

    for (const auto & parameter : parameters)
    {
        description += parameter.dump();
        description += ", ";
    }

    if (!parameters.empty())
    {
        description.pop_back();
        description.pop_back();
    }

    description += ')';

    description += '(';

    for (const auto & argument_type : argument_types)
    {
        description += argument_type->getName();
        description += ", ";
    }

    if (!argument_types.empty())
    {
        description.pop_back();
        description.pop_back();
    }

    description += ')';

    return description;
}
}
