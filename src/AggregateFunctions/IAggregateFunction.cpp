#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

DataTypePtr IAggregateFunction::getStateType() const
{
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), argument_types, parameters);
}

DataTypePtr IAggregateFunction::getNormalizedStateType() const
{
    DataTypes normalized_argument_types;
    normalized_argument_types.reserve(argument_types.size());
    for (const auto & arg : argument_types)
        normalized_argument_types.emplace_back(arg->getNormalizedType());
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), normalized_argument_types, parameters);
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

bool IAggregateFunction::haveEqualArgumentTypes(const IAggregateFunction & rhs) const
{
    return std::equal(
        argument_types.begin(),
        argument_types.end(),
        rhs.argument_types.begin(),
        rhs.argument_types.end(),
        [](const auto & t1, const auto & t2) { return t1->equals(*t2); });
}

bool IAggregateFunction::haveSameStateRepresentation(const IAggregateFunction & rhs) const
{
    const auto & lhs_base = getBaseAggregateFunctionWithSameStateRepresentation();
    const auto & rhs_base = rhs.getBaseAggregateFunctionWithSameStateRepresentation();
    return lhs_base.haveSameStateRepresentationImpl(rhs_base);
}

bool IAggregateFunction::haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const
{
    return getStateType()->equals(*rhs.getStateType());
}

}
