#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

namespace
{

/// Substitute return type for Date and DateTime
class AggregateFunctionGroupUniqArrayDate : public AggregateFunctionGroupUniqArray<DataTypeDate::FieldType>
{
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

class AggregateFunctionGroupUniqArrayDateTime : public AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType>
{
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};


static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type)
{
         if (typeid_cast<const DataTypeDate *>(argument_type.get())) return new AggregateFunctionGroupUniqArrayDate;
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get())) return new AggregateFunctionGroupUniqArrayDateTime;
    else
    {
        /// Check that we can use plain version of AggreagteFunctionGroupUniqArrayGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggreagteFunctionGroupUniqArrayGeneric<true>(argument_type);
        else
            return new AggreagteFunctionGroupUniqArrayGeneric<false>(argument_type);
    }
}

AggregateFunctionPtr createAggregateFunctionGroupUniqArray(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupUniqArray>(*argument_types[0]));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes(argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() +
            " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupUniqArray", createAggregateFunctionGroupUniqArray);
}

}
