#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTopK.h>
#include <AggregateFunctions/Helpers.h>
#include <memory>

namespace DB
{

namespace
{

/// Substitute return type for Date and DateTime
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType>
{
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType>
{
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};


static IAggregateFunction * createWithExtraTypes(const DataTypes & argument_types)
{
    const IDataType& argument_type = *argument_types[0];

         if (typeid_cast<const DataTypeDate *>(&argument_type))     return new AggregateFunctionTopKDate;
    else if (typeid_cast<const DataTypeDateTime *>(&argument_type))    return new AggregateFunctionTopKDateTime;
    else
    {
        std::unique_ptr<IAggregateFunction> fun;

        /// Check that we can use plain version of AggregateFunctionTopKGeneric
        if (argument_type.isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            fun.reset(new AggregateFunctionTopKGeneric<true>);
        else
            fun.reset(new AggregateFunctionTopKGeneric<false>);

        fun->setArguments(argument_types);
        return fun.release();
    }
}

AggregateFunctionPtr createAggregateFunctionTopK(const std::string & name, const DataTypes & argument_types, const Array & /*parameters*/)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK>(*argument_types[0]));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes(argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() +
            " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionTopK(AggregateFunctionFactory & factory)
{
    factory.registerFunction("topK", createAggregateFunctionTopK);
}

}
