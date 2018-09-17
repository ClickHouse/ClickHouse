#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTopK.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#define TOP_K_MAX_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{

/// Substitute return type for Date and DateTime
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType>
{
    using AggregateFunctionTopK<DataTypeDate::FieldType>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType>
{
    using AggregateFunctionTopK<DataTypeDateTime::FieldType>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};


static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, UInt64 threshold)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTopKDate(threshold);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTopKDateTime(threshold);

    /// Check that we can use plain version of AggregateFunctionTopKGeneric
    if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionTopKGeneric<true>(threshold, argument_type);
    else
        return new AggregateFunctionTopKGeneric<false>(threshold, argument_type);
}

AggregateFunctionPtr createAggregateFunctionTopK(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);

    UInt64 threshold = 10;  /// default value

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (k > TOP_K_MAX_SIZE)
            throw Exception("Too large parameter for aggregate function " + name + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (k == 0)
            throw Exception("Parameter 0 is illegal for aggregate function " + name,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = k;
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK>(*argument_types[0], threshold));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes(argument_types[0], threshold));

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
