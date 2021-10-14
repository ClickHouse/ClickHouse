#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTopK.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include "registerAggregateFunctions.h"

#define TOP_K_MAX_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace
{

/// Substitute return type for Date and DateTime
template <bool is_weighted>
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>
{
    using AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <bool is_weighted>
class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>
{
    using AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};


template <bool is_weighted>
static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, UInt64 threshold, UInt64 load_factor, const Array & params)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTopKDate<is_weighted>(threshold, load_factor, {argument_type}, params);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTopKDateTime<is_weighted>(threshold, load_factor, {argument_type}, params);

    /// Check that we can use plain version of AggregateFunctionTopKGeneric
    if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionTopKGeneric<true, is_weighted>(threshold, load_factor, argument_type, params);
    else
        return new AggregateFunctionTopKGeneric<false, is_weighted>(threshold, load_factor, argument_type, params);
}


template <bool is_weighted>
AggregateFunctionPtr createAggregateFunctionTopK(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    if (!is_weighted)
    {
        assertUnary(name, argument_types);
    }
    else
    {
        assertBinary(name, argument_types);
        if (!isInteger(argument_types[1]))
            throw Exception("The second argument for aggregate function 'topKWeighted' must have integer type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    UInt64 threshold = 10;  /// default values
    UInt64 load_factor = 3;

    if (!params.empty())
    {
        if (params.size() > 2)
            throw Exception("Aggregate function " + name + " requires two parameters or less.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        if (params.size() == 2)
        {
            load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

            if (load_factor < 1)
                throw Exception("Too small parameter 'load_factor' for aggregate function " + name + ". Minimum: 1",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (k > TOP_K_MAX_SIZE || load_factor > TOP_K_MAX_SIZE || k * load_factor > TOP_K_MAX_SIZE)
            throw Exception("Too large parameter(s) for aggregate function " + name + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (k == 0)
            throw Exception("Parameter 0 is illegal for aggregate function " + name,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = k;
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK, is_weighted>(
        *argument_types[0], threshold, load_factor, argument_types, params));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<is_weighted>(argument_types[0], threshold, load_factor, params));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() +
            " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionTopK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("topK", { createAggregateFunctionTopK<false>, properties });
    factory.registerFunction("topKWeighted", { createAggregateFunctionTopK<true>, properties });
}

}
