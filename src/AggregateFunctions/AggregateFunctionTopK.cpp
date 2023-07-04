#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTopK.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


static inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
IAggregateFunction * createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, UInt64 load_factor, const Array & params)
{
    if (argument_types.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

    WhichDataType which(argument_types[0]);
    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTopKDate<is_weighted>(threshold, load_factor, argument_types, params);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTopKDateTime<is_weighted>(threshold, load_factor, argument_types, params);

    /// Check that we can use plain version of AggregateFunctionTopKGeneric
    if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionTopKGeneric<true, is_weighted>(threshold, load_factor, argument_types, params);
    else
        return new AggregateFunctionTopKGeneric<false, is_weighted>(threshold, load_factor, argument_types, params);
}


template <bool is_weighted>
AggregateFunctionPtr createAggregateFunctionTopK(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Aggregate function '{}' requires two parameters or less", name);

        if (params.size() == 2)
        {
            load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

            if (load_factor < 1)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "Too small parameter 'load_factor' for aggregate function '{}' (got {}, minimum is 1)", name, load_factor);
        }

        threshold = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold > TOP_K_MAX_SIZE || load_factor > TOP_K_MAX_SIZE || threshold * load_factor > TOP_K_MAX_SIZE)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Too large parameter(s) for aggregate function '{}' (maximum is {})", name, toString(TOP_K_MAX_SIZE));

        if (threshold == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function '{}'", name);
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK, is_weighted>(
        *argument_types[0], threshold, load_factor, argument_types, params));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<is_weighted>(argument_types, threshold, load_factor, params));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function '{}'", argument_types[0]->getName(), name);
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
