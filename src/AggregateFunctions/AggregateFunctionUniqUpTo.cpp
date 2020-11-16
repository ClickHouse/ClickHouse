#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionUniqUpTo.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{

constexpr UInt8 uniq_upto_max_threshold = 100;


AggregateFunctionPtr createAggregateFunctionUniqUpTo(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    UInt8 threshold = 5;    /// default value

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 threshold_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold_param > uniq_upto_max_threshold)
            throw Exception("Too large parameter for aggregate function " + name + ". Maximum: " + toString(uniq_upto_max_threshold),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = threshold_param;
    }

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniqUpTo>(*argument_types[0], threshold, argument_types, params));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDate::FieldType>>(threshold, argument_types, params);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDateTime::FieldType>>(threshold, argument_types, params);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniqUpTo<String>>(threshold, argument_types, params);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeUUID::FieldType>>(threshold, argument_types, params);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqUpToVariadic<true, true>>(argument_types, params, threshold);
            else
                return std::make_shared<AggregateFunctionUniqUpToVariadic<false, true>>(argument_types, params, threshold);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqUpToVariadic<true, false>>(argument_types, params, threshold);
    else
        return std::make_shared<AggregateFunctionUniqUpToVariadic<false, false>>(argument_types, params, threshold);
}

}

void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqUpTo", {createAggregateFunctionUniqUpTo, {true}});
}

}
