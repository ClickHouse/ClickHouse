#include <AggregateFunctions/AggregateFunctionUniqCombined.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqCombined(
    const std::string & name, const DataTypes & argument_types, const Array & params)
{
    UInt8 precision = detail::UNIQ_COMBINED_DEFAULT_PRECISION;

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception(
                "Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        // This range is hardcoded into |AggregateFunctionUniqCombinedData|
        if (precision_param > 20 || precision_param < 12)
            throw Exception(
                "Parameter for aggregate function " + name + "is out or range: [12, 20].", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        precision = precision_param;
    }

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// We use exact hash function if the user wants it;
    /// or if the arguments are not contiguous in memory, because only exact hash function have support for this case.
    bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniqCombined>(*argument_types[0], precision));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniqCombined<DataTypeDate::FieldType>>(precision);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniqCombined<DataTypeDateTime::FieldType>>(precision);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniqCombined<String>>(precision);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniqCombined<DataTypeUUID::FieldType>>(precision);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqCombinedVariadic<true, true>>(argument_types, precision);
            else
                return std::make_shared<AggregateFunctionUniqCombinedVariadic<false, true>>(argument_types, precision);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqCombinedVariadic<true, false>>(argument_types, precision);
    else
        return std::make_shared<AggregateFunctionUniqCombinedVariadic<false, false>>(argument_types, precision);
}

} // namespace

void registerAggregateFunctionUniqCombined(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqCombined", createAggregateFunctionUniqCombined);
}

} // namespace DB
