#include <AggregateFunctions/AggregateFunctionUniqCombined.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqCombined(bool use_64_bit_hash,
    const std::string & name, const DataTypes & argument_types, const Array & params)
{
    /// log2 of the number of cells in HyperLogLog.
    /// Reasonable default value, selected to be comparable in quality with "uniq" aggregate function.
    UInt8 precision = 17;

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one parameter or less.",
                name);

        UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        // This range is hardcoded below
        if (precision_param > 20 || precision_param < 12)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter for aggregate function {} is out of range: [12, 20].",
                name);
        precision = precision_param;
    }

    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments for aggregate function {}", name);

    switch (precision) // NOLINT(bugprone-switch-missing-default-case)
    {
        case 12:
            return createAggregateFunctionWithHashType<12>(use_64_bit_hash, argument_types, params);
        case 13:
            return createAggregateFunctionWithHashType<13>(use_64_bit_hash, argument_types, params);
        case 14:
            return createAggregateFunctionWithHashType<14>(use_64_bit_hash, argument_types, params);
        case 15:
            return createAggregateFunctionWithHashType<15>(use_64_bit_hash, argument_types, params);
        case 16:
            return createAggregateFunctionWithHashType<16>(use_64_bit_hash, argument_types, params);
        case 17:
            return createAggregateFunctionWithHashType<17>(use_64_bit_hash, argument_types, params);
        case 18:
            return createAggregateFunctionWithHashType<18>(use_64_bit_hash, argument_types, params);
        case 19:
            return createAggregateFunctionWithHashType<19>(use_64_bit_hash, argument_types, params);
        case 20:
            return createAggregateFunctionWithHashType<20>(use_64_bit_hash, argument_types, params);
    }

    UNREACHABLE();
}

}

void registerAggregateFunctionUniqCombined(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqCombined",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return createAggregateFunctionUniqCombined(false, name, argument_types, parameters);
        });
    factory.registerFunction("uniqCombined64",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return createAggregateFunctionUniqCombined(true, name, argument_types, parameters);
        });
}

}
