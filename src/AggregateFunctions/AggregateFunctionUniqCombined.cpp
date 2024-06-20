#include <AggregateFunctions/AggregateFunctionUniqCombined.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

#include <Common/FieldVisitorConvertToNumber.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>

#include <functional>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    template <UInt8 K, typename HashValueType>
    struct WithK
    {
        template <typename T>
        using AggregateFunction = AggregateFunctionUniqCombined<T, K, HashValueType>;

        template <bool is_exact, bool argument_is_tuple>
        using AggregateFunctionVariadic = AggregateFunctionUniqCombinedVariadic<is_exact, argument_is_tuple, K, HashValueType>;
    };

    template <UInt8 K, typename HashValueType>
    AggregateFunctionPtr createAggregateFunctionWithK(const DataTypes & argument_types, const Array & params)
    {
        /// We use exact hash function if the arguments are not contiguous in memory, because only exact hash function has support for this case.
        bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

        if (argument_types.size() == 1)
        {
            const IDataType & argument_type = *argument_types[0];

            AggregateFunctionPtr res(createWithNumericType<WithK<K, HashValueType>::template AggregateFunction>(*argument_types[0], argument_types, params));

            WhichDataType which(argument_type);
            if (res)
                return res;
            else if (which.isDate())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<DataTypeDate::FieldType>>(argument_types, params);
            else if (which.isDate32())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<DataTypeDate32::FieldType>>(argument_types, params);
            else if (which.isDateTime())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<DataTypeDateTime::FieldType>>(argument_types, params);
            else if (which.isStringOrFixedString())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<String>>(argument_types, params);
            else if (which.isUUID())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<DataTypeUUID::FieldType>>(argument_types, params);
            else if (which.isIPv4())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<DataTypeIPv4::FieldType>>(argument_types, params);
            else if (which.isIPv6())
                return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunction<DataTypeIPv6::FieldType>>(argument_types, params);
            else if (which.isTuple())
            {
                if (use_exact_hash_function)
                    return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunctionVariadic<true, true>>(argument_types, params);
                else
                    return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunctionVariadic<false, true>>(argument_types, params);
            }
        }

        /// "Variadic" method also works as a fallback generic case for a single argument.
        if (use_exact_hash_function)
            return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunctionVariadic<true, false>>(argument_types, params);
        else
            return std::make_shared<typename WithK<K, HashValueType>::template AggregateFunctionVariadic<false, false>>(argument_types, params);
    }

    template <UInt8 K>
    AggregateFunctionPtr createAggregateFunctionWithHashType(bool use_64_bit_hash, const DataTypes & argument_types, const Array & params)
    {
        if (use_64_bit_hash)
            return createAggregateFunctionWithK<K, UInt64>(argument_types, params);
        else
            return createAggregateFunctionWithK<K, UInt32>(argument_types, params);
    }

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

        switch (precision)
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
    using namespace std::placeholders;
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
