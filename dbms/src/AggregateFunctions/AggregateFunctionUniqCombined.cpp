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
    template <UInt8 K>
    struct WithK
    {
        template <typename T>
        using AggregateFunction = AggregateFunctionUniqCombined<T, K>;

        template <bool is_exact, bool argument_is_tuple>
        using AggregateFunctionVariadic = AggregateFunctionUniqCombinedVariadic<is_exact, argument_is_tuple, K>;
    };

    template <UInt8 K>
    AggregateFunctionPtr createAggregateFunctionWithK(const DataTypes & argument_types)
    {
        /// We use exact hash function if the arguments are not contiguous in memory, because only exact hash function has support for this case.
        bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

        if (argument_types.size() == 1)
        {
            const IDataType & argument_type = *argument_types[0];

            AggregateFunctionPtr res(createWithNumericType<WithK<K>::template AggregateFunction>(*argument_types[0]));

            WhichDataType which(argument_type);
            if (res)
                return res;
            else if (which.isDate())
                return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeDate::FieldType>>();
            else if (which.isDateTime())
                return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeDateTime::FieldType>>();
            else if (which.isStringOrFixedString())
                return std::make_shared<typename WithK<K>::template AggregateFunction<String>>();
            else if (which.isUUID())
                return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeUUID::FieldType>>();
            else if (which.isTuple())
            {
                if (use_exact_hash_function)
                    return std::make_shared<typename WithK<K>::template AggregateFunctionVariadic<true, true>>(argument_types);
                else
                    return std::make_shared<typename WithK<K>::template AggregateFunctionVariadic<false, true>>(argument_types);
            }
        }

        /// "Variadic" method also works as a fallback generic case for a single argument.
        if (use_exact_hash_function)
            return std::make_shared<typename WithK<K>::template AggregateFunctionVariadic<true, false>>(argument_types);
        else
            return std::make_shared<typename WithK<K>::template AggregateFunctionVariadic<false, false>>(argument_types);
    }

    AggregateFunctionPtr createAggregateFunctionUniqCombined(
        const std::string & name, const DataTypes & argument_types, const Array & params)
    {
        /// log2 of the number of cells in HyperLogLog.
        /// Reasonable default value, selected to be comparable in quality with "uniq" aggregate function.
        UInt8 precision = 17;

        if (!params.empty())
        {
            if (params.size() != 1)
                throw Exception(
                    "Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

            // This range is hardcoded below
            if (precision_param > 20 || precision_param < 12)
                throw Exception(
                    "Parameter for aggregate function " + name + "is out or range: [12, 20].", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            precision = precision_param;
        }

        if (argument_types.empty())
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        switch (precision)
        {
            case 12:
                return createAggregateFunctionWithK<12>(argument_types);
            case 13:
                return createAggregateFunctionWithK<13>(argument_types);
            case 14:
                return createAggregateFunctionWithK<14>(argument_types);
            case 15:
                return createAggregateFunctionWithK<15>(argument_types);
            case 16:
                return createAggregateFunctionWithK<16>(argument_types);
            case 17:
                return createAggregateFunctionWithK<17>(argument_types);
            case 18:
                return createAggregateFunctionWithK<18>(argument_types);
            case 19:
                return createAggregateFunctionWithK<19>(argument_types);
            case 20:
                return createAggregateFunctionWithK<20>(argument_types);
        }

        __builtin_unreachable();
    }

} // namespace

void registerAggregateFunctionUniqCombined(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqCombined", createAggregateFunctionUniqCombined);
}

} // namespace DB
