#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{


/** `DataForVariadic` is a data structure that will be used for `uniq` aggregate function of multiple arguments.
  * It differs, for example, in that it uses a trivial hash function, since `uniq` of many arguments first hashes them out itself.
  */
template <typename Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data>>(argument_types);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, true>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, true>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, false>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, false>>(argument_types);
}

template <bool is_exact, template <typename> class Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// We use exact hash function if the user wants it;
    /// or if the arguments are not contiguous in memory, because only exact hash function have support for this case.
    bool use_exact_hash_function = is_exact || !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data<String>>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data<DataTypeUUID::FieldType>>>(argument_types);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, true>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, true>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, false>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, false>>(argument_types);
}

}

void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("uniq",
        {createAggregateFunctionUniq<AggregateFunctionUniqUniquesHashSetData, AggregateFunctionUniqUniquesHashSetDataForVariadic>, properties});

    factory.registerFunction("uniqHLL12",
        {createAggregateFunctionUniq<false, AggregateFunctionUniqHLL12Data, AggregateFunctionUniqHLL12DataForVariadic>, properties});

    factory.registerFunction("uniqExact",
        {createAggregateFunctionUniq<true, AggregateFunctionUniqExactData, AggregateFunctionUniqExactData<String>>, properties});

#if USE_DATASKETCHES
    factory.registerFunction("uniqTheta",
        {createAggregateFunctionUniq<AggregateFunctionUniqThetaData, AggregateFunctionUniqThetaData>, properties});
#endif

}

}
