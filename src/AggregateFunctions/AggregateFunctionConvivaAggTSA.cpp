#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionConvivaAggTSA.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

    template <template <typename> class AggregateFunctionTemplate, typename ... TArgs>
    IAggregateFunction * createWithNumericOrTimeType(const IDataType & argument_type, TArgs && ... args)
    {
        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<UInt16>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<UInt32>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::IPv4) return new AggregateFunctionTemplate<IPv4>(std::forward<TArgs>(args)...);
        return createWithNumericType<AggregateFunctionTemplate, TArgs...>(argument_type, std::forward<TArgs>(args)...);
    }

    template <typename ... TArgs>
    inline AggregateFunctionPtr createAggregateFunctionConvivaAggTSAImpl(const DataTypePtr & argument_type, const Array & parameters, TArgs ... args)
    {
        if (auto res = createWithNumericOrTimeType<ConvivaAggTSANumericImpl>(*argument_type, argument_type, parameters, std::forward<TArgs>(args)...))
            return AggregateFunctionPtr(res);

        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::String)
            return std::make_shared<ConvivaAggTSAGeneralImpl<ConvivaAggTSANodeString>>(argument_type, parameters, std::forward<TArgs>(args)...);

        return std::make_shared<ConvivaAggTSAGeneralImpl<ConvivaAggTSANodeGeneral>>(argument_type, parameters, std::forward<TArgs>(args)...);
    }

    AggregateFunctionPtr createAggregateFunctionConvivaAggTSA(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertUnary(name, argument_types);

        UInt64 max_elems = std::numeric_limits<UInt64>::max();
        // TODO: add per query limit parameter for input array size (max_elems)

        std::string yaml_config;

        if (parameters.size() == 1)
        {
            auto type = parameters[0].getType();
            if (type != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be a string", name);

            yaml_config = parameters[0].get<String>();
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect number of parameters for aggregate function {}, should be 1", name);

        return createAggregateFunctionConvivaAggTSAImpl(argument_types[0], parameters, yaml_config, max_elems);
    }
}


void registerAggregateFunctionConvivaAggTSA(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("convivaAggTSA", { createAggregateFunctionConvivaAggTSA, properties });
}

}
