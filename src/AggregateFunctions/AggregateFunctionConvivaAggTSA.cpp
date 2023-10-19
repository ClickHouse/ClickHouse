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

    template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename ... TArgs>
    IAggregateFunction * createWithNumericOrTimeType(const IDataType & argument_type, TArgs && ... args)
    {
        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<UInt16, Data>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<UInt32, Data>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::IPv4) return new AggregateFunctionTemplate<IPv4, Data>(std::forward<TArgs>(args)...);
        return createWithNumericType<AggregateFunctionTemplate, Data, TArgs...>(argument_type, std::forward<TArgs>(args)...);
    }


    template <typename Trait, typename ... TArgs>
    inline AggregateFunctionPtr createAggregateFunctionConvivaAggTSAImpl(const DataTypePtr & argument_type, const Array & parameters, TArgs ... args)
    {
        if (auto res = createWithNumericOrTimeType<ConvivaAggTSANumericImpl, Trait>(*argument_type, argument_type, parameters, std::forward<TArgs>(args)...))
            return AggregateFunctionPtr(res);

        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::String)
            return std::make_shared<ConvivaAggTSAGeneralImpl<ConvivaAggTSANodeString, Trait>>(argument_type, parameters, std::forward<TArgs>(args)...);

        return std::make_shared<ConvivaAggTSAGeneralImpl<ConvivaAggTSANodeGeneral, Trait>>(argument_type, parameters, std::forward<TArgs>(args)...);
    }


    template <bool Tlast>
    AggregateFunctionPtr createAggregateFunctionConvivaAggTSA(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertUnary(name, argument_types);

        bool limit_size = false;
        UInt64 max_elems = std::numeric_limits<UInt64>::max();

        if (parameters.empty())
        {
            // no limit
        }
        else if (parameters.size() == 1)
        {
            auto type = parameters[0].getType();
            if (type != Field::Types::Int64 && type != Field::Types::UInt64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

            if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
                (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

            limit_size = true;
            max_elems = parameters[0].get<UInt64>();
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect number of parameters for aggregate function {}, should be 0 or 1", name);

        if (!limit_size)
        {
            if (Tlast)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "convivaAggTSALast make sense only with max_elems (convivaAggTSALast(max_elems)())");
            return createAggregateFunctionConvivaAggTSAImpl<ConvivaAggTSATrait</* Thas_limit= */ false, Tlast, /* Tsampler= */ Sampler::NONE>>(argument_types[0], parameters);
        }
        else
            return createAggregateFunctionConvivaAggTSAImpl<ConvivaAggTSATrait</* Thas_limit= */ true, Tlast, /* Tsampler= */ Sampler::NONE>>(argument_types[0], parameters, max_elems);
    }

    AggregateFunctionPtr createAggregateFunctionConvivaAggTSASample(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertUnary(name, argument_types);

        if (parameters.size() != 1 && parameters.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect number of parameters for aggregate function {}, should be 1 or 2", name);

        auto get_parameter = [&](size_t i)
        {
            auto type = parameters[i].getType();
            if (type != Field::Types::Int64 && type != Field::Types::UInt64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

            if ((type == Field::Types::Int64 && parameters[i].get<Int64>() < 0) ||
                (type == Field::Types::UInt64 && parameters[i].get<UInt64>() == 0))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

            return parameters[i].get<UInt64>();
        };

        UInt64 max_elems = get_parameter(0);

        UInt64 seed;
        if (parameters.size() >= 2)
            seed = get_parameter(1);
        else
            seed = thread_local_rng();

        return createAggregateFunctionConvivaAggTSAImpl<ConvivaAggTSATrait</* Thas_limit= */ true, /* Tlast= */ false, /* Tsampler= */ Sampler::RNG>>(argument_types[0], parameters, max_elems, seed);
    }

}


void registerAggregateFunctionConvivaAggTSA(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("convivaAggTSA", { createAggregateFunctionConvivaAggTSA<false>, properties });
    //factory.registerAlias("array_agg", "convivaAggTSA", AggregateFunctionFactory::CaseInsensitive);
    //factory.registerAliasUnchecked("array_concat_agg", "convivaAggTSAArray", AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("convivaAggTSASample", { createAggregateFunctionConvivaAggTSASample, properties });
    factory.registerFunction("convivaAggTSALast", { createAggregateFunctionConvivaAggTSA<true>, properties });
}

}
