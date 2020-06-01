#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionDifferentiallyPrivate.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/thread_local_rng.h>

#include <random>

namespace DB {

template <typename T>
using AccumulatorType = std::conditional_t<IsDecimalNumber<T>, Decimal128, NearestFieldType<T>>;


template <template <typename> class AggregateFunctionTemplate>
IAggregateFunction * createWithNumericAndDecimal(const DataTypes & argument_types, const Array & parameters)
{
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        return createWithDecimalType<AggregateFunctionTemplate>(*data_type, argument_types, parameters);
    else
        return createWithNumericType<AggregateFunctionTemplate>(*data_type, argument_types, parameters);
}

AggregateFunctionPtr createDPCount(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception("Aggregate function anon_count required exactly one parameter: epsilon",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    assertArityAtMost<1>(name, argument_types);
    return std::make_shared<AggregateFunctionCountDP>(argument_types, parameters);
}

AggregateFunctionPtr createDPSum(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() != 1 && parameters.size() != 2)
        throw Exception("Aggregate function anon_sum required 1 or 2 parameters: epsilon, [max_abs]",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    assertArityAtMost<1>(name, argument_types);

    AggregateFunctionPtr res;
    if (parameters.size() == 1)
        res.reset(createWithNumericAndDecimal<AggregateFunctionSumDP>(argument_types, parameters));
    else
        res.reset(createWithNumericAndDecimal<AggregateFunctionSumDPBounded>(argument_types, parameters));
    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}


AggregateFunctionPtr createDPAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() < 1 && parameters.size() > 3)
        throw Exception("Aggregate function anon_sum required 1-3 parameters: epsilon, [lower_bound], [upper_bound]",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    assertArityAtMost<1>(name, argument_types);

    AggregateFunctionPtr res;
    if (parameters.size() == 1)
        res.reset(createWithNumericAndDecimal<AggregateFunctionAvgDP>(argument_types, parameters));
    else
        res.reset(createWithNumericAndDecimal<AggregateFunctionAvgDPBounded>(argument_types, parameters));
    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}


AggregateFunctionPtr createDPQuantile(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() < 1 && parameters.size() > 3)
        throw Exception("Aggregate function anon_quantile required 2 or 4 parameters: epsilon, level, [lower_bound, upper_bound]",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    assertArityAtMost<1>(name, argument_types);

    AggregateFunctionPtr res(createWithNumericAndDecimal<AggregateFunctionQuantileDP>(argument_types, parameters));
    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

template <int level>
AggregateFunctionPtr createDPQuantileFixed(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception("Aggregate function " + name + " required 1 parameter: epsilon",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    assertArityAtMost<1>(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionQuantileDP>(*data_type, argument_types, parameters, level));
    else
        res.reset(createWithNumericType<AggregateFunctionQuantileDP>(*data_type, argument_types, parameters, level));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

void registerAggregateFunctionsDP(DB::AggregateFunctionFactory &factory)
{

    factory.registerFunction("anon_max", createDPQuantileFixed<1>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("anon_min", createDPQuantileFixed<0>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("anon_quantile", createDPQuantile, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("anon_count", createDPCount, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("anon_sum", createDPSum, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("anon_avg", createDPAvg, AggregateFunctionFactory::CaseInsensitive);
}

Float64 sampleLaplace() {
    return std::exponential_distribution()(thread_local_rng) - std::exponential_distribution()(thread_local_rng);
}

}