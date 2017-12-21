#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionStatistics.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<FunctionTemplate>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

template <template <typename, typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1]));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsStatistics(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSamp>);
    factory.registerFunction("varPop", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPop>);
    factory.registerFunction("stddevSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionStdDevSamp>);
    factory.registerFunction("stddevPop", createAggregateFunctionStatisticsUnary<AggregateFunctionStdDevPop>);
    factory.registerFunction("covarSamp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSamp>);
    factory.registerFunction("covarPop", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPop>);
    factory.registerFunction("corr", createAggregateFunctionStatisticsBinary<AggregateFunctionCorr>, AggregateFunctionFactory::CaseInsensitive);

    /// Synonims for compatibility.
    factory.registerFunction("VAR_SAMP", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSamp>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("VAR_POP", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPop>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("STDDEV_SAMP", createAggregateFunctionStatisticsUnary<AggregateFunctionStdDevSamp>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("STDDEV_POP", createAggregateFunctionStatisticsUnary<AggregateFunctionStdDevPop>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("COVAR_SAMP", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSamp>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("COVAR_POP", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPop>, AggregateFunctionFactory::CaseInsensitive);
}

}
