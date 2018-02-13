#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


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

void registerAggregateFunctionsStatisticsSimple(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampSimple>);
    factory.registerFunction("varPop", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopSimple>);
    factory.registerFunction("stddevSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampSimple>);
    factory.registerFunction("stddevPop", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopSimple>);
    factory.registerFunction("covarSamp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampSimple>);
    factory.registerFunction("covarPop", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopSimple>);
    factory.registerFunction("corr", createAggregateFunctionStatisticsBinary<AggregateFunctionCorrSimple>, AggregateFunctionFactory::CaseInsensitive);

    /// Synonims for compatibility.
    factory.registerFunction("VAR_SAMP", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampSimple>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("VAR_POP", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopSimple>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("STDDEV_SAMP", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampSimple>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("STDDEV_POP", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopSimple>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("COVAR_SAMP", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampSimple>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("COVAR_POP", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopSimple>, AggregateFunctionFactory::CaseInsensitive);
}

}
