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

    if (typeid_cast<const DataTypeFloat32 *>(argument_types[0].get())) return std::make_shared<FunctionTemplate<Float32>>();
    if (typeid_cast<const DataTypeFloat64 *>(argument_types[0].get())) return std::make_shared<FunctionTemplate<Float64>>();

    throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <template <typename> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    if (!argument_types[0]->equals(*argument_types[1]))
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name + ", must be the same", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (typeid_cast<const DataTypeFloat32 *>(argument_types[0].get())) return std::make_shared<FunctionTemplate<Float32>>();
    if (typeid_cast<const DataTypeFloat64 *>(argument_types[0].get())) return std::make_shared<FunctionTemplate<Float64>>();

    throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
