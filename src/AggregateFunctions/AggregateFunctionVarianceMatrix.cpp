#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionVarianceMatrix.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionVarianceMatrix(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    for (const auto & argument_type : argument_types)
        if (!isNativeNumber(argument_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<FunctionTemplate>(argument_types);
}

}

void registerAggregateFunctionsVarianceMatrix(AggregateFunctionFactory & factory)
{
    factory.registerFunction("covarSampMatrix", createAggregateFunctionVarianceMatrix<AggregateFunctionCovarSampMatrix>);
    factory.registerFunction("covarPopMatrix", createAggregateFunctionVarianceMatrix<AggregateFunctionCovarPopMatrix>);
    factory.registerFunction("corrMatrix", createAggregateFunctionVarianceMatrix<AggregateFunctionCorrMatrix>);
}

}
