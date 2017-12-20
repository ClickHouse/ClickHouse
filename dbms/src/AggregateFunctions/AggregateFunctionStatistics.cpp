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

AggregateFunctionPtr createAggregateFunctionVarPop(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionVarPop>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionVarSamp(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionVarSamp>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionStdDevPop(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionStdDevPop>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionStdDevSamp(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionStdDevSamp>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionCovarPop(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionCovarPop>(*argument_types[0], *argument_types[1]));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionCovarSamp(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionCovarSamp>(*argument_types[0], *argument_types[1]));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}


AggregateFunctionPtr createAggregateFunctionCorr(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionCorr>(*argument_types[0], *argument_types[1]));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsStatistics(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSamp", createAggregateFunctionVarSamp);
    factory.registerFunction("varPop", createAggregateFunctionVarPop);
    factory.registerFunction("stddevSamp", createAggregateFunctionStdDevSamp);
    factory.registerFunction("stddevPop", createAggregateFunctionStdDevPop);
    factory.registerFunction("covarSamp", createAggregateFunctionCovarSamp);
    factory.registerFunction("covarPop", createAggregateFunctionCovarPop);
    factory.registerFunction("corr", createAggregateFunctionCorr, AggregateFunctionFactory::CaseInsensitive);

    /// Synonims for compatibility.
    factory.registerFunction("VAR_SAMP", createAggregateFunctionVarSamp, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("VAR_POP", createAggregateFunctionVarPop, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("STDDEV_SAMP", createAggregateFunctionStdDevSamp, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("STDDEV_POP", createAggregateFunctionStdDevPop, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("COVAR_SAMP", createAggregateFunctionCovarSamp, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("COVAR_POP", createAggregateFunctionCovarPop, AggregateFunctionFactory::CaseInsensitive);
}

}
