#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionKolmogorovSmirnovTest.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionKolmogorovSmirnovTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionKolmogorovSmirnov>(argument_types, parameters);
}


}

void registerAggregateFunctionKolmogorovSmirnovTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("kolmogorovSmirnovTest", createAggregateFunctionKolmogorovSmirnovTest, AggregateFunctionFactory::CaseInsensitive);
}

}
