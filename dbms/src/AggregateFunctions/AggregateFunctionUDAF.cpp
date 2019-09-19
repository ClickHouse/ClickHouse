#include <AggregateFunctions/AggregateFunctionUDAF.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/UDFHeaders.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionUDAF(
    const String & name,
    const DataTypes & arguments,
    const Array & params
)
{
    if (params.size() != 1)
        throw Exception {
            "Aggregate function " + name + " requires one parameter",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
        };

    String code {
        params.front().safeGet<String>()
    };

    return std::make_shared<AggregateFunctionUDAF<header_code, udaf_code>>(
        code,
        arguments,
        params
    );
}

}

void registerAggregateFunctionUDAF(AggregateFunctionFactory & factory)
{
    factory.registerFunction("udaf", createAggregateFunctionUDAF);
}

}
