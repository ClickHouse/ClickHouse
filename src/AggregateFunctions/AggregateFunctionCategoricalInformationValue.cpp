#include <AggregateFunctions/AggregateFunctionCategoricalInformationValue.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionCategoricalIV(
    const std::string & name,
    const DataTypes & arguments,
    const Array & params
)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception(
            "Aggregate function " + name + " requires two or more arguments",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (const auto & argument : arguments)
    {
        if (!WhichDataType(argument).isUInt8())
            throw Exception(
                "All the arguments of aggregate function " + name + " should be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    return std::make_shared<AggregateFunctionCategoricalIV<>>(arguments, params);
}

}

void registerAggregateFunctionCategoricalIV(
    AggregateFunctionFactory & factory
)
{
    factory.registerFunction("categoricalInformationValue", createAggregateFunctionCategoricalIV);
}

}
