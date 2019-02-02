#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionEntropy.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionEntropy(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    WhichDataType which(argument_types[0]);
    if (isNumber(argument_types[0]))
    {
        if (which.isUInt64())
        {
            return std::make_shared<AggregateFunctionEntropy<UInt64>>();
        }
        else if (which.isInt64())
        {
            return std::make_shared<AggregateFunctionEntropy<Int64>>();
        }
        else if (which.isInt32())
        {
            return std::make_shared<AggregateFunctionEntropy<Int32>>();
        }
        else if (which.isUInt32())
        {
            return std::make_shared<AggregateFunctionEntropy<UInt32>>();
        }
        else if (which.isUInt128())
        {
            return std::make_shared<AggregateFunctionEntropy<UInt128, true>>();
        }
    }

    return std::make_shared<AggregateFunctionEntropy<UInt128>>();
}

}

void registerAggregateFunctionEntropy(AggregateFunctionFactory & factory)
{
    factory.registerFunction("entropy", createAggregateFunctionEntropy);
}

}
