#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionEntropy.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include "registerAggregateFunctions.h"


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

    size_t num_args = argument_types.size();
    if (num_args == 1)
    {
        /// Specialized implementation for single argument of numeric type.
        if (auto * res = createWithNumericBasedType<AggregateFunctionEntropy>(*argument_types[0], argument_types))
            return AggregateFunctionPtr(res);
    }

    /// Generic implementation for other types or for multiple arguments.
    return std::make_shared<AggregateFunctionEntropy<UInt128>>(argument_types);
}

}

void registerAggregateFunctionEntropy(AggregateFunctionFactory & factory)
{
    factory.registerFunction("entropy", createAggregateFunctionEntropy);
}

}
