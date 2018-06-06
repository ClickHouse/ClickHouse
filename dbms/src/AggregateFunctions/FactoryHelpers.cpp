#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void assertNoParameters(const std::string & name, const Array & parameters)
{
    if (!parameters.empty())
        throw Exception("Aggregate function " + name + " cannot have parameters", ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
}

void assertUnary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " require single argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void assertBinary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception("Aggregate function " + name + " require two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

}
