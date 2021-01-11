#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

inline void assertNoParameters(const std::string & name, const Array & parameters)
{
    if (!parameters.empty())
        throw Exception("Aggregate function " + name + " cannot have parameters", ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
}

inline void assertUnary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " requires single argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

inline void assertBinary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception("Aggregate function " + name + " requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

template<std::size_t maximal_arity>
inline void assertArityAtMost(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() <= maximal_arity)
        return;

    if constexpr (maximal_arity == 0)
        throw Exception("Aggregate function " + name + " cannot have arguments",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if constexpr (maximal_arity == 1)
        throw Exception("Aggregate function " + name + " requires zero or one argument",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    throw Exception("Aggregate function " + name + " requires at most " + toString(maximal_arity) + " arguments",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

}
