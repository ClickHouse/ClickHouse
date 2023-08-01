#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

inline void assertNoParameters(const std::string & name, const Array & parameters)
{
    if (!parameters.empty())
        throw Exception(ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS, "Aggregate function {} cannot have parameters", name);
}

inline void assertUnary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires single argument", name);
}

inline void assertBinary(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires two arguments", name);
}

template<std::size_t maximal_arity>
inline void assertArityAtMost(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() <= maximal_arity)
        return;

    if constexpr (maximal_arity == 0)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} cannot have arguments", name);

    if constexpr (maximal_arity == 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires zero or one argument",
                        name);

    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at most {} arguments",
                    name, maximal_arity);
}

}
