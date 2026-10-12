#pragma once

#include <Common/Exception.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

/// For a creator that accepts any argument type but whose execution path cannot handle a Variant (or Dynamic)
/// value faithfully: it would fail later during execution (getDataAt / getFloat64), or round-trip the value
/// through Field, which loses the original alternative type (1::UInt8 and 1::UInt64 collapse). Reject these
/// types at resolution instead. Reporting ILLEGAL_TYPE_OF_ARGUMENT here lets AggregateFunctionVariantAdapter
/// retry over the least common supertype of the variants, and reports a clean error at resolution otherwise,
/// instead of a resolve-success / execute-fail (or silently wrong) path.
inline void assertNoDynamicOrVariantArguments(const std::string & name, const DataTypes & argument_types)
{
    for (const auto & argument_type : argument_types)
        if (isDynamic(*argument_type) || isVariant(*argument_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}", argument_type->getName(), name);
}

}
