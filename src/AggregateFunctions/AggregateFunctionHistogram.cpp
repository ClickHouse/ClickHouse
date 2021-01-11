#include <AggregateFunctions/AggregateFunctionHistogram.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <Common/FieldVisitors.h>
#include "registerAggregateFunctions.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_PARAMETER;
    extern const int PARAMETER_OUT_OF_BOUND;
}


namespace
{

AggregateFunctionPtr createAggregateFunctionHistogram(const std::string & name, const DataTypes & arguments, const Array & params)
{
    if (params.size() != 1)
        throw Exception("Function " + name + " requires single parameter: bins count", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (params[0].getType() != Field::Types::UInt64)
        throw Exception("Invalid type for bins count", ErrorCodes::UNSUPPORTED_PARAMETER);

    UInt32 bins_count = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), params[0]);

    auto limit = AggregateFunctionHistogramData::bins_count_limit;
    if (bins_count > limit)
        throw Exception("Unsupported bins count. Should not be greater than " + std::to_string(limit), ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (bins_count == 0)
        throw Exception("Bin count should be positive", ErrorCodes::BAD_ARGUMENTS);

    assertUnary(name, arguments);
    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionHistogram>(*arguments[0], bins_count, arguments, params));

    if (!res)
        throw Exception("Illegal type " + arguments[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionHistogram(AggregateFunctionFactory & factory)
{
    factory.registerFunction("histogram", createAggregateFunctionHistogram);
}

}
