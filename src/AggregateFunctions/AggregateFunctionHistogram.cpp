#include <AggregateFunctions/AggregateFunctionHistogram.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/FieldVisitorConvertToNumber.h>


namespace DB
{
struct Settings;

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

AggregateFunctionPtr createAggregateFunctionHistogram(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    if (params.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires single parameter: bins count", name);

    if (params[0].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::UNSUPPORTED_PARAMETER, "Invalid type for bins count");

    UInt32 bins_count = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), params[0]);

    auto limit = AggregateFunctionHistogramData::bins_count_limit;
    if (bins_count > limit)
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Unsupported bins count. Should not be greater than {}", limit);

    if (bins_count == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bin count should be positive");

    assertUnary(name, arguments);
    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionHistogram>(*arguments[0], bins_count, arguments, params));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}", arguments[0]->getName(), name);

    return res;
}

}

void registerAggregateFunctionHistogram(AggregateFunctionFactory & factory)
{
    factory.registerFunction("histogram", createAggregateFunctionHistogram);
}

}
