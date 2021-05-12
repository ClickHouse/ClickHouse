#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRangeSum.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    template <template <typename> class Data>
    AggregateFunctionPtr createAggregateFunctionRangeSum(const std::string & name, const DataTypes & arguments, const Array &)
    {
        if (arguments.size() != 2)
            throw Exception(
                "Aggregate function " + name + " requires two timestamps argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto * arg = arguments[i].get();
            if (!WhichDataType(arg).isNativeUInt())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + toString(i + 1) + " of aggregate function " + name
                        + ", must be Native Unsigned Number",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        AggregateFunctionPtr res(createWithNativeUnsignedIntegerType<AggregateFunctionRangeSum, Data>(*arguments[0], arguments));

        if (res)
            return res;

        throw Exception{
            "Illegal type " + arguments.front().get()->getName() + " of first argument of aggregate function " + name
                + ", must be Native Unsigned Number",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerAggregateFunctionRangeSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("rangeSum", createAggregateFunctionRangeSum<AggregateFunctionRangeSumData>);
}

}
