#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSegmentLengthSum.h>
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
    AggregateFunctionPtr createAggregateFunctionSegmentLengthSum(const std::string & name, const DataTypes & arguments, const Array &)
    {
        if (arguments.size() != 2)
            throw Exception(
                "Aggregate function " + name + " requires two timestamps argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto args = {arguments[0].get(), arguments[1].get()};

        if (WhichDataType{args.begin()[0]}.idx != WhichDataType{args.begin()[1]}.idx)
            throw Exception(
                "Illegal type " + args.begin()[0]->getName() + " and " + args.begin()[1]->getName() + " of arguments of aggregate function "
                    + name + ", there two arguments should have same DataType",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (const auto & arg : args)
        {
            if (!isNativeNumber(arg) && !isDateOrDateTime(arg))
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument of aggregate function " + name
                        + ", must be Number, Date, DateTime or DateTime64",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        AggregateFunctionPtr res(createWithBasicNumberOrDateOrDateTime<AggregateFunctionSegmentLengthSum, Data>(*arguments[0], arguments));

        if (res)
            return res;

        throw Exception(
            "Illegal type " + arguments.front().get()->getName() + " of first argument of aggregate function " + name
                + ", must be Native Unsigned Number",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionSegmentLengthSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("segmentLengthSum", createAggregateFunctionSegmentLengthSum<AggregateFunctionSegmentLengthSumData>);
}

}
