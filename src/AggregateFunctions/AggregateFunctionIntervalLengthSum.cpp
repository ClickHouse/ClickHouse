#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionIntervalLengthSum.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <common/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Settings;

namespace
{
    template <template <typename> class Data>
    AggregateFunctionPtr
    createAggregateFunctionIntervalLengthSum(const std::string & name, const DataTypes & arguments, const Array &, const Settings *)
    {
        if (arguments.size() != 2)
            throw Exception(
                "Aggregate function " + name + " requires two timestamps argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto args = {arguments[0].get(), arguments[1].get()};

        if (WhichDataType{args.begin()[0]}.idx != WhichDataType{args.begin()[1]}.idx)
            throw Exception(
                "Illegal types " + args.begin()[0]->getName() + " and " + args.begin()[1]->getName() + " of arguments of aggregate function "
                    + name + ", both arguments should have same data type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (const auto & arg : args)
        {
            if (!isNativeNumber(arg) && !isDate(arg) && !isDateTime(arg))
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument of aggregate function " + name
                        + ", must be native integral type, Date/DateTime or Float",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        AggregateFunctionPtr res(createWithBasicNumberOrDateOrDateTime<AggregateFunctionIntervalLengthSum, Data>(*arguments[0], arguments));

        if (res)
            return res;

        throw Exception(
            "Illegal type " + arguments.front().get()->getName() + " of argument of aggregate function " + name
            + ", must be native integral type, Date/DateTime or Float",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionIntervalLengthSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("intervalLengthSum", createAggregateFunctionIntervalLengthSum<AggregateFunctionIntervalLengthSumData>);
}

}
