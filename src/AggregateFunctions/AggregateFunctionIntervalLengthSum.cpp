#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionIntervalLengthSum.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <base/range.h>


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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {} requires two timestamps argument.", name);

        auto args = {arguments[0].get(), arguments[1].get()};

        if (WhichDataType{args.begin()[0]}.idx != WhichDataType{args.begin()[1]}.idx)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal types {} and {} of arguments "
                            "of aggregate function {}, both arguments should have same data type",
                            args.begin()[0]->getName(), args.begin()[1]->getName(), name);

        for (const auto & arg : args)
        {
            if (!isNativeNumber(arg) && !isDate(arg) && !isDateTime(arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of argument of aggregate function {}, must "
                                "be native integral type, Date/DateTime or Float", arg->getName(), name);
        }

        AggregateFunctionPtr res(createWithBasicNumberOrDateOrDateTime<AggregateFunctionIntervalLengthSum, Data>(*arguments[0], arguments));

        if (res)
            return res;

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument of aggregate function {}, must "
                        "be native integral type, Date/DateTime or Float", arguments.front().get()->getName(), name);
}

}

void registerAggregateFunctionIntervalLengthSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("intervalLengthSum", createAggregateFunctionIntervalLengthSum<AggregateFunctionIntervalLengthSumData>);
}

}
