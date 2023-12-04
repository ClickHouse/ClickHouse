#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionLargestTriangleThreeBuckets.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

namespace
{

    AggregateFunctionPtr
    createAggregateFunctionLargestTriangleThreeBuckets(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertBinary(name, argument_types);


        if (!(isNumber(argument_types[0]) || isDateOrDate32(argument_types[0]) || isDateTime(argument_types[0])
              || isDateTime64(argument_types[0])))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Aggregate function {} only supports Date, Date32, DateTime, DateTime64 and Number as the first argument",
                name);

        if (!(isNumber(argument_types[1]) || isDateOrDate32(argument_types[1]) || isDateTime(argument_types[1])
              || isDateTime64(argument_types[1])))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Aggregate function {} only supports Date, Date32, DateTime, DateTime64 and Number as the second argument",
                name);

        return std::make_shared<AggregateFunctionLargestTriangleThreeBuckets>(argument_types, parameters);
    }

}


void registerAggregateFunctionLargestTriangleThreeBuckets(AggregateFunctionFactory & factory)
{
    factory.registerFunction(AggregateFunctionLargestTriangleThreeBuckets::name, createAggregateFunctionLargestTriangleThreeBuckets);
    factory.registerAlias("lttb", AggregateFunctionLargestTriangleThreeBuckets::name);
}


}
