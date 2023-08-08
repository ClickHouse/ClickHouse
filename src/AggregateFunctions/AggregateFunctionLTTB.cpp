#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionLTTB.h>
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
    createAggregateFunctionLTTB(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertBinary(name, argument_types);


        if (!(isNumber(argument_types[0]) || isDateOrDate32(argument_types[0]) || isDateTime(argument_types[0])
              || isDateTime64(argument_types[0])))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Aggregate function {} only supports Date, Date32, DateTime, DateTime64 and Number as arg 1",
                name);

        if (!(isNumber(argument_types[1]) || isDateOrDate32(argument_types[1]) || isDateTime(argument_types[1])
              || isDateTime64(argument_types[1])))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Aggregate function {} only supports Date, Date32, DateTime, DateTime64 and Number as arg 2",
                name);

        return std::make_shared<AggregateFunctionLTTB>(argument_types, parameters);
    }

}


void registerAggregateFunctionLTTB(AggregateFunctionFactory & factory)
{
    factory.registerFunction("lttb", createAggregateFunctionLTTB);
}


}
