#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>

#include <Common/FieldVisitors.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <map>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRate.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionRate(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments, with the first being a timestamp",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


    return std::make_shared<AggregateFunctionRate>(argument_types, parameters);
}

}

void registerAggregateFunctionRate(AggregateFunctionFactory & factory)
{
    factory.registerFunction("rate", createAggregateFunctionRate, AggregateFunctionFactory::CaseInsensitive);
}

}
