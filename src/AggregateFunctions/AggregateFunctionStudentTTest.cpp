#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStudentTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"

#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>


// the return type is boolean (we use UInt8 as we do not have boolean in clickhouse)

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionStudentTTest(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    AggregateFunctionPtr res;

    if (isDecimal(argument_types[0]) || isDecimal(argument_types[1]))
    {
        throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::NOT_IMPLEMENTED);
    }
    else
    {
        res.reset(createWithTwoNumericTypes<AggregateFunctionStudentTTest>(*argument_types[0], *argument_types[1], argument_types));
    }

    if (!res)
    {
        throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::NOT_IMPLEMENTED);
    }

    return res;
}
}

void registerAggregateFunctionStudentTTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("studentTTest", createAggregateFunctionStudentTTest);
}
}
