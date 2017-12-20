#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileExactWeighted.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileExactWeighted(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertBinary(name, argument_types);

    if (params.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionQuantileExactWeighted>(*argument_types[0], *argument_types[1], argument_types[0], level));

    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionQuantilesExactWeighted(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoNumericTypes<AggregateFunctionQuantilesExactWeighted>(*argument_types[0], *argument_types[1], argument_types[0], params));

    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}


}

void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileExactWeighted", createAggregateFunctionQuantileExactWeighted);
    factory.registerFunction("medianExactWeighted", createAggregateFunctionQuantileExactWeighted);
    factory.registerFunction("quantilesExactWeighted", createAggregateFunctionQuantilesExactWeighted);
}

}
