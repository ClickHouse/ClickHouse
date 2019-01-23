#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMLMethod.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

using FuncLinearRegression = AggregateFunctionMLMethod<LinearRegressionData, NameLinearRegression>;

template <class Method>
AggregateFunctionPtr createAggregateFunctionMLMethod(
        const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() > 1)
        throw Exception("Aggregate function " + name + " requires at most one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!WhichDataType(argument_types[i]).isFloat64())
            throw Exception("Illegal type " + argument_types[i]->getName() + " of argument " +
                                    std::to_string(i) + "for aggregate function " + name,
                                  ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    Float64 learning_rate;
    if (parameters.empty())
    {
        learning_rate = Float64(0.01);
    } else
    {
        learning_rate = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    }

    if (argument_types.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<Method>(argument_types.size() - 1, learning_rate);
}

}

void registerAggregateFunctionMLMethod(AggregateFunctionFactory & factory) {
    factory.registerFunction("LinearRegression", createAggregateFunctionMLMethod<FuncLinearRegression>);
}

}