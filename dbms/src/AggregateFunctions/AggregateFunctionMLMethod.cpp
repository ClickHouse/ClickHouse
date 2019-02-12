#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMLMethod.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

using FuncLinearRegression = AggregateFunctionMLMethod<LinearModelData, NameLinearRegression>;
using FuncLogisticRegression = AggregateFunctionMLMethod<LinearModelData, NameLogisticRegression>;
template <class Method>
AggregateFunctionPtr createAggregateFunctionMLMethod(
        const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() > 4)
        throw Exception("Aggregate function " + name + " requires at most four parameters", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!WhichDataType(argument_types[i]).isFloat64())
            throw Exception("Illegal type " + argument_types[i]->getName() + " of argument " 
			                    + std::to_string(i) + "for aggregate function " + name,
                             ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    Float64 learning_rate = Float64(0.01);
    UInt32 batch_size = 1;

    std::shared_ptr<IGradientComputer> gc;
    std::shared_ptr<IWeightsUpdater> wu;
    if (!parameters.empty())
    {
        learning_rate = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    }
    if (parameters.size() > 1)
    {
        batch_size = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[1]);

    }

    /// Gradient_Computer for LinearRegression has LinearRegression gradient computer
    if (std::is_same<Method, FuncLinearRegression>::value)
    {
       gc = std::make_shared<LinearRegression>(argument_types.size());
    } else if (std::is_same<Method, FuncLogisticRegression>::value)
    {
       gc = std::make_shared<LogisticRegression>(argument_types.size());
    } else
    {
        throw Exception("Such gradient computer is not implemented yet", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    if (parameters.size() > 2)
    {
        if (applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[2]) == Float64{1.0})
        {
            wu = std::make_shared<StochasticGradientDescent>();
        } else if (applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[2]) == Float64{2.0})
        {
            wu = std::make_shared<Momentum>();
        } else
        {
            throw Exception("Such weights updater is not implemented yet", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    } else
    {
        wu = std::make_unique<StochasticGradientDescent>();
    }

    if (argument_types.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<Method>(argument_types.size() - 1, gc, wu, learning_rate, batch_size, argument_types, parameters);
}

}

void registerAggregateFunctionMLMethod(AggregateFunctionFactory & factory)
{
    factory.registerFunction("LinearRegression", createAggregateFunctionMLMethod<FuncLinearRegression>);
    factory.registerFunction("LogisticRegression", createAggregateFunctionMLMethod<FuncLogisticRegression>);
}

}
