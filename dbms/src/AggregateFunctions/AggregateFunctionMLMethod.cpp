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
                const std::string & name, const DataTypes & arguments, const Array & parameters)
        {
            if (parameters.size() > 1)
                throw Exception("Aggregate function " + name + " requires at most one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            Float64 lr;
            if (parameters.empty())
                lr = Float64(0.01);
            else
                lr = static_cast<const Float64>(parameters[0].template get<Float64>());

            if (arguments.size() < 2)
                throw Exception("Aggregate function " + name + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            return std::make_shared<Method>(arguments.size() - 1, lr);
        }

    }

    void registerAggregateFunctionMLMethod(AggregateFunctionFactory & factory) {
        factory.registerFunction("LinearRegression", createAggregateFunctionMLMethod<FuncLinearRegression>);
    }

}