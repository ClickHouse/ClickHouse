#include "Common/Exception.h"
#include "AggregateFunctions/AggregateFunctionFactory.h"
#include "AggregateFunctions/FactoryHelpers.h"
#include "AggregateFunctions/IAggregateFunction.h"


namespace DB {

template<typename GraphOperation>
AggregateFunctionPtr createGraphOperation(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (GraphOperation::ExpectsFromToInput) {
        if (parameters.size() != 2) {
            throw Exception("Aggregate function " + name + " requires 2 parameters", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    } else {
        assertNoParameters(name, parameters);
    }

    if (!argument_types[0]->equals(*argument_types[1])) {
        throw Exception("Parameters for aggregate function " + name + " should be of equal types. Got " + argument_types[0]->getName() + " and " + argument_types[1]->getName(), ErrorCodes::BAD_ARGUMENTS);
    }
    return std::make_shared<GraphOperation>(argument_types[0], parameters);
}

template<typename GraphOperation>
void registerGraphAggregateFunction(AggregateFunctionFactory & factory)
{
    factory.registerFunction(GraphOperation::name, { createGraphOperation<GraphOperation>, AggregateFunctionProperties{} });
}

}  // namespace DB
