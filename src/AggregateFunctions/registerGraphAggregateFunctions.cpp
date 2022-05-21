#include <boost/preprocessor/seq/for_each.hpp>

#include <memory>
#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/stringize.hpp>
#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"

namespace DB
{
#define GRAPH_FUNCTION_CLASSES \
    (TreeDiameter)(GraphIsTree)(GraphComponentsCount)(GraphProbabilityConnected)(GraphAvgChildren)(EdgeDistance)(TreeHeight)( \
        GraphCountBridges)(GraphCountStronglyConnectedComponents)(GraphIsBipartite)(GraphCountBipartiteMaximumMatching)(GraphMaxFlow)


#define EXTERN_GRAPH_OPERATION_FACTORY(r, data, operation) \
    AggregateFunctionPtr BOOST_PP_CAT(createGraphOperation, operation)( \
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *);

BOOST_PP_SEQ_FOR_EACH(EXTERN_GRAPH_OPERATION_FACTORY, , GRAPH_FUNCTION_CLASSES)

void registerGraphAggregateFunctions(AggregateFunctionFactory & factory)
{
#define REGISTER_GRAPH_FUNCTION(r, data, elem) \
    factory.registerFunction(BOOST_PP_STRINGIZE(elem), \
        {BOOST_PP_CAT(createGraphOperation, elem), \
         AggregateFunctionProperties{.returns_default_when_only_null = true, .is_order_dependent = false}});

    BOOST_PP_SEQ_FOR_EACH(REGISTER_GRAPH_FUNCTION, , GRAPH_FUNCTION_CLASSES)
}

}
