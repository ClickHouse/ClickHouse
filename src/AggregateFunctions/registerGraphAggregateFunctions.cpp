#include <AggregateFunctions/AggregateFunctionGraphFactory.h>
#include <boost/preprocessor/seq/for_each.hpp>

namespace DB
{
#define GRAPH_FUNCTION_CLASSES \
    (GraphDiameterGeneral)(GraphIsTreeGeneral)(GraphComponentsCountGeneral)(GraphProbabilityConnected)(GraphAvgChildrenGeneral)( \
        EdgeDistanceGeneral)(GraphTreeHeight)(GraphCountBridges)(GraphCountStronglyConnectedComponents)(GraphIsBipartiteGeneral)( \
        GraphCountBipartiteMaximumMatching)(GraphMaxFlow)


#define EXTERN_GRAPH_FUNCTION(r, data, elem) \
    class elem; \
    extern template void registerGraphAggregateFunction<elem>(AggregateFunctionFactory & factory);

BOOST_PP_SEQ_FOR_EACH(EXTERN_GRAPH_FUNCTION, , GRAPH_FUNCTION_CLASSES)


void registerGraphAggregateFunctions(AggregateFunctionFactory & factory)
{
#define REGISTER_GRAPH_FUNCTION(r, data, elem) registerGraphAggregateFunction<elem>(factory);

    BOOST_PP_SEQ_FOR_EACH(REGISTER_GRAPH_FUNCTION, , GRAPH_FUNCTION_CLASSES)
}

}
