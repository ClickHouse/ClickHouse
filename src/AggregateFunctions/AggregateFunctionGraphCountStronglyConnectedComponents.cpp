#include <algorithm>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphDirectionalData.h"
#include "base/types.h"

namespace DB
{

template<typename VertexType>
class GraphCountStronglyConnectedComponents final
    : public GraphOperation<DirectionalGraphData<VertexType>, GraphCountStronglyConnectedComponents<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<DirectionalGraphData<VertexType>, GraphCountStronglyConnectedComponents<VertexType>>)

    static constexpr const char * name = "GraphCountStronglyConnectedComponents";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void dfsOrder(
        const GraphType & graph,
        Vertex vertex,
        VertexSet & used,
        std::vector<Vertex> & order) const
    {
        typename VertexSet::LookupResult it;
        bool inserted;
        used.emplace(vertex, it, inserted);
        if (!inserted)
            return;
        if (const auto * graph_iter = graph.find(vertex); graph_iter)
            for (Vertex next : graph_iter->getMapped())
                dfsOrder(graph, next, used, order);
        order.emplace_back(vertex);
    }

    void dfsColor(const GraphType & reverseGraph, Vertex vertex, VertexSet & used) const
    {
        typename VertexSet::LookupResult it;
        bool inserted;
        used.emplace(vertex, it, inserted);
        if (!inserted)
            return;
        for (Vertex next : reverseGraph.at(vertex))
            dfsColor(reverseGraph, next, used);
    }

    static GraphType createReverseGraph(const GraphType & graph)
    {
        GraphType reverse_graph;
        for (const auto & [vertex, neighbors] : graph)
        {
            reverse_graph.insert({vertex, {}});
            for (Vertex next : neighbors)
                reverse_graph[next].emplace_back(vertex);
        }
        return reverse_graph;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        const auto & graph = data(place).graph;
        if (graph.size() < 2)
            return graph.size();
        VertexSet used;
        std::vector<Vertex> order;
        order.reserve(graph.size());
        for (const auto & [vertex, neighbors] : graph)
        {
            if (used.has(vertex))
                continue;
            dfsOrder(graph, vertex, used, order);
        }
        std::reverse(order.begin(), order.end());
        UInt64 answer = 0;
        used = {};
        const auto & reverse_graph = createReverseGraph(graph);
        for (Vertex vertex : order)
        {
            if (used.has(vertex))
                continue;
            dfsColor(reverse_graph, vertex, used);
            ++answer;
        }
        return answer;
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphCountStronglyConnectedComponents)

}
