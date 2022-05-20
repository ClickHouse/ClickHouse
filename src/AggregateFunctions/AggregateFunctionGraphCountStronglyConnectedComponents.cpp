#include <algorithm>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphDirectionalData.h"
#include "base/types.h"

namespace DB
{
template <typename VertexType>
class GraphCountStronglyConnectedComponents final
    : public GraphOperation<DirectionalGraphData<VertexType>, GraphCountStronglyConnectedComponents<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<DirectionalGraphData<VertexType>, GraphCountStronglyConnectedComponents<VertexType>>)

    static constexpr const char * name = "GraphCountStronglyConnectedComponents";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void dfsOrder(const GraphType & graph, Vertex from, VertexSet & used, std::vector<Vertex> & order) const
    {
        std::vector<std::pair<Vertex, std::decay_t<decltype(graph.at(from).begin())>>> dfs_stack;
        dfs_stack.emplace_back(from, graph.at(from).begin());
        used.insert(from);
        while (!dfs_stack.empty()) {
            auto [vertex, it] = dfs_stack.back();
            dfs_stack.pop_back();
            if (it == graph.at(vertex).end()) {
                order.push_back(vertex);
            } else {
                auto cp_it = it;
                ++cp_it;
                dfs_stack.emplace_back(vertex, cp_it);
                if (!used.has(*it)) {
                    Vertex next = *it;
                    dfs_stack.emplace_back(next, graph.at(next).begin());
                    used.insert(next);
                }
            }
        }
    }

    void dfsColor(const GraphType & reverseGraph, Vertex vertex, VertexSet & used) const
    {
        std::queue<Vertex> buff;
        buff.push(vertex);
        used.insert(vertex);
        while (!buff.empty()) {
            vertex = buff.front();
            buff.pop();
            for (Vertex next : reverseGraph.at(vertex)) {
                if (!used.has(next)) {
                    buff.push(next);
                    used.insert(next);
                }
            }
        }
        // typename VertexSet::LookupResult it;
        // bool inserted;
        // used.emplace(vertex, it, inserted);
        // if (!inserted)
        //     return;
        // for (Vertex next : reverseGraph.at(vertex))
        //     dfsColor(reverseGraph, next, used);
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
