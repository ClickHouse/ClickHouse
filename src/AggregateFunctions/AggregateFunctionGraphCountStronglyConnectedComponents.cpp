#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"
#include "Common/HashTable/HashSet.h"
#include <algorithm>

namespace DB
{

class GraphCountStronglyConnectedComponents final : public GraphOperationGeneral<DirectionalGraphGenericData, GraphCountStronglyConnectedComponents>
{
public:
    using GraphOperationGeneral::GraphOperationGeneral;

    static constexpr const char* name = "countStronglyConnectedComponents";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void dfsOrder(const HashMap<StringRef, std::vector<StringRef>>& graph, StringRef vertex, HashSet<StringRef>& used, std::vector<StringRef>& order) const {
        HashSet<StringRef>::LookupResult it;
        bool inserted;
        used.emplace(vertex, it, inserted);
        if (!inserted) {
            return;
        }
        if (const auto* graph_iter = graph.find(vertex); graph_iter) {
            for (StringRef next : graph_iter->getMapped()) {
                dfsOrder(graph, next, used, order);
            }
        }
        order.emplace_back(vertex);
    }

    void dfsColor(const HashMap<StringRef, std::vector<StringRef>>& reverseGraph, StringRef vertex, HashSet<StringRef>& used) const {
        HashSet<StringRef>::LookupResult it;
        bool inserted;
        used.emplace(vertex, it, inserted);
        if (!inserted) {
            return;
        }
        for (StringRef next : reverseGraph.at(vertex)) {
            dfsColor(reverseGraph, next, used);
        }
    }

    static HashMap<StringRef, std::vector<StringRef>> createReverseGraph(const HashMap<StringRef, std::vector<StringRef>>& graph)  {
        HashMap<StringRef, std::vector<StringRef>> reverse_graph;
        for (const auto& [vertex, neighbors] : graph) {
            reverse_graph.insert({vertex, {}});
            for (StringRef next : neighbors) {
                reverse_graph[next].emplace_back(vertex);
            }
        }
        return reverse_graph;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        const auto& graph = this->data(place).graph;
        if (graph.size() < 2) {
            return graph.size();
        }
        HashSet<StringRef> used;
        std::vector<StringRef> order;
        order.reserve(graph.size());
        for (const auto& [vertex, neighbors] : graph) {
            if (used.has(vertex)) {
                continue;
            }
            dfsOrder(graph, vertex, used, order);
        }
        std::reverse(order.begin(), order.end());
        UInt64 answer = 0;
        used = {};
        const auto& reverse_graph = createReverseGraph(graph);
        for (StringRef vertex : order) {
            if (used.has(vertex)) {
                continue;
            }
            dfsColor(reverse_graph, vertex, used);
            ++answer;
        }
        return answer;
    }
};

template void registerGraphAggregateFunction<GraphCountStronglyConnectedComponents>(AggregateFunctionFactory & factory);

}
