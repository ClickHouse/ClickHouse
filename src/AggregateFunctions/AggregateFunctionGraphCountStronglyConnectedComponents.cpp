#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"
#include "Common/HashTable/HashSet.h"
#include <algorithm>

namespace DB
{

class GraphCountStronglyConnectedComponents final : public GraphOperationGeneral<DirectionalGraphGenericData, GraphCountStronglyConnectedComponents>
{
public:
    using GraphOperationGeneral<DirectionalGraphGenericData, GraphCountStronglyConnectedComponents>::GraphOperationGeneral;

    static constexpr const char* name = "countStronglyConnectedComponents";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void dfsOrder(const HashMap<StringRef, std::vector<StringRef>>& graph, StringRef vertex, HashSet<StringRef>& used, std::vector<StringRef>& order) const {
        used.insert(vertex);
        if (graph.find(vertex) != graph.end()) {
            for (StringRef next : graph.at(vertex)) {
                if (used.has(next)) {
                    continue;
                }
                dfsOrder(graph, next, used, order);
            }
        }
        order.emplace_back(vertex);
    }

    void dfsColor(const HashMap<StringRef, std::vector<StringRef>>& reverseGraph, StringRef vertex, HashSet<StringRef>& used) const {
        used.insert(vertex);
        for (StringRef next : reverseGraph.at(vertex)) {
            if (used.has(next)) {
                continue;
            }
            dfsColor(reverseGraph, next, used);
        }
    }

    HashMap<StringRef, std::vector<StringRef>> createReverseGraph(const HashMap<StringRef, std::vector<StringRef>>& graph) const {
        HashMap<StringRef, std::vector<StringRef>> reverseGraph;
        for (const auto& [vertex, neighbors] : graph) {
            if (reverseGraph.find(vertex) == reverseGraph.end()) {
                reverseGraph[vertex] = {};
            }
            for (StringRef next : neighbors) {
                reverseGraph[next].emplace_back(vertex);
            }
        }
        return reverseGraph;
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
        const auto& reverseGraph = createReverseGraph(graph);
        for (StringRef vertex : order) {
            if (used.has(vertex)) {
                continue;
            }
            dfsColor(reverseGraph, vertex, used);
            ++answer;
        }
        return answer;
    }
};

template void registerGraphAggregateFunction<GraphCountStronglyConnectedComponents>(AggregateFunctionFactory & factory);

}
