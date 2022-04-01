#include <limits>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"
#include <algorithm>

namespace DB
{

class GraphMaxFlow final : public GraphOperationGeneral<DirectionalGraphGenericData, GraphMaxFlow, 2>
{
public:
    using GraphOperationGeneral<DirectionalGraphGenericData, GraphMaxFlow, 2>::GraphOperationGeneral;

    static constexpr const char* name = "maxFlow";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena* arena) const {
        StringRef from = serializeFieldToArena(parameters.at(0), arena);
        StringRef to = serializeFieldToArena(parameters.at(1), arena);
        if (from == to) {
            return std::numeric_limits<UInt64>::max();
        }
        const auto& graph = this->data(place).graph;
        MaxFlow helper(from, to);
        for (const auto& [vertex, neighbors] : graph) {
            for (StringRef next : neighbors) {
                helper.addEdge(vertex, next);
            }
        }
        return helper.getMaxFlow();
    }
private:
    struct Edge{
        StringRef to;
        Int64 capacity, flow;
        Edge(StringRef to_, Int64 capacity_) : to(to_), capacity(capacity_), flow(0) {} 
        Int64 getCurrentCapacity() const {
            return capacity - flow;
        }
    };

    struct MaxFlow{
        HashMap<StringRef, std::vector<UInt64>> graph;
        std::vector<Edge> edges;
        StringRef from, to;

        MaxFlow(StringRef from_, StringRef to_) : graph(), edges(), from(from_), to(to_) {}

        void addEdge(StringRef from_, StringRef to_, Int64 capacity = 1) {
            graph[from_].emplace_back(edges.size());
            edges.emplace_back(to_, capacity);
            graph[to_].emplace_back(edges.size());
            edges.emplace_back(from_, 0);
        }

        HashMap<StringRef, UInt64> distance;

        bool bfs() {
            distance.clear();
            std::queue<StringRef> buffer;
            buffer.push(from);
            distance[from] = 0;
            while (!buffer.empty()) {
                const auto current = buffer.front();
                buffer.pop();
                for (const UInt64 id : graph.at(current)) {
                    if (edges[id].getCurrentCapacity() < 1) {
                        continue;
                    }
                    const auto to_ = edges[id].to;
                    if (distance.has(to_)) {
                        continue;
                    }
                    distance[to_] = distance[current] + 1;
                    buffer.push(to_);
                }
            }
            return distance.has(to);
        }

        UInt64 dfs(StringRef vertex, Int64 currentFlow = std::numeric_limits<Int64>::max()) {
            if (vertex == to) {
                return currentFlow;
            }
            for (const UInt64 id : graph.at(vertex)) {
                if (edges[id].getCurrentCapacity() < 1) {
                    continue;
                }
                const auto to_ = edges[id].to;
                if (distance.at(vertex) + 1 != distance.at(to_)) {
                    continue;
                }
                UInt64 add = dfs(to_, std::min(currentFlow, edges[id].getCurrentCapacity()));
                if (add > 0) {
                    edges[id].flow += add;
                    edges[id ^ 1].flow -= add;
                    return add;
                }
            }
            return 0;
        }

        UInt64 getMaxFlow() {
            UInt64 answer = 0;
            while (bfs()) {
                while (true) {
                    UInt64 currentFlow = dfs(from);
                    if (!currentFlow) {
                        break;
                    } 
                    answer += currentFlow;
                }
            }
            return answer;
        }
    };
};

template void registerGraphAggregateFunction<GraphMaxFlow>(AggregateFunctionFactory & factory);

}
