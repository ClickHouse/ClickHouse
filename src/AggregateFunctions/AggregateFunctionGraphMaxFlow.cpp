#include <algorithm>
#include <limits>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphDirectionalData.h"
#include "DataTypes/DataTypeNullable.h"
#include "base/types.h"

namespace DB
{
template <typename VertexType>
class GraphMaxFlow final : public GraphOperation<DirectionalGraphData<VertexType>, GraphMaxFlow<VertexType>, 2>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<DirectionalGraphData<VertexType>, GraphMaxFlow<VertexType>, 2>)

    static constexpr const char * name = "GraphMaxFlow";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()); }

    std::optional<UInt64> calculateOperation(ConstAggregateDataPtr __restrict place, Arena * arena) const
    {
        Vertex from = getVertexFromField(parameters.at(0), arena);
        Vertex to = getVertexFromField(parameters.at(1), arena);
        if (!data(place).graph.has(from) || !data(place).graph.has(to))
            return std::nullopt;
        if (from == to)
            return std::numeric_limits<UInt64>::max();
        MaxFlow helper(from, to);
        for (const auto & [vertex, neighbors] : data(place).graph)
            for (Vertex next : neighbors)
                helper.addEdge(vertex, next);
        return helper.getMaxFlow();
    }

private:
    struct Edge
    {
        Vertex to;
        Int64 capacity;
        Int64 flow = 0;
        Edge(Vertex to_, Int64 capacity_) : to(to_), capacity(capacity_) { }
        Int64 getCurrentCapacity() const { return capacity - flow; }
    };

    class MaxFlow
    {
    public:
        using Vertex = GraphMaxFlow::Vertex;

        MaxFlow(Vertex start, Vertex end) : starting_point{start}, ending_point{end} { }

        void addEdge(Vertex from, Vertex to, Int64 capacity = 1)
        {
            graph[from].emplace_back(edges.size());
            edges.emplace_back(to, capacity);
            graph[to].emplace_back(edges.size());
            edges.emplace_back(from, 0);
        }

        UInt64 getMaxFlow()
        {
            if (starting_point == ending_point)
            {
                return std::numeric_limits<Int64>::max();
            }
            UInt64 answer = 0;
            while (bfs())
            {
                while (true)
                {
                    UInt64 current_flow = dfs(starting_point);
                    if (!current_flow)
                        break;
                    answer += current_flow;
                }
            }
            return answer;
        }

    private:
        bool bfs()
        {
            distance.clear();
            std::queue<Vertex> buffer;
            buffer.push(starting_point);
            distance[starting_point] = 0;
            while (!buffer.empty())
            {
                const auto current = buffer.front();
                buffer.pop();
                for (const UInt64 id : graph.at(current))
                {
                    if (edges[id].getCurrentCapacity() < 1)
                        continue;
                    const auto to = edges[id].to;
                    if (distance.has(to))
                        continue;
                    distance[to] = distance[current] + 1;
                    buffer.push(to);
                }
            }
            return distance.has(ending_point);
        }

        UInt64 dfs(Vertex from, Int64 currentFlow = std::numeric_limits<Int64>::max())
        {
            std::vector<std::pair<Vertex, std::decay_t<decltype(graph.at(from).begin())>>> dfs_stack;
            dfs_stack.emplace_back(from, graph.at(from).begin());
            HashMap<Vertex, UInt64> par;
            HashMap<Vertex, Int64> hasFlow;
            hasFlow[from] = currentFlow;

            while (!dfs_stack.empty())
            {
                auto [vertex, it] = dfs_stack.back();
                if (vertex == ending_point)
                {
                    break;
                }
                dfs_stack.pop_back();
                if (it != graph.at(vertex).end())
                {
                    auto cp_it = it;
                    ++cp_it;
                    dfs_stack.emplace_back(vertex, cp_it);
                    UInt64 id = *it;
                    const auto to = edges[id].to;
                    if (edges[id].getCurrentCapacity() <= 0)
                    {
                        continue;
                    }
                    if (distance.at(vertex) + 1 != distance.at(to))
                    {
                        continue;
                    }
                    Int64 add = std::min(hasFlow[vertex], edges[id].getCurrentCapacity());
                    if (add > 0)
                    {
                        dfs_stack.emplace_back(to, graph.at(to).begin());
                        par[to] = id;
                        hasFlow[to] = add;
                    }
                }
            }

            currentFlow = hasFlow[ending_point];

            if (currentFlow == 0)
            {
                return 0;
            }

            Vertex cur = ending_point;

            while (cur != starting_point)
            {
                UInt64 id = par[cur];
                Vertex parent = edges[id ^ 1].to;
                edges[id].flow += currentFlow;
                edges[id ^ 1].flow -= currentFlow;
                cur = parent;
            }

            return currentFlow;
        }

        HashMap<Vertex, UInt64> distance{};
        HashMap<Vertex, std::vector<UInt64>> graph{};
        std::vector<Edge> edges{};
        Vertex starting_point, ending_point;
    };
};

INSTANTIATE_GRAPH_OPERATION(GraphMaxFlow)

}
