#include <optional>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "AggregateFunctions/AggregateFunctionGraphDirectionalData.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"

namespace DB
{
template <typename VertexType>
class GraphCountBipartiteMaximumMatching final
    : public GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBipartiteMaximumMatching<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBipartiteMaximumMatching<VertexType>>)

    static constexpr const char * name = "GraphCountBipartiteMaximumMatching";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()); }

    bool isBipartite(const GraphType & graph, Vertex vertex, HashMap<Vertex, bool> & color, bool currentColor = true) const
    {
        std::queue<Vertex> buff;
        color[vertex] = currentColor;
        buff.push(vertex);
        while (!buff.empty())
        {
            Vertex cur = buff.front();
            buff.pop();
            for (Vertex next : graph.at(cur))
            {
                if (!color.has(next))
                {
                    color[next] = true ^ color[cur];
                    buff.push(next);
                }
                else if (color[next] == color[cur])
                {
                    return false;
                }
            }
        }
        return true;
    }

    std::optional<HashMap<Vertex, bool>> getColor(const GraphType & graph) const
    {
        HashMap<Vertex, bool> color;
        for (const auto & [vertex, neighbours] : graph)
            if (!color.has(vertex))
                if (!isBipartite(graph, vertex, color))
                    return std::nullopt;
        return color;
    }

    bool dfsMatch(Vertex from, UInt64 currentColor, const GraphType & graph, HashMap<Vertex, UInt64> & used, VertexMap & matching) const
    {
        std::vector<std::pair<Vertex, std::decay_t<decltype(graph.at(from).begin())>>> dfs_stack;
        dfs_stack.emplace_back(from, graph.at(from).begin());
        used[from] = currentColor;
        while (!dfs_stack.empty())
        {
            auto [vertex, it] = dfs_stack.back();
            dfs_stack.pop_back();
            if (it == graph.at(vertex).end())
            {
                continue;
            }
            auto cp_it = it;
            ++cp_it;
            dfs_stack.emplace_back(vertex, cp_it);
            if (it == graph.at(vertex).begin())
            {
                for (auto next : graph.at(vertex))
                {
                    if (!matching.has(next))
                    {
                        while (!dfs_stack.empty())
                        {
                            auto [cur_vertex, next_it] = dfs_stack.back();
                            dfs_stack.pop_back();
                            --next_it;
                            matching[*next_it] = cur_vertex;
                        }
                        return true;
                    }
                }
            }
            Vertex next = *it;
            if (used[matching[next]] != currentColor)
            {
                dfs_stack.emplace_back(matching[next], graph.at(matching[next]).begin());
                used[matching[next]] = currentColor;
            }
        }

        return false;
    }

    std::optional<UInt64> calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        const auto & graph = data(place).graph;
        if (graph.empty())
            return 0;
        const auto color = getColor(graph);
        if (color == std::nullopt)
            return std::nullopt;
        HashMap<Vertex, UInt64> used;
        VertexMap matching;
        UInt64 current_color = 0;
        UInt64 matching_size = 0;
        for (const auto & [vertex, neighbours] : graph)
        {
            if (color->at(vertex))
            {
                for (auto next : neighbours)
                {
                    if (!matching.has(next))
                    {
                        matching[next] = vertex;
                        used[vertex] = ++current_color;
                        ++matching_size;
                        break;
                    }
                }
            }
        }
        for (const auto & [vertex, neighbours] : graph)
            if (color->at(vertex) && !used.has(vertex))
                if (dfsMatch(vertex, ++current_color, graph, used, matching))
                    ++matching_size;
        return matching_size;
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphCountBipartiteMaximumMatching)

}
