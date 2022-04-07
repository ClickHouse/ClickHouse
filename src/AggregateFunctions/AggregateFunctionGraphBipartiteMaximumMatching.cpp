#include <optional>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "AggregateFunctions/AggregateFunctionGraphDirectionalData.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_PARAMETER;
}

template<typename VertexType>
class GraphCountBipartiteMaximumMatching final
    : public GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBipartiteMaximumMatching<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBipartiteMaximumMatching<VertexType>>)

    static constexpr const char * name = "GraphCountBipartiteMaximumMatching";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    bool isBipartite(
        const GraphType & graph,
        Vertex vertex,
        HashMap<Vertex, bool> & color,
        bool currentColor = true) const
    {
        color[vertex] = currentColor;
        for (Vertex next : graph.at(vertex))
        {
            if (!color.has(next))
            {
                if (!isBipartite(graph, next, color, true ^ currentColor))
                    return false;
            }
            else if (color[next] == currentColor)
                return false;
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
        return std::make_optional(std::move(color));
    }

    bool dfsMatch(
        Vertex vertex,
        UInt64 currentColor,
        const GraphType & graph,
        HashMap<Vertex, UInt64> & used,
        VertexMap & matching) const
    {
        if (std::exchange(used[vertex], currentColor) == currentColor)
            return false;
        for (Vertex next : graph.at(vertex))
            if (!matching.has(next))
            {
                matching[next] = vertex;
                return true;
            }
        for (Vertex next : graph.at(vertex))
            if (dfsMatch(matching[next], currentColor, graph, used, matching))
            {
                matching[next] = vertex;
                return true;
            }
        return false;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        const auto & graph = data(place).graph;
        if (graph.empty())
            return 0;
        const auto color = getColor(graph);
        if (color == std::nullopt)
            throw Exception("Graph must be bipartite", ErrorCodes::UNSUPPORTED_PARAMETER);
        HashMap<Vertex, UInt64> used;
        VertexMap matching;
        UInt64 current_color = 0;
        UInt64 matching_size = 0;
        for (const auto & [vertex, neighbours] : graph)
            if (color->at(vertex))
                if (dfsMatch(vertex, ++current_color, graph, used, matching))
                    ++matching_size;
        return matching_size;
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphCountBipartiteMaximumMatching)

}
