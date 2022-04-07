#include <DataTypes/DataTypeFactory.h>
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"

namespace DB
{

template<typename VertexType>
class GraphIsBipartite final : public GraphOperation<BidirectionalGraphData<VertexType>, GraphIsBipartite<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphIsBipartite<VertexType>>)

    static constexpr const char * name = "GraphIsBipartite";

    DataTypePtr getReturnType() const override { return DataTypeFactory::instance().get("Bool"); }

    bool isBipartite(
        const GraphType & graph,
        Vertex vertex,
        HashMap<Vertex, bool> & color,
        bool currentColor = true) const
    {
        color[vertex] = currentColor;
        for (Vertex next : graph.at(vertex))
        {
            if (auto * color_next = color.find(next); color_next == nullptr)
            {
                if (!isBipartite(graph, next, color, true ^ currentColor))
                    return false;
            }
            else if (color_next->getMapped() == currentColor)
                return false;
        }
        return true;
    }

    bool calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        const auto & graph = data(place).graph;
        HashMap<Vertex, bool> color;
        for (const auto & [vertex, neighbours] : graph)
            if (!color.has(vertex))
                if (!isBipartite(graph, vertex, color))
                    return false;
        return true;
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphIsBipartite)

}
