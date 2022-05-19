#include <DataTypes/DataTypeFactory.h>
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "base/types.h"

namespace DB
{
template <typename VertexType>
class GraphIsBipartite final : public GraphOperation<BidirectionalGraphData<VertexType>, GraphIsBipartite<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphIsBipartite<VertexType>>)

    static constexpr const char * name = "GraphIsBipartite";

    DataTypePtr getReturnType() const override { return DataTypeFactory::instance().get("Bool"); }


    bool isBipartite(const GraphType & graph, Vertex vertex, HashMap<Vertex, bool> & color, bool currentColor = true) const
    {
        std::queue<Vertex> buff;
        color[vertex] = currentColor;
        buff.push(vertex);
        while (!buff.empty()) {
            Vertex cur = buff.front();
            buff.pop();
            for (Vertex next : graph.at(cur)) {
                if (!color.has(next)) {
                    color[next] = true ^ color[cur];
                    buff.push(next);
                } else if (color[next] == color[cur]) {
                    return false;
                }
            }
        }
        return true;
    }

    UInt8 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
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
