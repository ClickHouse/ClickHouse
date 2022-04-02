#include <DataTypes/DataTypeFactory.h>
#include "AggregateFunctionGraphOperation.h"

namespace DB
{
class GraphIsBipartiteGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphIsBipartiteGeneral>
{
public:
    using GraphOperationGeneral::GraphOperationGeneral;

    static constexpr const char * name = "isBipartite";

    DataTypePtr getReturnType() const override { return DataTypeFactory::instance().get("Bool"); }

    bool isBipartite(
        const HashMap<StringRef, std::vector<StringRef>> & graph,
        StringRef vertex,
        HashMap<StringRef, bool> & color,
        bool currentColor = true) const
    {
        color[vertex] = currentColor;
        for (StringRef next : graph.at(vertex))
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
        const auto & graph = this->data(place).graph;
        HashMap<StringRef, bool> color;
        for (const auto & [vertex, neighbours] : graph)
            if (color.find(vertex) == color.end())
                if (!isBipartite(graph, vertex, color))
                    return false;
        return true;
    }
};

template void registerGraphAggregateFunction<GraphIsBipartiteGeneral>(AggregateFunctionFactory & factory);

}
