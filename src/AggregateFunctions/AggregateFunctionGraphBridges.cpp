#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "base/types.h"

namespace DB
{

template<typename VertexType>
class GraphCountBridges final : public GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBridges<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBridges<VertexType>>)

    static constexpr const char * name = "GraphCountBridges";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void countBridges(
        ConstAggregateDataPtr __restrict place,
        Vertex vertex,
        Vertex parent,
        VertexSet & used,
        HashMap<Vertex, UInt64> & tin,
        HashMap<Vertex, UInt64> & up,
        UInt64 & cntBridges,
        UInt64 & timer) const
    {
        used.insert(vertex);
        tin[vertex] = timer;
        up[vertex] = timer;
        ++timer;

        for (Vertex next : data(place).graph.at(vertex))
        {
            if (next == parent)
                continue;
            else if (used.has(next))
                up[vertex] = std::min(up[vertex], tin[next]);
            else
            {
                countBridges(place, next, vertex, used, tin, up, cntBridges, timer);
                up[vertex] = std::min(up[vertex], up[next]);
                if (up[next] > tin[vertex])
                    ++cntBridges;
            }
        }
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        const auto & graph = data(place).graph;
        if (graph.size() < 2)
            return 0;
        VertexSet used;
        HashMap<Vertex, UInt64> tin, up;
        UInt64 cnt_bridges = 0;
        UInt64 timer = 0;
        countBridges(place, graph.begin()->getKey(), graph.begin()->getKey(), used, tin, up, cnt_bridges, timer);
        return cnt_bridges;
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphCountBridges)

}
