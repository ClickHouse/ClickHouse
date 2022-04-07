#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_PARAMETER;
}

template<typename VertexType>
class TreeDiameter final : public GraphOperation<BidirectionalGraphData<VertexType>, TreeDiameter<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, TreeDiameter<VertexType>>)

    static constexpr const char * name = "TreeDiameter";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    std::pair<UInt64, Vertex> calculateDiameter(ConstAggregateDataPtr __restrict place, Vertex vertex, Vertex parent) const
    {
        std::pair<UInt64, Vertex> answer = {0, vertex};
        for (Vertex next : data(place).graph.at(vertex))
        {
            if (next == parent)
                continue;
            auto cur_answer = calculateDiameter(place, next, vertex);
            cur_answer.first += 1;
            if (cur_answer.first > answer.first)
                answer = cur_answer;
        }
        return answer;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        if (!data(place).isTree())
            throw Exception("Graph must have structure of tree", ErrorCodes::UNSUPPORTED_PARAMETER);
        const auto & graph = data(place).graph;
        if (graph.size() < 2)
            return 0;
        Vertex first_leaf = calculateDiameter(place, graph.begin()->getKey(), graph.begin()->getKey()).second;
        return calculateDiameter(place, first_leaf, first_leaf).first;
    }
};

INSTANTIATE_GRAPH_OPERATION(TreeDiameter)

}
