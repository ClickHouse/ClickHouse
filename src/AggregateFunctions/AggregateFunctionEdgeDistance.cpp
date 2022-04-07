#include <limits>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "base/types.h"

namespace DB
{

template <typename VertexType>
class EdgeDistance final : public GraphOperation<BidirectionalGraphData<VertexType>, EdgeDistance<VertexType>, 2>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, EdgeDistance<VertexType>, 2>)

    static constexpr const char* name = "EdgeDistance";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena* arena) const {
        Vertex from = getVertexFromField(this->parameters.at(0), arena);
        Vertex to = getVertexFromField(this->parameters.at(1), arena);
        if (from == to) {
            return 0;
        }
        VertexSet visited;
        std::queue<std::pair<Vertex, UInt64>> buffer;
        buffer.emplace(from, 0);
        while (!buffer.empty()) {
            auto [vertex, distance] = buffer.front();
            buffer.pop();
            typename VertexSet::LookupResult it;
            bool inserted;
            visited.emplace(vertex, it, inserted);
            if (!inserted) {
                continue;
            }
            for (Vertex next_vertex : data(place).graph.at(vertex)) {
                if (next_vertex == to) {
                    return distance + 1;
                }
                buffer.emplace(next_vertex, distance + 1);
            }
        }
        return std::numeric_limits<UInt64>::max();
    }
};

INSTANTIATE_GRAPH_OPERATION(EdgeDistance)

}
