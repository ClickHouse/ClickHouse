#include <limits>
#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"

namespace DB
{

class EdgeDistanceGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, EdgeDistanceGeneral, 2>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, EdgeDistanceGeneral, 2>::GraphOperationGeneral;

    static constexpr const char* name = "edgeDistance";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena* arena) const {
        StringRef from = serializeFieldToArena(parameters.at(0), arena);
        StringRef to = serializeFieldToArena(parameters.at(1), arena);
        if (from == to) {
            return 0;
        }
        HashSet<StringRef> visited;
        std::queue<std::pair<StringRef, UInt64>> buffer;
        buffer.emplace(from, 0);
        while (!buffer.empty()) {
            auto [vertex, distance] = buffer.front();
            buffer.pop();
            HashSet<StringRef>::LookupResult it;
            bool inserted;
            visited.emplace(vertex, it, inserted);
            if (!inserted) {
                continue;
            }
            for (StringRef next_vertex : this->data(place).graph.at(vertex)) {
                if (next_vertex == to) {
                    return distance + 1;
                }
                buffer.emplace(next_vertex, distance + 1);
            }
        }
        return std::numeric_limits<UInt64>::max();
    }
};

template void registerGraphAggregateFunction<EdgeDistanceGeneral>(AggregateFunctionFactory & factory);

}
