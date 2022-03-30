#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"

namespace DB
{

class GraphCountBridges final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphCountBridges>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, GraphCountBridges>::GraphOperationGeneral;

    static constexpr const char* name = "countBridges";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void countBridges(ConstAggregateDataPtr __restrict place, StringRef vertex, StringRef parent, HashSet<StringRef>& used, HashMap<StringRef, UInt64>& tin, HashMap<StringRef, UInt64>& up, UInt64& cntBridges, UInt64& timer) const {
        used.insert(vertex);
        tin[vertex] = timer;
        up[vertex] = timer;
        ++timer;

        for (StringRef next : this->data(place).graph.at(vertex)) {
            if (next == parent) {
                continue;
            } else if (used.find(vertex) != used.end()) {
                auto vup = up.find(vertex);
                *vup = std::min(*vup, tin[next]);
            } else {
                countBridges(place, next, vertex, used, tin, up, cntBridges, timer);
                auto vup = up.find(vertex);
                auto nup = up.find(next);
                *vup = std::min(*vup, *nup);
                if (*nup > tin[vertex]) {
                    ++cntBridges;
                }
            }
        }
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        const auto& graph = this->data(place).graph;
        if (graph.size() < 2) {
            return 0;
        }
        HashSet<StringRef> used;
        HashMap<StringRef, UInt64> tin, up;
        UInt64 cntBridges = 0;
        UInt64 timer = 0;
        countBridges(place, graph.begin()->getKey(), graph.begin()->getKey(), used, tin, up, cntBridges, timer);
        return cntBridges;
    }
};

template void registerGraphAggregateFunction<GraphCountBridges>(AggregateFunctionFactory & factory);

}
