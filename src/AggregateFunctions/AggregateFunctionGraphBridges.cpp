#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"

namespace DB
{

namespace ErrorCodes 
{
extern const int UNSUPPORTED_PARAMETER;
}

class GraphCountBridges final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphCountBridges>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, GraphCountBridges>::GraphOperationGeneral;

    static constexpr const char* name = "countBridges";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 min(UInt64 a, UInt64 b) const {
        if (a < b) {
            return a;
        } else {
            return b;
        }
    }

    void countBridges(ConstAggregateDataPtr __restrict place, StringRef vertex, StringRef parent, HashMap<StringRef, bool>& used, HashMap<StringRef, UInt64>& tin, HashMap<StringRef, UInt64>& up, UInt64& cntBridges, UInt64& timer) const {
        used[vertex] = true;
        tin[vertex] = timer;
        up[vertex] = timer;
        ++timer;

        for (StringRef next : this->data(place).graph.at(vertex)) {
            if (next == parent) {
                continue;
            } else if (used[next]) {
                up[vertex] = min(up[vertex], tin[next]);
            } else {
                countBridges(place, next, vertex, used, tin, up, cntBridges, timer);
                up[vertex] = min(up[vertex], up[next]);
                if (up[next] > tin[vertex]) {
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
        HashMap<StringRef, bool> used;
        HashMap<StringRef, UInt64> tin, up;
        UInt64 cntBridges = 0;
        UInt64 timer = 0;
        countBridges(place, graph.begin()->getKey(), graph.begin()->getKey(), used, tin, up, cntBridges, timer);
        return cntBridges;
    }
};

template void registerGraphAggregateFunction<GraphCountBridges>(AggregateFunctionFactory & factory);

}
