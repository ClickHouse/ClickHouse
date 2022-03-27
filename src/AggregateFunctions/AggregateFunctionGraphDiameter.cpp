#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"

namespace DB
{

namespace ErrorCodes {
  extern const int UNSUPPORTED_PARAMETER;
}

class GraphDiameterGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphDiameterGeneral>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, GraphDiameterGeneral>::GraphOperationGeneral;

    static constexpr const char* name = "treeDiameter";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    std::pair<UInt64, StringRef> calculateDiameter(ConstAggregateDataPtr __restrict place, StringRef vertex, StringRef parent) const {
        std::pair<UInt64, StringRef> answer = {0, vertex};
        for (StringRef next : this->data(place).graph.at(vertex)) {
            if (next == parent) {
                continue;
            }
            auto cur_answer = calculateDiameter(place, next, vertex);
            cur_answer.first += 1;
            if (cur_answer.first > answer.first) {
                answer = cur_answer;
            }
        }
        return answer;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        if (!this->data(place).isTree()) {
          throw Exception("Graph must have structure of tree", ErrorCodes::UNSUPPORTED_PARAMETER); 
        }
        const auto& graph = this->data(place).graph;
        if (graph.size() < 2) {
            return 0;
        }
        StringRef first_leaf = calculateDiameter(place, graph.begin()->getKey(), graph.begin()->getKey()).second;
        return calculateDiameter(place, first_leaf, first_leaf).first;
    }
};

template void registerGraphAggregateFunction<GraphDiameterGeneral>(AggregateFunctionFactory & factory);

}
