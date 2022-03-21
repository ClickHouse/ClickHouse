#include "AggregateFunctionGraphOperation.h"

#include <utility>

namespace DB
{

class GraphDiameterGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData>::GraphOperationGeneral;

    String getName() const override { return "GraphDiameter"; }

    std::pair<UInt64, StringRef> calculateDiameter(const StringRef& vertex, const StringRef& parent, const HashMap<StringRef, std::vector<StringRef>>& graph) {
        std::pair<UInt64, StringRef> answer = {0, vertex};
        for (const auto& next : graph.at(vertex)) {
            if (next == parent) {
                continue;
            }
            const auto& cur_answer = calculateDiameter(next, vertex, graph);
            cur_answer.first += 1;
            if (cur_answer.first > answer.first) {
                answer = cur_answer;
            }
        }
        return answer;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, [[maybe_unused]] Arena* arena) const override {
        const auto& graph = this->data(place).graph;
        if (graph.size() < 2) {
            return 0;
        }
        auto cur = calculateDiameter(graph.begin()->first, graph.begin()->first, graph);
        auto answer = calculateDiameter(cur.second, cur.second, graph);
        return answer.first;
        // return this->data(place).edges_count;
    }
};

void registerAggregateFunctionGraphDiameter(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = false };

    factory.registerFunction("graphDiameter", { createGraphOperation<GraphDiameterGeneral>, properties });
}

}
