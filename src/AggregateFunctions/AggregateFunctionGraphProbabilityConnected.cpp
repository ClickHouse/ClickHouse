#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"

namespace DB
{

class GraphProbabilityConnected final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphProbabilityConnected>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, GraphProbabilityConnected>::GraphOperationGeneral;

    static constexpr const char* name = "graphProbabilityConnected";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeFloat64>(); }

    void visitComponent(ConstAggregateDataPtr place, StringRef vertex, HashSet<StringRef>& visited) const {
        HashSet<StringRef>::LookupResult it;
        bool inserted;
        visited.emplace(vertex, it, inserted);
        if (!inserted) {
          return;
        }
        for (StringRef to : this->data(place).graph.at(vertex)) {
            visitComponent(place, to, visited);
        }
    }

    Float64 calculateOperation(ConstAggregateDataPtr place, Arena*) const {
        const auto& graph = this->data(place).graph;
        if (graph.size() < 2) {
            return 1;
        }
        UInt64 connected_vertices = 0;
        HashSet<StringRef> visited;
        for (const auto& [from, to] : graph) {
          if (visited.find(from) == visited.end()) {
            UInt64 was_visited = visited.size();
            visitComponent(place, from, visited);
            UInt64 component_size = visited.size() - was_visited;
            connected_vertices += component_size * (component_size - 1);
          }
        }
        
        return static_cast<Float64>(connected_vertices) / graph.size() / (graph.size() - 1);
    }
};

template void registerGraphAggregateFunction<GraphProbabilityConnected>(AggregateFunctionFactory & factory);

}
