#include "Common/HashTable/HashSet.h"
#include "AggregateFunctionGraphOperation.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"

namespace DB
{
class GraphProbabilityConnected final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphProbabilityConnected>
{
public:
    using GraphOperationGeneral::GraphOperationGeneral;

    static constexpr const char * name = "graphProbabilityConnected";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeFloat64>(); }

    Float64 calculateOperation(ConstAggregateDataPtr place, Arena *) const
    {
        const auto & graph = this->data(place).graph;
        if (graph.size() < 2)
            return 1;
        UInt64 connected_vertices = 0;
        HashSet<StringRef> visited;
        for (const auto & [from, _] : graph) 
        {
            if (!visited.has(from))
            {
                UInt64 component_size = this->data(place).componentSize(from, &visited);
                connected_vertices += component_size * (component_size - 1);
            }
        }
        return static_cast<Float64>(connected_vertices) / graph.size() / (graph.size() - 1);
    }
};

template void registerGraphAggregateFunction<GraphProbabilityConnected>(AggregateFunctionFactory & factory);

}
