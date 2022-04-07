#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctionGraphDirectionalData.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"

namespace DB
{

template<typename DataType>
class GraphAvgChildren final : public GraphOperation<DirectionalGraphData<DataType>, GraphAvgChildren<DataType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<DirectionalGraphData<DataType>, GraphAvgChildren<DataType>>)

    static constexpr const char * name = "GraphAvgChildren";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeFloat64>(); }

    Float64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        return static_cast<Float64>(data(place).edges_count) / data(place).graph.size();
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphAvgChildren)

}
