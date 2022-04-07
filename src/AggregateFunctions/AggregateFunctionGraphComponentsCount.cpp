#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"

namespace DB
{

template<typename VertexType>
class GraphComponentsCount final : public GraphOperation<BidirectionalGraphData<VertexType>, GraphComponentsCount<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphComponentsCount<VertexType>>)

    static constexpr const char * name = "GraphComponentsCount";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const { return data(place).componentsCount(); }
};

INSTANTIATE_GRAPH_OPERATION(GraphComponentsCount)

}
