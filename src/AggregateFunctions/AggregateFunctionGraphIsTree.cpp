#include <DataTypes/DataTypeFactory.h>
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"

namespace DB
{

template<typename VertexType>
class GraphIsTree final : public GraphOperation<BidirectionalGraphData<VertexType>, GraphIsTree<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphIsTree<VertexType>>)

    static constexpr const char * name = "GraphIsTree";

    DataTypePtr getReturnType() const override { return DataTypeFactory::instance().get("Bool"); }

    bool calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const { return data(place).isTree(); }
};

INSTANTIATE_GRAPH_OPERATION(GraphIsTree)

}
