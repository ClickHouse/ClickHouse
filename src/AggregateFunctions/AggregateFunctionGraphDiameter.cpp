#include "AggregateFunctionGraphOperation.h"

namespace DB
{

class GraphDiameterGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData>::GraphOperationGeneral;

    String getName() const override { return "GraphDiameter"; }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, [[maybe_unused]] Arena* arena) const override {
        return this->data(place).edges_count;
    }
};

void registerAggregateFunctionGraphDiameter(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = false };

    factory.registerFunction("graphDiameter", { createGraphOperation<GraphDiameterGeneral>, properties });
}

}
