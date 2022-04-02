#include "AggregateFunctionGraphOperation.h"

namespace DB
{

class GraphComponentsCountGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphComponentsCountGeneral>
{
public:
    using GraphOperationGeneral::GraphOperationGeneral;

    static constexpr const char* name = "graphComponentsCount";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        return this->data(place).componentsCount();
    }
};

template void registerGraphAggregateFunction<GraphComponentsCountGeneral>(AggregateFunctionFactory & factory);

}
