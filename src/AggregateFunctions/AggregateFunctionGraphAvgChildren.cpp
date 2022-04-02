#include "AggregateFunctionGraphOperation.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"

namespace DB
{
class GraphAvgChildrenGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphAvgChildrenGeneral>
{
public:
    using GraphOperationGeneral::GraphOperationGeneral;

    static constexpr const char * name = "graphAvgChildren";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeFloat64>(); }

    Float64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        return static_cast<Float64>(this->data(place).edges_count) / this->data(place).graph.size();
    }
};

template void registerGraphAggregateFunction<GraphAvgChildrenGeneral>(AggregateFunctionFactory & factory);

}
