#include <DataTypes/DataTypeFactory.h>
#include "AggregateFunctionGraphOperation.h"

namespace DB
{
class GraphIsTreeGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphIsTreeGeneral>
{
public:
    using GraphOperationGeneral::GraphOperationGeneral;

    static constexpr const char * name = "isTree";

    DataTypePtr getReturnType() const override { return DataTypeFactory::instance().get("Bool"); }

    bool calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const { return this->data(place).isTree(); }
};

template void registerGraphAggregateFunction<GraphIsTreeGeneral>(AggregateFunctionFactory & factory);

}
