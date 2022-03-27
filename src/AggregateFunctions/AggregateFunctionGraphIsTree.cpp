#include "AggregateFunctionGraphOperation.h"
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes {
  extern const int UNSUPPORTED_PARAMETER;
}

class GraphIsTreeGeneral final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphIsTreeGeneral>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, GraphIsTreeGeneral>::GraphOperationGeneral;

    static constexpr const char* name = "isTree";

    DataTypePtr getReturnType() const override { return DataTypeFactory::instance().get("Bool"); }

    bool calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        return this->data(place).isTree();
    }
};

template void registerGraphAggregateFunction<GraphIsTreeGeneral>(AggregateFunctionFactory & factory);

}
