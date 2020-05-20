
#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>
#include <AggregateFunctions/Cuda/CudaAggregateFunctionCount.h>
#include <AggregateFunctions/Cuda/CudaAggregateFunctionUniq.h>
#include <AggregateFunctions/Cuda/createAggregateFunction.h>


namespace DB
{

CudaAggregateFunctionPtr    createCudaAggregateFunctionCount()
{
    return CudaAggregateFunctionPtr(new CudaAggregateFunctionCount());
}

CudaAggregateFunctionPtr    createCudaAggregateFunctionUniq()
{
    return CudaAggregateFunctionPtr(new CudaAggregateFunctionUniq<String, CudaAggregateFunctionUniqHLL12Data>());
}

}
