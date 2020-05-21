
#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>
#include <AggregateFunctions/Cuda/CudaAggregateFunctionCount.h>
#include <AggregateFunctions/Cuda/CudaAggregateFunctionUniq.h>
#include <AggregateFunctions/Cuda/createAggregateFunction.h>


namespace DB
{

CudaAggregateFunctionPtr createCudaAggregateFunctionCount()
{
    return std::make_shared<CudaAggregateFunctionCount>();
}

CudaAggregateFunctionPtr createCudaAggregateFunctionUniq()
{
    return std::make_shared<CudaAggregateFunctionUniq<String>>();
}

}
