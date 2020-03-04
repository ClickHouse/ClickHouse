#pragma once 

#include <Core/Cuda/Types.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>


namespace DB
{

CudaAggregateFunctionPtr    createCudaAggregateFunctionCount();
CudaAggregateFunctionPtr    createCudaAggregateFunctionUniq();

}
