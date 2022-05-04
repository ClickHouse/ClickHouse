#pragma once

#include <base/types.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>


namespace DB
{

CudaAggregateFunctionPtr createCudaAggregateFunctionCount();
CudaAggregateFunctionPtr createCudaAggregateFunctionUniq();

}
