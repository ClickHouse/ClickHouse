#pragma once

namespace DB
{

class ICudaAggregateFunction;
using CudaAggregateFunctionPtr = std::shared_ptr<ICudaAggregateFunction>;

CudaAggregateFunctionPtr createCudaAggregateFunctionCount();
CudaAggregateFunctionPtr createCudaAggregateFunctionUniq();

}
