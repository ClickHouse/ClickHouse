#pragma once

#include <string>
#include <memory>
#include <cuda.h>
#include <cuda_runtime.h>

#include <Core/Cuda/Types.h>
#include <Columns/Cuda/CudaColumnString.h>

namespace DB
{

using CudaAggregateDataPtr = char*;
//using AggregateDataPtr = char*;

class ICudaAggregateFunction
{
public:
    typedef UInt32      CudaSizeType;
    typedef UInt64      ResultType;

    virtual size_t      cudaSizeOfData() const = 0;
    virtual bool        isDataNeeded() const = 0;
    virtual void        cudaInitAggregateData(CudaSizeType places_num, CudaAggregateDataPtr places, cudaStream_t stream = 0) const = 0;
    virtual size_t      cudaSizeOfAddBulkInternalBuf(CudaSizeType max_elements_num) = 0;
    virtual void        cudaAddBulk(CudaAggregateDataPtr places, CudaColumnStringPtr str_column,
                                    CudaSizeType elements_num, CudaSizeType *res_buckets, 
                                    char *tmp_buf, cudaStream_t stream = 0) const = 0;
    virtual void        cudaMergeBulk(CudaAggregateDataPtr places, CudaSizeType elements_num,
                                      CudaAggregateDataPtr places_from, CudaSizeType *res_buckets, 
                                      cudaStream_t stream = 0) const = 0;
    virtual ResultType  getResult(CudaAggregateDataPtr place) const = 0;

    virtual ~ICudaAggregateFunction() {}
};

using CudaAggregateFunctionPtr = std::shared_ptr<ICudaAggregateFunction>;


}
