#pragma once

#include <memory>

#include <Common/Cuda/common.h>
#include <Core/Cuda/Types.h>

namespace DB
{

class CudaColumnString;
using CudaColumnStringPtr = std::shared_ptr<CudaColumnString>;


class ICudaAggregateFunction
{
public:
    using DataPtr = char *;
    using CudaSizeType = UInt32;
    using ResultType = UInt64;

    virtual size_t      cudaSizeOfData() const = 0;
    virtual bool        isDataNeeded() const = 0;
    virtual void        cudaInitAggregateData(CudaSizeType places_num, DataPtr places, cudaStream_t stream = 0) const = 0;
    virtual size_t      cudaSizeOfAddBulkInternalBuf(CudaSizeType max_elements_num) = 0;
    virtual void        cudaAddBulk(DataPtr places, CudaColumnStringPtr str_column, CudaSizeType elements_num, CudaSizeType * res_buckets,
                                    char * tmp_buf, cudaStream_t stream = 0) const = 0;
    virtual void        cudaMergeBulk(DataPtr places, CudaSizeType elements_num, DataPtr places_from, CudaSizeType * res_buckets,
                                      cudaStream_t stream = 0) const = 0;
    virtual ResultType  getResult(DataPtr place) const = 0;

    virtual ~ICudaAggregateFunction() {}
};

using CudaAggregateDataPtr = ICudaAggregateFunction::DataPtr;
using CudaAggregateFunctionPtr = std::shared_ptr<ICudaAggregateFunction>;


}
