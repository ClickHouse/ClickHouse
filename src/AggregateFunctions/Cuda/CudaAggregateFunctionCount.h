#pragma once

#include <Core/Cuda/Types.h>
#include <Common/Cuda/CudaArray.h>

#include <Columns/Cuda/CudaColumnString.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>

namespace DB
{

class CudaAggregateFunctionCount final : public ICudaAggregateFunction
{
    using CudaSizeType = ICudaAggregateFunction::CudaSizeType;
    using ResultType = ICudaAggregateFunction::ResultType;

public:
    struct Data
    {
        UInt64 count = 0;
    };

    size_t cudaSizeOfData() const override
    {
        return sizeof(Data);
    }

    bool isDataNeeded() const override
    {
        return false;
    }

    /// TODO move to cu file
    void cudaInitAggregateData(CudaSizeType places_num, CudaAggregateDataPtr places, cudaStream_t /*stream*/ = 0) const override
    {
        CUDA_SAFE_CALL( cudaMemset(places, 0, sizeof(Data) * places_num) );
    }

    size_t cudaSizeOfAddBulkInternalBuf(CudaSizeType /*max_elements_num*/) override
    {
        return 0;
    }

    void cudaAddBulk(CudaAggregateDataPtr places, CudaColumnStringPtr str_column,
                     CudaSizeType elements_num, CudaSizeType * res_buckets, char * tmp_buf, cudaStream_t stream = 0) const override;
    void cudaMergeBulk(CudaAggregateDataPtr places, CudaSizeType elements_num,
                       CudaAggregateDataPtr places_from, CudaSizeType * res_buckets, cudaStream_t stream = 0) const override;

    ResultType getResult(CudaAggregateDataPtr place) const override
    {
        return ((Data *)place)->count;
    }

    virtual ~CudaAggregateFunctionCount() override {}
};


}
