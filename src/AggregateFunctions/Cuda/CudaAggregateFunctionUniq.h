#pragma once

#include <Core/Cuda/Types.h>
#include <Common/Cuda/CudaArray.h>
#include <Common/Cuda/CudaHyperLogLogWithSmallSetOptimization.h>

#include <Columns/Cuda/CudaColumnString.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>


namespace DB
{

/// the only supported 'type'(T) is String

struct CudaAggregateFunctionUniqHLL12Data
{
    /// temporaly without SmallSet correction
    //using Set = CudaHyperLogLogCounter<12>;
    using Set = CudaHyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    __device__ __host__ void initNonzeroData()
    {
        set.initNonzeroData();
    }

    __device__ __host__ CudaAggregateFunctionUniqHLL12Data()
    {
    }
};


template <typename T>
class CudaAggregateFunctionUniq final : public ICudaAggregateFunction
{
    using CudaSizeType = ICudaAggregateFunction::CudaSizeType;
    using Data = CudaAggregateFunctionUniqHLL12Data;

public:
    size_t cudaSizeOfData() const override;
    void cudaAddBulk(CudaAggregateDataPtr places, const CudaColumnString *str_column,
                     CudaSizeType elements_num, CudaSizeType *res_buckets, cudaStream_t stream = 0) const override;

    virtual ~CudaAggregateFunctionUniq() override {}
};


template <>
class CudaAggregateFunctionUniq<String> final : public ICudaAggregateFunction
{
    using ResultType = ICudaAggregateFunction::ResultType;
    using Data = CudaAggregateFunctionUniqHLL12Data;

public:
    size_t cudaSizeOfData() const override
    {
        return sizeof(Data);
    }

    bool isDataNeeded() const override
    {
        return true;
    }

    void cudaInitAggregateData(CudaSizeType places_num, CudaAggregateDataPtr places, cudaStream_t stream = 0) const override;

    size_t cudaSizeOfAddBulkInternalBuf(CudaSizeType max_elements_num) override
    {
        return sizeof(UInt64)*max_elements_num;
        //hashes = CudaArrayPtr<UInt64>(new CudaArray<UInt64>(max_elements_num));
    }

    void cudaAddBulk(CudaAggregateDataPtr places, CudaColumnStringPtr str_column,
                     CudaSizeType elements_num, CudaSizeType * res_buckets, char * tmp_buf, cudaStream_t stream = 0) const override;
    void cudaMergeBulk(CudaAggregateDataPtr places, CudaSizeType elements_num,
                       CudaAggregateDataPtr places_from, CudaSizeType * res_buckets, cudaStream_t stream = 0) const override;

    ResultType getResult(CudaAggregateDataPtr place) const override
    {
        return ((Data *)place)->set.size();
    }

    virtual ~CudaAggregateFunctionUniq() override {}
};


}
