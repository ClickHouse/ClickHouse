#pragma once

#include <Core/Cuda/Types.h>
#include <Common/Cuda/CudaIntHash32.h>
#include <Common/Cuda/CudaSmallSet.h>
#include <Common/Cuda/CudaHyperLogLogCounter.h>


namespace DB
{

/// Analog of HyperLogLogWithSmallSetOptimization for CUDA
template
<
    typename Key,
    UInt8 small_set_size,
    UInt8 K,
    typename Hash = CudaIntHash32<Key>,
    typename DenominatorType = double>
class CudaHyperLogLogWithSmallSetOptimization
{
    using Small = CudaSmallSet<Key, small_set_size>;
    using Large = CudaHyperLogLogCounter<K, Hash, UInt32, DenominatorType>;

    bool    is_large;
    Small   small;
    Large   large;

public:
    __device__ __host__ CudaHyperLogLogWithSmallSetOptimization() : is_large(false)
    {   
    }
    __device__ __host__ void initNonzeroData()
    {
        is_large = false;
        small.initNonzeroData();
        large.initNonzeroData();
    }

    __device__ void insert(const Key &value)
    {
        large.insert(value);
        if (!is_large)
        {
            if (!small.tryInsert(value))
                is_large = true;
        }
    }

    UInt32 size() const
    {
        return !is_large ? small.size() : large.size();
    }

    __device__ void merge(const CudaHyperLogLogWithSmallSetOptimization & rhs)
    {
        if (rhs.is_large)
        {
            is_large = true;
            large.merge(rhs.large);
        }
        else
        {
            for (UInt8 i = 0;i < rhs.small.sizeWithoutZeroElem();++i)
                insert( rhs.small.getWithoutZeroElem(i) );
            if (rhs.small.hasZeroElem())
                insert( CudaZeroTraits::zero<Key>() );
        }
    }

    bool isLarge() const
    {
        return is_large;
    }
};


}
