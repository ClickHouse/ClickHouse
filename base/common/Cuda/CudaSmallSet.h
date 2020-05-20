#pragma once

#include <Core/Cuda/Types.h>
#include <Common/Cuda/CudaZeroTraits.h>
#include <Common/Cuda/CudaAtomics.cuh>

/// Not an actual analog of SmallSet from CH. Only for use in HLL Uniq algorithm
/// Also it will only work with simple types (UIntXX) - i.e. such a types 'Key' that
/// cuda_details::atomicCAS<Key> is defined and for which there is no non-trivial
/// destructor

template
<
    typename Key,
    DB::UInt8 capacity
>
class CudaSmallSet
{
protected:
    using Self = CudaSmallSet<Key, capacity>;

    bool    has_zero_elem;
    Key     buf[capacity];       /// A piece of memory for all elements.

public:
    using           key_type = Key;
    //constexpr DB::UInt8 capacity_value = capacity;

public:
    __device__ __host__ CudaSmallSet() : has_zero_elem(false)
    {
        for (DB::UInt8 i = 0;i < capacity;++i)
            CudaZeroTraits::set<Key>(buf[i]);
    }
    /// TODO there is no garantee that memset(0) will create correct 'zero' keys (only true for simple types)
    __device__ __host__ void initNonzeroData()
    {
        has_zero_elem = false;
    }

    /// Insert the value. In the case of any more complex values, it is better to use the `emplace` function.
    __device__ bool tryInsert(const Key & key)
    {
        if (CudaZeroTraits::check(key)) {
            has_zero_elem = true;
            return true;
        }
        for (DB::UInt8 i = 0;i < capacity;++i) {
            Key x = buf[i];
            if (x == key) return true;
            if (CudaZeroTraits::check(x)) {
                Key    old = cuda_details::atomicCAS<Key>(&buf[i], x, key);
                if ((CudaZeroTraits::check(old))||(old == key)) return true;
            }
        }
        return false;
    }

    __device__ bool tryMerge(const CudaSmallSet &rhs)
    {
        for (DB::UInt8 i = 0;i < capacity;++i) {
            if (CudaZeroTraits::check(rhs.buf[i])) break;
            if (!tryInsert(rhs.buf[i])) return false;
        }
        if (rhs.has_zero_elem) has_zero_elem = true;
        return true;
    }

    DB::UInt8 size() const
    {
        DB::UInt8 res = 0;
        while (res < capacity) {
            if (CudaZeroTraits::check(buf[res])) break;
            ++res;
        }
        if (has_zero_elem) ++res;
        return res;
    }

    __device__ __host__ bool hasZeroElem() const
    {
        return has_zero_elem;
    }
    __device__ __host__ bool sizeWithoutZeroElem() const
    {
        DB::UInt8 res = 0;
        while (res < capacity) {
            if (CudaZeroTraits::check(buf[res])) break;
            ++res;
        }
        return res;   
    }
    __device__ __host__ const Key  &getWithoutZeroElem(DB::UInt8 i) const
    {
        return buf[i];
    }
};
