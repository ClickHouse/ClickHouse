#pragma once

#include <Common/PODArray.h>

namespace DB
{

/**
 * This class is intended to push sortable data into.
 * When looking up values the container ensures that it is sorted for log(N) lookup
 *
 * Note, this is only efficient when the insertions happen in one stage, followed by all retrievals
 * This way the data only gets sorted once.
 */

template <typename T, size_t INITIAL_SIZE = 4096, typename TAllocator = Allocator<false>>
class SortedLookupPODArray : private PaddedPODArray<T, INITIAL_SIZE, TAllocator>
{
public:
    using Base = PaddedPODArray<T, INITIAL_SIZE, TAllocator>;
    using typename Base::PODArray;
    using Base::cbegin;
    using Base::cend;

    template <typename U, typename ... TAllocatorParams>
    void insert(U && x, TAllocatorParams &&... allocator_params)
    {
        Base::push_back(std::forward<U>(x), std::forward<TAllocatorParams>(allocator_params)...);
        sorted = false;
    }

    typename Base::const_iterator upper_bound (const T& k)
    {
        if (!sorted)
            this->sort();
        return std::upper_bound(this->cbegin(), this->cend(), k);
    }
private:
    void sort()
    {
        std::sort(this->begin(), this->end());
        sorted = true;
    }

    bool sorted = false;
};

}
