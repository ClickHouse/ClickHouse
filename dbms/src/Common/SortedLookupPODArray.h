#pragma once

#include <vector>
//#include <Common/PODArray.h>

namespace DB
{

/**
 * This class is intended to push sortable data into.
 * When looking up values the container ensures that it is sorted for log(N) lookup
 *
 * Note, this is only efficient when the insertions happen in one stage, followed by all retrievals
 * This way the data only gets sorted once.
 */

template <typename T>
class SortedLookupPODArray
{
public:
    using Base = std::vector<T>;
    //using Base = PaddedPODArray<T>;

    template <typename U, typename ... TAllocatorParams>
    void insert(U && x, TAllocatorParams &&... allocator_params)
    {
        array.push_back(std::forward<U>(x), std::forward<TAllocatorParams>(allocator_params)...);
        sorted = false;
    }

    typename Base::const_iterator upper_bound(const T & k)
    {
        if (!sorted)
            sort();
        return std::upper_bound(array.cbegin(), array.cend(), k);
    }

    typename Base::const_iterator cbegin() const { return array.cbegin(); }
    typename Base::const_iterator cend() const { return array.cend(); }

private:
    Base array;
    bool sorted = false;

    void sort()
    {
        std::sort(array.begin(), array.end());
        sorted = true;
    }
};

}
