#include <algorithm>
#include <type_traits>
#include <utility>
#include <iostream>

#include "pcg_random.hpp"

#include <Columns/ColumnVector.h>
#include <Common/ArenaAllocator.h>
#include <Common/RadixSort.h>
#include <Columns/ColumnArray.h>


using namespace DB;

template <typename T>
struct GroupArraySortedDataHeap
{
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array values;

    static bool compare(const T & lhs, const T & rhs)
    {
        return lhs < rhs;
    }

    struct Comparator
    {
        bool operator()(const T & lhs, const T & rhs)
        {
            return compare(lhs, rhs);
        }
    };

    ALWAYS_INLINE void replaceTop()
    {
        size_t size = values.size();
        if (size < 2)
            return;

        size_t child_index = 1;

        if (values.size() > 2 && compare(values[1], values[2]))
            ++child_index;

        /// Check if we are in order
        if (compare(values[child_index], values[0]))
            return;

        size_t current_index = 0;
        auto current = values[current_index];

        do
        {
            /// We are not in heap-order, swap the parent with it's largest child.
            values[current_index] = values[child_index];
            current_index = child_index;

            // Recompute the child based off of the updated parent
            child_index = 2 * child_index + 1;

            if (child_index >= size)
                break;

            if ((child_index + 1) < size && compare(values[child_index], values[child_index + 1]))
            {
                /// Right child exists and is greater than left child.
                ++child_index;
            }

            /// Check if we are in order.
        } while (!compare(values[child_index], current));

        values[current_index] = current;
    }

    ALWAYS_INLINE void addElement(const T & element, size_t max_elements, Arena * arena)
    {
        if (values.size() >= max_elements)
        {
            /// Element is greater or equal than current max element, it cannot be in k min elements
            if (!compare(element, values[0]))
                return;

            values[0] = element;
            replaceTop();
            return;
        }

        values.push_back(element, arena);
        std::push_heap(values.begin(), values.end(), Comparator());
    }

    ALWAYS_INLINE void dump()
    {
        while (!values.empty())
        {
            std::pop_heap(values.begin(), values.end(), Comparator());
            std::cerr << values.back() << ' ';
            values.pop_back();
        }

        std::cerr << '\n';
    }
};

template <typename T>
struct GroupArraySortedDataSort
{
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array values;

    static bool compare(const T & lhs, const T & rhs)
    {
        return lhs < rhs;
    }

    struct Comparator
    {
        bool operator()(const T & lhs, const T & rhs)
        {
            return compare(lhs, rhs);
        }
    };

    ALWAYS_INLINE void sortAndLimit(size_t max_elements, Arena * arena)
    {
        RadixSort<RadixSortNumTraits<T>>::executeLSD(values.data(), values.size());
        values.resize(max_elements, arena);
    }

    ALWAYS_INLINE void partialSortAndLimitIfNeeded(size_t max_elements, Arena * arena)
    {
        if (values.size() < max_elements * 4)
            return;

        std::nth_element(values.begin(), values.begin() + max_elements, values.end(), Comparator());
        values.resize(max_elements, arena);
    }

    ALWAYS_INLINE void addElement(const T & element, size_t max_elements, Arena * arena)
    {
        values.push_back(element, arena);
        partialSortAndLimitIfNeeded(max_elements, arena);
    }
};

template <typename SortedData>
NO_INLINE void benchmark(size_t elements, size_t max_elements)
{
    Stopwatch watch;
    watch.start();

    SortedData data;
    pcg64_fast rng;

    Arena arena;

    for (size_t i = 0; i < elements; ++i)
    {
        uint64_t value = rng();
        data.addElement(value, max_elements, &arena);
    }

    watch.stop();
    std::cerr << "Elapsed " << watch.elapsedMilliseconds() << " milliseconds" << '\n';
}

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);

    if (argc != 4)
    {
        std::cerr << "./group_array_sorted method elements max_elements" << '\n';
        return 1;
    }

    std::string method = std::string(argv[1]);
    uint64_t elements = std::atol(argv[2]); /// NOLINT
    uint64_t max_elements = std::atol(argv[3]); /// NOLINT

    std::cerr << "Method " << method << " elements " << elements << " max elements " << max_elements << '\n';

    if (method == "heap")
    {
        benchmark<GroupArraySortedDataHeap<UInt64>>(elements, max_elements);
    }
    else if (method == "sort")
    {
        benchmark<GroupArraySortedDataSort<UInt64>>(elements, max_elements);
    }
    else
    {
        std::cerr << "Invalid method " << method << '\n';
        return 1;
    }

    return 0;
}
