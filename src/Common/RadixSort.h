#pragma once

#include <string.h>
#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
#include <malloc.h>
#endif
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cassert>
#include <type_traits>
#include <memory>

#include <Core/Defines.h>
#include <base/bit_cast.h>
#include <base/extended_types.h>
#include <base/sort.h>
#include <Common/TargetSpecific.h>
#include <Common/PODArray.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif


/** Radix sort, has the following functionality:
  *
  * Can sort unsigned, signed numbers, and floats.
  * Can sort an array of fixed length elements that contain something else besides the key.
  * Can sort an array and form sorted result containing some transformation of elements.
  * Can do partial sort.
  * Customizable radix size.
  *
  * Two flavours of radix sort are implemented:
  *
  * 1. LSB, stable.
  * 2. MSB, unstable, with support for partial sort.
  */


/** Used as a template parameter. See below.
  */
struct RadixSortAllocator
{
    static void * allocate(size_t size)
    {
        return ::operator new(size);
    }

    static void deallocate(void * ptr, size_t size)
    {
        ::operator delete(ptr, size);
    }
};


/** A transformation that transforms the bit representation of a key into an unsigned integer number,
  *  that the order relation over the keys will match the order relation over the obtained unsigned numbers.
  * For floats this conversion does the following:
  *  if the signed bit is set, it flips all other bits.
  * In this case, NaN-s are bigger than all normal numbers.
  */
template <typename KeyBits>
struct RadixSortFloatTransform
{
    /// Is it worth writing the result in memory, or is it better to do calculation every time again?
    static constexpr bool transform_is_simple = false;

    static KeyBits forward(KeyBits x)
    {
        return x ^ ((-(x >> (sizeof(KeyBits) * 8 - 1))) | (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)));
    }

    static KeyBits backward(KeyBits x)
    {
        return x ^ (((x >> (sizeof(KeyBits) * 8 - 1)) - 1) | (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)));
    }
};


template <typename TElement>
struct RadixSortFloatTraits
{
    /// The type of the element. It can be a structure with a key and some other payload. Or just a key.
    using Element = TElement;

    /// The key to sort by.
    using Key = Element;

    /// Part of the element that you need in the result array.
    /// There are cases when elements are sorted by one part but you need other parts in array of results.
    using Result = Element;

    /// Type for calculating histograms. In the case of a known small number of elements, it can be less than size_t.
    using CountType = uint32_t;

    /// The type to which the key is transformed to do bit operations. This UInt is the same size as the key.
    using KeyBits = std::conditional_t<sizeof(Key) == 8, uint64_t, uint32_t>;

    static constexpr size_t PART_SIZE_BITS = 8;    /// With what pieces of the key, in bits, to do one pass - reshuffle of the array.

    /// Converting a key into KeyBits is such that the order relation over the key corresponds to the order relation over KeyBits.
    using Transform = RadixSortFloatTransform<KeyBits>;

    /// An object with the functions allocate and deallocate.
    /// Can be used, for example, to allocate memory for a temporary array on the stack.
    /// To do this, the allocator itself is created on the stack.
    using Allocator = RadixSortAllocator;

    /// The function to get the key from an array element.
    static Key & extractKey(Element & elem) { return elem; }

    /// The function to get the result part from an array element.
    static Result & extractResult(Element & elem) { return elem; }

    /// Used when fallback to comparison based sorting is needed.
    /// TODO: Correct handling of NaNs, NULLs, etc
    static bool less(Key x, Key y)
    {
        return x < y;
    }

    static bool greater(Key x, Key y)
    {
        return x > y;
    }
};


template <typename KeyBits>
struct RadixSortIdentityTransform
{
    static constexpr bool transform_is_simple = true;

    static KeyBits forward(KeyBits x)     { return x; }
    static KeyBits backward(KeyBits x)    { return x; }
};


template <typename TElement>
struct RadixSortUIntTraits
{
    using Element = TElement;
    using Result = Element;
    using Key = Element;
    using CountType = uint32_t;
    using KeyBits = Key;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortIdentityTransform<KeyBits>;
    using Allocator = RadixSortAllocator;

    static Key & extractKey(Element & elem) { return elem; }
    static Result & extractResult(Element & elem) { return elem; }

    static bool less(Key x, Key y)
    {
        return x < y;
    }

    static bool greater(Key x, Key y)
    {
        return x > y;
    }
};


template <typename KeyBits>
struct RadixSortSignedTransform
{
    static constexpr bool transform_is_simple = true;

    static KeyBits forward(KeyBits x)     { return x ^ (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)); }
    static KeyBits backward(KeyBits x)    { return x ^ (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)); }
};


template <typename TElement>
struct RadixSortIntTraits
{
    using Element = TElement;
    using Result = Element;
    using Key = Element;
    using CountType = uint32_t;
    using KeyBits = make_unsigned_t<Key>;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortSignedTransform<KeyBits>;
    using Allocator = RadixSortAllocator;

    static Key & extractKey(Element & elem) { return elem; }
    static Result & extractResult(Element & elem) { return elem; }

    static bool less(Key x, Key y)
    {
        return x < y;
    }

    static bool greater(Key x, Key y)
    {
        return x > y;
    }
};


template <typename T>
using RadixSortNumTraits = std::conditional_t<
    is_integer<T>,
    std::conditional_t<is_unsigned_v<T>, RadixSortUIntTraits<T>, RadixSortIntTraits<T>>,
    RadixSortFloatTraits<T>>;


template <typename Traits>
struct RadixSort
{
private:
    using Element     = typename Traits::Element;
    using Result      = typename Traits::Result;
    using Key         = typename Traits::Key;
    using CountType   = typename Traits::CountType;
    using KeyBits     = typename Traits::KeyBits;

    // Use insertion sort if the size of the array is less than equal to this threshold
    static constexpr size_t INSERTION_SORT_THRESHOLD = 64;

    static constexpr size_t HISTOGRAM_SIZE = 1 << Traits::PART_SIZE_BITS;
    static constexpr size_t PART_BITMASK = HISTOGRAM_SIZE - 1;
    static constexpr size_t KEY_BITS = sizeof(Key) * 8;
    static constexpr size_t NUM_PASSES = (KEY_BITS + (Traits::PART_SIZE_BITS - 1)) / Traits::PART_SIZE_BITS;


    static KeyBits keyToBits(Key x) { return bit_cast<KeyBits>(x); }
    static Key bitsToKey(KeyBits x) { return bit_cast<Key>(x); }

    struct LessComparator
    {
        ALWAYS_INLINE bool operator()(Element & lhs, Element & rhs)
        {
            return Traits::less(Traits::extractKey(lhs), Traits::extractKey(rhs));
        }
    };

    struct GreaterComparator
    {
        ALWAYS_INLINE bool operator()(Element & lhs, Element & rhs)
        {
            return Traits::greater(Traits::extractKey(lhs), Traits::extractKey(rhs));
        }
    };

    static ALWAYS_INLINE KeyBits getPart(size_t N, KeyBits x)
    {
        if (Traits::Transform::transform_is_simple)
            x = Traits::Transform::forward(x);

        return (x >> (N * Traits::PART_SIZE_BITS)) & PART_BITMASK;
    }

    static ALWAYS_INLINE KeyBits extractPart(size_t N, Element & elem)
    {
        return getPart(N, keyToBits(Traits::extractKey(elem)));
    }

    static ALWAYS_INLINE KeyBits extractPartFromKey(size_t N, Key & key)
    {
        return getPart(N, keyToBits(key));
    }

    static void insertionSortInternal(Element * arr, size_t size)
    {
        Element * end = arr + size;
        for (Element * i = arr + 1; i < end; ++i)
        {
            if (Traits::less(Traits::extractKey(*i), Traits::extractKey(*(i - 1))))
            {
                Element * j;
                Element tmp = *i;
                *i = *(i - 1);
                for (j = i - 1; j > arr && Traits::less(Traits::extractKey(tmp), Traits::extractKey(*(j - 1))); --j)
                    *j = *(j - 1);
                *j = tmp;
            }
        }
    }

    /*
    template <bool DIRECT_WRITE_TO_DESTINATION>
    static NO_INLINE void radixSortLSDInternalWithSOA(Key * arr, UInt32 * indices, size_t size, bool reverse, Result * destination)
    {
        /// If the array is smaller than 256, then it is better to use another algorithm.

        /// There are loops of NUM_PASSES. It is very important that they are unfolded at compile-time.

        /// For each of the NUM_PASSES bit ranges of the key, consider how many times each value of this bit range met.
        std::unique_ptr<CountType[]> histograms{new CountType[HISTOGRAM_SIZE * NUM_PASSES]{}};

        typename Traits::Allocator allocator;

        /// We will do several passes through the array. On each pass, the data is transferred to another array. Let's allocate this temporary array.
        Key * swap_arr_buffer = reinterpret_cast<Key *>(allocator.allocate(size * sizeof(Key)));
        UInt32 * swap_indices_buffer = reinterpret_cast<UInt32 *>(allocator.allocate(size * sizeof(UInt32)));

        /// Transform the array and calculate the histogram.
        /// NOTE This is slightly suboptimal. Look at https://github.com/powturbo/TurboHist
        if constexpr (!Traits::Transform::transform_is_simple)
        {
            for (size_t i = 0; i < size; ++i)
                arr[i] = bitsToKey(Traits::Transform::forward(keyToBits(arr[i])));
        }

        for (size_t pass = 0; pass < NUM_PASSES; ++pass)
        {
            size_t base = pass * HISTOGRAM_SIZE;
            for (size_t i = 0; i < size; ++i)
            {
                histograms[base + extractPartFromKey(pass, arr[i])]++;
            }
        }

        {
            /// Replace the histograms with the accumulated sums: the value in position i is the sum of the previous positions minus one.
            for (size_t pass = 0; pass < NUM_PASSES; ++pass)
            {
                CountType sum = 0;
                size_t base = pass * HISTOGRAM_SIZE;
                for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
                {
                    CountType new_sum = histograms[base + i] + sum;
                    histograms[pass * HISTOGRAM_SIZE + i] = sum;
                    sum = new_sum;
                }
            }
        }

        /// Move the elements in the order starting from the least bit piece, and then do a few passes on the number of pieces.
        for (size_t pass = 0; pass < NUM_PASSES - DIRECT_WRITE_TO_DESTINATION; ++pass)
        {
            Key * arr_writer = pass % 2 ? arr : swap_arr_buffer;
            UInt32 * indices_writer = pass % 2 ? indices : swap_indices_buffer;

            Key * arr_reader = pass % 2 ? swap_arr_buffer : arr;
            UInt32 * indices_reader = pass % 2 ? swap_indices_buffer : indices;

            for (size_t i = 0; i < size; ++i)
            {
                size_t pos = extractPartFromKey(pass, arr_reader[i]);

                /// Place the element on the next free position.
                size_t hist_idx = pass * HISTOGRAM_SIZE + pos;
                size_t sorted_idx = histograms[hist_idx]++;
                indices_writer[sorted_idx] = indices_reader[i];

                /// On the last pass, we do the reverse transformation.
                if constexpr (!Traits::Transform::transform_is_simple)
                {
                    if (pass == NUM_PASSES - 1)
                        arr_writer[sorted_idx] = bitsToKey(Traits::Transform::backward(keyToBits(arr_reader[i])));
                    else
                        arr_writer[sorted_idx] = arr_reader[i];
                }
                else
                    arr_writer[sorted_idx] = arr_reader[i];
            }
        }

        if (DIRECT_WRITE_TO_DESTINATION)
        {
            constexpr size_t pass = NUM_PASSES - 1;
            Result * result_writer = destination;
            Key * arr_reader = pass % 2 ? swap_arr_buffer : arr;
            UInt32 * indices_reader = pass % 2 ? swap_indices_buffer : indices;

            if (reverse)
            {
                for (size_t i = 0; i < size; ++i)
                {
                    size_t pos = extractPartFromKey(pass, arr_reader[i]);
                    result_writer[size - 1 - histograms[pass * HISTOGRAM_SIZE + pos]++] = indices_reader[i];
                }
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    size_t pos = extractPartFromKey(pass, arr_reader[i]);
                    result_writer[histograms[pass * HISTOGRAM_SIZE + pos]++] = indices_reader[i];
                }
            }
        }
        else
        {
            /// If the number of passes is odd, the result array is in a temporary buffer. Copy it to the place of the original array.
            if (NUM_PASSES % 2)
            {
                memcpy(arr, swap_arr_buffer, size * sizeof(Key));
                memcpy(indices, swap_indices_buffer, size * sizeof(UInt32));
            }

            /// TODO This is suboptimal, we can embed it to the last pass.
            if (reverse)
            {
                std::reverse(arr, arr + size);
                std::reverse(indices, indices + size);
            }
        }

        allocator.deallocate(swap_arr_buffer, size * sizeof(Key));
        allocator.deallocate(swap_indices_buffer, size * sizeof(UInt32));
    }
    */


    template <bool DIRECT_WRITE_TO_DESTINATION>
    static NO_INLINE void radixSortLSDInternal(Element * arr, size_t size, bool reverse, Result * destination)
    {
        /// If the array is smaller than 256, then it is better to use another algorithm.

        /// There are loops of NUM_PASSES. It is very important that they are unfolded at compile-time.

        /// For each of the NUM_PASSES bit ranges of the key, consider how many times each value of this bit range met.
        std::unique_ptr<CountType[]> histograms{new CountType[HISTOGRAM_SIZE * NUM_PASSES]{}};

        typename Traits::Allocator allocator;

        /// We will do several passes through the array. On each pass, the data is transferred to another array. Let's allocate this temporary array.
        Element * swap_buffer = reinterpret_cast<Element *>(allocator.allocate(size * sizeof(Element)));

        // if constexpr (!Traits::Transform::transform_is_simple)
        // {
        //     for (size_t i = 0; i < size; ++i)
        //         Traits::extractKey(arr[i]) = bitsToKey(Traits::Transform::forward(keyToBits(Traits::extractKey(arr[i]))));
        // }

        // CountType sums[NUM_PASSES] = {0};
        // for (size_t pass = 0; pass < NUM_PASSES; ++pass)
        // {
        //     /// Transform the array and calculate the histogram.
        //     /// NOTE This is slightly suboptimal. Look at https://github.com/powturbo/TurboHist
        //     for (size_t i = 0; i < size; ++i)
        //         ++histograms[pass * HISTOGRAM_SIZE + extractPart(pass, arr[i])];

        //     /// Replace the histograms with the accumulated sums: the value in position i is the sum of the previous positions minus one.
        //     for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
        //     {
        //         CountType new_sum = histograms[pass * HISTOGRAM_SIZE + i] + sums[pass];
        //         histograms[pass * HISTOGRAM_SIZE + i] = sums[pass];
        //         sums[pass] = new_sum;
        //     }
        // }

        /// Transform the array and calculate the histogram.
        /// NOTE This is slightly suboptimal. Look at https://github.com/powturbo/TurboHist
        for (size_t i = 0; i < size; ++i)
        {
            if (!Traits::Transform::transform_is_simple)
                Traits::extractKey(arr[i]) = bitsToKey(Traits::Transform::forward(keyToBits(Traits::extractKey(arr[i]))));

            for (size_t pass = 0; pass < NUM_PASSES; ++pass)
                ++histograms[pass * HISTOGRAM_SIZE + extractPart(pass, arr[i])];
        }

        {
            /// Replace the histograms with the accumulated sums: the value in position i is the sum of the previous positions minus one.
            CountType sums[NUM_PASSES] = {0};

            for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
            {
                for (size_t pass = 0; pass < NUM_PASSES; ++pass)
                {
                    CountType tmp = histograms[pass * HISTOGRAM_SIZE + i] + sums[pass];
                    histograms[pass * HISTOGRAM_SIZE + i] = sums[pass];
                    sums[pass] = tmp;
                }
            }
        }

        /// Move the elements in the order starting from the least bit piece, and then do a few passes on the number of pieces.
        for (size_t pass = 0; pass < NUM_PASSES - DIRECT_WRITE_TO_DESTINATION; ++pass)
        {
            Element * writer = pass % 2 ? arr : swap_buffer;
            Element * reader = pass % 2 ? swap_buffer : arr;

            for (size_t i = 0; i < size; ++i)
            {
                size_t pos = extractPart(pass, reader[i]);

                if (i + 1 < size)
                {
                    size_t next_pos = extractPart(pass, reader[i + 1]);
                    __builtin_prefetch(&writer[histograms[pass * HISTOGRAM_SIZE + next_pos]], 1, 0);
                }

                /// Place the element on the next free position.
                auto & dest = writer[histograms[pass * HISTOGRAM_SIZE + pos]++];
                dest = reader[i];

                /// On the last pass, we do the reverse transformation.
                if (!Traits::Transform::transform_is_simple && pass == NUM_PASSES - 1)
                    Traits::extractKey(dest) = bitsToKey(Traits::Transform::backward(keyToBits(Traits::extractKey(reader[i]))));
            }
        }

        if (DIRECT_WRITE_TO_DESTINATION)
        {
            constexpr size_t pass = NUM_PASSES - 1;
            Result * writer = destination;
            Element * reader = pass % 2 ? swap_buffer : arr;

            if (reverse)
            {
                for (size_t i = 0; i < size; ++i)
                {
                    size_t pos = extractPart(pass, reader[i]);
                    if (i + 1 < size)
                    {
                        size_t next_pos = extractPart(pass, reader[i + 1]);
                        __builtin_prefetch(&writer[size - 1 - histograms[pass * HISTOGRAM_SIZE + next_pos]], 1, 0);
                    }

                    writer[size - 1 - (histograms[pass * HISTOGRAM_SIZE + pos]++)] = Traits::extractResult(reader[i]);
                }
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    size_t pos = extractPart(pass, reader[i]);
                    if (i + 1 < size)
                    {
                        size_t next_pos = extractPart(pass, reader[i + 1]);
                        __builtin_prefetch(&writer[histograms[pass * HISTOGRAM_SIZE + next_pos]], 1, 0);
                    }

                    writer[histograms[pass * HISTOGRAM_SIZE + pos]++] = Traits::extractResult(reader[i]);
                }
            }
        }
        else
        {
            /// If the number of passes is odd, the result array is in a temporary buffer. Copy it to the place of the original array.
            if (NUM_PASSES % 2)
                memcpy(arr, swap_buffer, size * sizeof(Element));

            /// TODO This is suboptimal, we can embed it to the last pass.
            if (reverse)
                std::reverse(arr, arr + size);
        }

        allocator.deallocate(swap_buffer, size * sizeof(Element));
    }


    /* Main MSD radix sort subroutine.
     * Puts elements to buckets based on PASS-th digit, then recursively calls insertion sort or itself on the buckets.
     *
     * TODO: Provide support for 'reverse' and 'DIRECT_WRITE_TO_DESTINATION'.
     *
     * Invariant: higher significant parts of the elements than PASS are constant within arr or is is the first PASS.
     * PASS is counted from least significant (0), so the first pass is NUM_PASSES - 1.
     */
    template <size_t PASS>
    static void radixSortMSDInternal(Element * arr, size_t size, size_t limit)
    {
        /// The beginning of every i-1-th bucket. 0th element will be equal to 1st.
        /// Last element will point to array end.
        std::unique_ptr<Element *[]> prev_buckets{new Element*[HISTOGRAM_SIZE + 1]};
        /// The beginning of every i-th bucket (the same array shifted by one).
        Element ** buckets = &prev_buckets[1];

        prev_buckets[0] = arr;
        prev_buckets[1] = arr;

        /// The end of the range of buckets that we need with limit.
        Element * finish = arr + size;

        /// Count histogram of current element parts.

        /// We use loop unrolling to minimize data dependencies and increase instruction level parallelism.
        /// Unroll 8 times looks better on experiments;
        ///  also it corresponds with the results from https://github.com/powturbo/TurboHist

        static constexpr size_t UNROLL_COUNT = 8;
        std::unique_ptr<CountType[]> count{new CountType[HISTOGRAM_SIZE * UNROLL_COUNT]{}};
        size_t unrolled_size = size / UNROLL_COUNT * UNROLL_COUNT;

        for (Element * elem = arr; elem < arr + unrolled_size; elem += UNROLL_COUNT)
            for (size_t i = 0; i < UNROLL_COUNT; ++i)
                ++count[i * HISTOGRAM_SIZE + extractPart(PASS, elem[i])];

        for (Element * elem = arr + unrolled_size; elem < arr + size; ++elem)
            ++count[extractPart(PASS, *elem)];

        for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
            for (size_t j = 1; j < UNROLL_COUNT; ++j)
                count[i] += count[j * HISTOGRAM_SIZE + i];

        /// Fill pointers to buckets according to the histogram.

        /// How many buckets we will recurse into.
        ssize_t buckets_for_recursion = HISTOGRAM_SIZE;
        bool finish_early = false;

        for (size_t i = 1; i < HISTOGRAM_SIZE; ++i)
        {
            /// Positions are just a cumulative sum of counts.
            buckets[i] = buckets[i - 1] + count[i - 1];

            /// If this bucket starts after limit, we don't need it.
            if (!finish_early && buckets[i] >= arr + limit)
            {
                buckets_for_recursion = i;
                finish = buckets[i];
                finish_early = true;
                /// We cannot break here, because we need correct pointers to all buckets, see the next loop.
            }
        }

        /* At this point, we have the following variables:
         * count[i] is the size of i-th bucket
         * buckets[i] is a pointer to the beginning of i-th bucket, buckets[-1] == buckets[0]
         * buckets_for_recursion is the number of buckets that should be sorted, the last of them only partially
         * finish is a pointer to the end of the first buckets_for_recursion buckets
         */

        /// Scatter array elements to buckets until the first buckets_for_recursion buckets are full
        /// After the above loop, buckets are shifted towards the end and now pointing to the beginning of i+1th bucket.

        for (ssize_t i = 0; /* guarded by 'finish' */; ++i)
        {
            assert(i < buckets_for_recursion);

            /// We look at i-1th index, because bucket pointers are shifted right on every loop iteration,
            ///  and all buckets before i was completely shifted to the beginning of the next bucket.
            /// So, the beginning of i-th bucket is at buckets[i - 1].

            Element * bucket_end = buckets[i - 1] + count[i];

            /// Fill this bucket.
            while (buckets[i] != bucket_end)
            {
                Element swapper = *buckets[i];
                KeyBits tag = extractPart(PASS, swapper);

                if (tag != KeyBits(i))
                {
                    /// Invariant: tag > i, because the elements with less tags are already at the right places.
                    assert(tag > KeyBits(i));

                    /// While the tag (digit) of the element is not that we need,
                    /// swap the element with the next element in the bucket for that tag.

                    /// Interesting observation:
                    /// - we will definitely find the needed element,
                    ///   because the tag's bucket will contain at least one "wrong" element,
                    ///   because the "right" element is appeared in our bucket.

                    /// After this loop we shift buckets[i] and buckets[tag] pointers to the right for all found tags.
                    /// And all positions that were traversed are filled with the proper values.

                    do
                    {
                        std::swap(swapper, *buckets[tag]);
                        ++buckets[tag];
                        tag = extractPart(PASS, swapper);
                    } while (tag != KeyBits(i));
                    *buckets[i] = swapper;
                }

                /// Now we have the right element at this place.
                ++buckets[i];
            }

            if (bucket_end == finish)
                break;
        }

        /// Recursion for the relevant buckets.

        if constexpr (PASS > 0)
        {
            /// Recursively sort buckets, except the last one
            for (ssize_t i = 0; i < buckets_for_recursion - 1; ++i)
            {
                Element * start = buckets[i - 1];
                ssize_t subsize = count[i];

                radixSortMSDInternalHelper<PASS - 1>(start, subsize, subsize);
            }

            /// Sort the last necessary bucket with limit
            {
                ssize_t i = buckets_for_recursion - 1;

                Element * start = buckets[i - 1];
                ssize_t subsize = count[i];
                ssize_t sublimit = limit - (start - arr);

                radixSortMSDInternalHelper<PASS - 1>(start, subsize, sublimit);
            }
        }
    }

    // A helper to choose sorting algorithm based on array length
    template <size_t PASS>
    static void radixSortMSDInternalHelper(Element * arr, size_t size, size_t limit)
    {
        if (size <= INSERTION_SORT_THRESHOLD)
            insertionSortInternal(arr, size);
        else
            radixSortMSDInternal<PASS>(arr, size, limit);
    }

    template <bool DIRECT_WRITE_TO_DESTINATION, typename Comparator>
    static void executeLSDWithTrySortInternal(Element * arr, size_t size, bool reverse, Comparator comparator, Result * destination)
    {
        bool try_sort = ::trySort(arr, arr + size, comparator);
        if (try_sort)
        {
            if constexpr (DIRECT_WRITE_TO_DESTINATION)
            {
                for (size_t i = 0; i < size; ++i)
                    destination[i] = Traits::extractResult(arr[i]);
            }

            return;
        }

        radixSortLSDInternal<DIRECT_WRITE_TO_DESTINATION>(arr, size, reverse, destination);
    }

public:
    /** Least significant digit radix sort (stable).
      * This function will sort inplace (modify 'arr')
      */
    static void executeLSD(Element * arr, size_t size)
    {
        radixSortLSDInternal<false>(arr, size, false, nullptr);
    }

    static void executeLSD(Element * arr, size_t size, bool reverse)
    {
        radixSortLSDInternal<false>(arr, size, reverse, nullptr);
    }

    /** This function will start to sort inplace (modify 'arr')
      *  but on the last step it will write result directly to the destination
      *  instead of finishing sorting 'arr'.
      * In this case it will fill only Result parts of the Element into destination.
      * It is handy to avoid unnecessary data movements.
      */
    static void executeLSD(Element * arr, size_t size, bool reverse, Result * destination)
    {
        radixSortLSDInternal<true>(arr, size, reverse, destination);
    }

    /*
    static void executeLSDWithSOA(Key * arr, UInt32 * indices, size_t size, bool reverse, Result * destination)
    {
        radixSortLSDInternalWithSOA<true>(arr, indices, size, reverse, destination);
    }
    */

    /** Tries to fast sort elements for common sorting patterns (unstable).
      * If fast sort cannot be performed, execute least significant digit radix sort.
      */
    static void executeLSDWithTrySort(Element * arr, size_t size)
    {
        return executeLSDWithTrySort(arr, size, false);
    }

    static void executeLSDWithTrySort(Element * arr, size_t size, bool reverse)
    {
        return executeLSDWithTrySort(arr, size, reverse, nullptr);
    }

    static void executeLSDWithTrySort(Element * arr, size_t size, bool reverse, Result * destination)
    {
        if (reverse)
        {

            if (destination)
                return executeLSDWithTrySortInternal<true>(arr, size, reverse, GreaterComparator(), destination);
            return executeLSDWithTrySortInternal<false>(arr, size, reverse, GreaterComparator(), destination);
        }

        if (destination)
            return executeLSDWithTrySortInternal<true>(arr, size, reverse, LessComparator(), destination);
        return executeLSDWithTrySortInternal<false>(arr, size, reverse, LessComparator(), destination);
    }

    /* Most significant digit radix sort
     * Is not stable, but allows partial sorting.
     * And it's more cache-friendly and usually faster than LSD variant.
     *
     * NOTE: It's beneficial over std::partial_sort only if limit is above ~2% of size for 8 bit radix.
     * NOTE: When lowering down limit to 1%, the radix of 4..6 or 10..12 bit started to become beneficial.
     * For less than 1% limit, it's not recommended to use.
     * NOTE: For huge arrays without limit, the radix 11 suddenly becomes better... but not for smaller arrays.
     * Maybe it because histogram will fit in half of L1d cache (2048 * 4 = 16384).
     *
     * Based on https://github.com/voutcn/kxsort, license:
     * The MIT License
     * Copyright (c) 2016 Dinghua Li <voutcn@gmail.com>
     *
     * Permission is hereby granted, free of charge, to any person obtaining
     * a copy of this software and associated documentation files (the
     * "Software"), to deal in the Software without restriction, including
     * without limitation the rights to use, copy, modify, merge, publish,
     * distribute, sublicense, and/or sell copies of the Software, and to
     * permit persons to whom the Software is furnished to do so, subject to
     * the following conditions:
     *
     * The above copyright notice and this permission notice shall be
     * included in all copies or substantial portions of the Software.
     *
     * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
     * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
     * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
     * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
     * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
     * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
     * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
     * SOFTWARE.
     */
    static void executeMSD(Element * arr, size_t size, size_t limit)
    {
        limit = std::min(limit, size);
        radixSortMSDInternalHelper<NUM_PASSES - 1>(arr, size, limit);
    }
};


/// Helper functions for numeric types.
/// Use RadixSort with custom traits for complex types instead.

template <typename T>
void radixSortLSD(T * arr, size_t size)
{
    RadixSort<RadixSortNumTraits<T>>::executeLSD(arr, size);
}

template <typename T>
void radixSortMSD(T * arr, size_t size, size_t limit)
{
    RadixSort<RadixSortNumTraits<T>>::executeMSD(arr, size, limit);
}

/*
DECLARE_AVX512F_SPECIFIC_CODE(

using namespace DB;

template <typename T, bool DIRECT_WRITE_TO_DESTINATION>
requires(std::is_integral_v<T> || std::is_floating_point_v<T>)
void radixSortLSDInternalWithSOA(
    typename RadixSortNumTraits<T>::Key * arr,
    UInt32 * indices,
    size_t size,
    bool reverse,
    size_t * destination)
{
    // RadixSort<RadixSortNumTraits<T>>::radixSortLSDInternalWithSOA<DIRECT_WRITE_TO_DESTINATION>(arr, indices, size, reverse, destination);

    using K = typename RadixSortNumTraits<T>::Key;
    using C = typename RadixSortNumTraits<T>::CountType;

    constexpr size_t key_bytes = sizeof(K);
    constexpr bool is_signed = std::is_signed_v<K>;
    constexpr bool is_floating_point = std::is_floating_point_v<K>;
    constexpr size_t step = sizeof(__m512i) / sizeof(K);

    PaddedPODArray<K> temp_arr(size);
    PaddedPODArray<UInt32> temp_indices(size);

    for (int k = 0; k < key_bytes; ++k)
    {
        alignas(64) int histogram[256] = {0};

        size_t i = 0;
        for (; i + step - 1 < size; i += step)
        {
            __m512i values = _mm512_loadu_si512(&arr[i]);
            for (int b = 0; b < step; ++b)
            {
                uint8_t byte = (reinterpret_cast<T *>(&values)[b] >> (k * 8)) & 0xFF;
                histogram[byte]++;
            }
        }

        // 处理剩余元素
        for (; i < size; ++i)
        {
            uint8_t byte = (temp_arr[i] >> (k * 8)) & 0xFF;
            count[byte]++;
        }

        // 计算前缀和
        int prefix[256];
        prefix[0] = count[0];
        for (int j = 1; j < 256; ++j)
        {
            prefix[j] = prefix[j - 1] + count[j];
        }

        // 根据reverse参数决定排序顺序
        std::vector<T> next_arr(size);
        std::vector<uint32_t> next_indices(size);
        if (reverse)
        {
            for (int i = size - 1; i >= 0; --i)
            {
                uint8_t byte = (temp_arr[i] >> (k * 8)) & 0xFF;
                int pos = prefix[255 - byte] - 1;
                next_arr[pos] = temp_arr[i];
                next_indices[pos] = temp_indices[i];
                prefix[255 - byte]--;
            }
        }
        else
        {
            for (int i = size - 1; i >= 0; --i)
            {
                uint8_t byte = (temp_arr[i] >> (k * 8)) & 0xFF;
                int pos = prefix[byte] - 1;
                next_arr[pos] = temp_arr[i];
                next_indices[pos] = temp_indices[i];
                prefix[byte]--;
            }
        }
        temp_arr = next_arr;
        temp_indices = next_indices;
    }

    // 复制结果到输出数组
    memcpy(out_arr, temp_arr.data(), size * sizeof(T));
    memcpy(out_indices, temp_indices.data(), size * sizeof(uint32_t));
}

)



template <typename T, bool DIRECT_WRITE_TO_DESTINATION>
void radixSortLSDInternalWithSOA(
    typename RadixSortNumTraits<T>::Key * arr,
    UInt32 * indices,
    size_t size,
    bool reverse,
    size_t * destination)
{
    // RadixSort<RadixSortNumTraits<T>>::radixSortLSDInternalWithSOA<DIRECT_WRITE_TO_DESTINATION>(arr, indices, size, reverse, destination);
}

template <typename T>
void radixSortLSDWithSOA(
    typename RadixSortNumTraits<T>::Key * arr,
    UInt32 * indices,
    size_t size,
    bool reverse,
    size_t * destination)
{
    radixSortLSDInternalWithSOA<T, true>(arr, indices, size, reverse, destination);
}

template <typename T>
void radixSortLSDWithSOA(
    typename RadixSortNumTraits<T>::Key * arr,
    UInt32 * indices,
    size_t size)
{
    radixSortLSDInternalWithSOA<T, true>(arr, indices, size, false, nullptr);
}

template <typename T>
void radixSortLSDWithSOA(typename RadixSortNumTraits<T>::Key * arr, UInt32 * indices, size_t size, bool reverse)
{
    radixSortLSDInternalWithSOA<T, true>(arr, indices, size, reverse, nullptr);
}
*/
