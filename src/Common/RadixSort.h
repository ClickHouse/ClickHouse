#pragma once

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

#include <base/bit_cast.h>
#include <base/extended_types.h>
#include <base/sort.h>

#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/TargetSpecific.h>

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
    using KeyBits = std::conditional_t<sizeof(Key) == 8, uint64_t, std::conditional_t<sizeof(Key) == 4, uint32_t, uint16_t>>;

    static constexpr size_t PART_SIZE_BITS = 8;    /// With what pieces of the key, in bits, to do one pass - reshuffle of the array.

    /// Converting a key into KeyBits is such that the order relation over the key corresponds to the order relation over KeyBits.
    using Transform = RadixSortFloatTransform<KeyBits>;

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
    static constexpr size_t PREFETCH_DISTANCE = std::max(1UL, 64 / sizeof(Element) / 2);
    static constexpr size_t UNROLL_DISTANCE = 512 / (8 * sizeof(Element));

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

    /// SOFTWARE_PREFETCH is used only on Intel CPUs, where software prefetching is beneficial
    /// On AMD x86 CPUs and on ARM, it does not help or even slows down
    /// See more measurements in https://github.com/ClickHouse/ClickHouse/pull/86378
    template <bool DIRECT_WRITE_TO_DESTINATION, bool SOFTWARE_PREFETCH>
    static NO_INLINE void radixSortLSDInternal(Element * arr, size_t size, bool reverse, Result * destination)
    {
        /// If the array is smaller than 256, then it is better to use another algorithm.

        /// There are loops of NUM_PASSES. It is very important that they are unfolded at compile-time.

        /// For each of the NUM_PASSES bit ranges of the key, consider how many times each value of this bit range met.
        std::unique_ptr<CountType[]> histograms{new CountType[HISTOGRAM_SIZE * NUM_PASSES]{}};

        AllocatorWithMemoryTracking<typename Traits::Element> allocator;

        /// We will do several passes through the array. On each pass, the data is transferred to another array. Let's allocate this temporary array.
        Element * swap_buffer = allocator.allocate(size);

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
            /// Replace the histograms with the accumulated sums: the value in position i is the sum of the previous positions.
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

            size_t unrolled_end = (size / UNROLL_DISTANCE * UNROLL_DISTANCE);

            size_t i = 0;
            /// Unrolling allows for vectorized code, but prefetching disables it
            if constexpr (!SOFTWARE_PREFETCH && UNROLL_DISTANCE >= 2)
            {
                for (; i < unrolled_end; i += UNROLL_DISTANCE)
                {
                    size_t positions[UNROLL_DISTANCE];

                    for (size_t p = 0; p < UNROLL_DISTANCE; p++)
                        positions[p] = extractPart(pass, reader[i + p]);

                    for (size_t p = 0; p < UNROLL_DISTANCE; p++)
                    {
                        auto element = reader[i + p];

                        /// Place the element on the next free position.
                        auto & dest = writer[histograms[pass * HISTOGRAM_SIZE + positions[p]]];
                        ++histograms[pass * HISTOGRAM_SIZE + positions[p]];
                        dest = element;

                        /// On the last pass, we do the reverse transformation.
                        if (!Traits::Transform::transform_is_simple && pass == NUM_PASSES - 1)
                            Traits::extractKey(dest) = bitsToKey(Traits::Transform::backward(keyToBits(Traits::extractKey(element))));
                    }
                }
            }

            for (; i < size; i++)
            {
                auto element = reader[i];
                size_t pos = extractPart(pass, element);

                if constexpr (SOFTWARE_PREFETCH)
                {
                    /// Note that for prefetch to be effective, the distance must be large enough so that the memory access is ready
                    /// when we actually need it. This depends on CPU and memory subsystem.
                    if (i + PREFETCH_DISTANCE < size) [[likely]]
                    {
                        size_t next_pos = extractPart(pass, reader[i + PREFETCH_DISTANCE]);
                        __builtin_prefetch(&writer[histograms[pass * HISTOGRAM_SIZE + next_pos]], 1);
                    }
                }

                /// Place the element on the next free position.
                auto & dest = writer[histograms[pass * HISTOGRAM_SIZE + pos]];
                ++histograms[pass * HISTOGRAM_SIZE + pos];
                dest = element;

                /// On the last pass, we do the reverse transformation.
                if (!Traits::Transform::transform_is_simple && pass == NUM_PASSES - 1)
                    Traits::extractKey(dest) = bitsToKey(Traits::Transform::backward(keyToBits(Traits::extractKey(element))));
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
                    auto element = reader[i];
                    size_t pos = extractPart(pass, element);
                    if constexpr (SOFTWARE_PREFETCH)
                    {
                        if (i + PREFETCH_DISTANCE < size) [[likely]]
                        {
                            size_t next_pos = extractPart(pass, reader[i + PREFETCH_DISTANCE]);
                            __builtin_prefetch(&writer[size - 1 - histograms[pass * HISTOGRAM_SIZE + next_pos]], 1);
                        }
                    }
                    writer[size - 1 - (histograms[pass * HISTOGRAM_SIZE + pos])] = Traits::extractResult(element);
                    ++histograms[pass * HISTOGRAM_SIZE + pos];
                }
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    auto element = reader[i];
                    size_t pos = extractPart(pass, element);
                    if constexpr (SOFTWARE_PREFETCH)
                    {
                        if (i + PREFETCH_DISTANCE < size)
                        {
                            size_t next_pos = extractPart(pass, reader[i + PREFETCH_DISTANCE]);
                            __builtin_prefetch(&writer[histograms[pass * HISTOGRAM_SIZE + next_pos]], 1);
                        }
                    }
                    writer[histograms[pass * HISTOGRAM_SIZE + pos]] = Traits::extractResult(element);
                    ++histograms[pass * HISTOGRAM_SIZE + pos];
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

        allocator.deallocate(swap_buffer, size);
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
#if USE_MULTITARGET_CODE
        if (DB::isArchSupported(DB::TargetArch::GenuineIntel))
        {
            radixSortLSDInternal<DIRECT_WRITE_TO_DESTINATION, true>(arr, size, reverse, destination);
            return;
        }
#endif
        radixSortLSDInternal<DIRECT_WRITE_TO_DESTINATION, false>(arr, size, reverse, destination);
    }

public:
    /** Least significant digit radix sort (stable).
      * This function will sort inplace (modify 'arr')
      */
    static void executeLSD(Element * arr, size_t size)
    {
#if USE_MULTITARGET_CODE
        if (DB::isArchSupported(DB::TargetArch::GenuineIntel))
        {
            radixSortLSDInternal<false, true>(arr, size, false, nullptr);
            return;
        }
#endif
        radixSortLSDInternal<false, false>(arr, size, false, nullptr);
    }

    static void executeLSD(Element * arr, size_t size, bool reverse)
    {
#if USE_MULTITARGET_CODE
        if (DB::isArchSupported(DB::TargetArch::GenuineIntel))
        {
            radixSortLSDInternal<false, true>(arr, size, reverse, nullptr);
            return;
        }
#endif
        radixSortLSDInternal<false, false>(arr, size, reverse, nullptr);
    }

    /** This function will start to sort inplace (modify 'arr')
      *  but on the last step it will write result directly to the destination
      *  instead of finishing sorting 'arr'.
      * In this case it will fill only Result parts of the Element into destination.
      * It is handy to avoid unnecessary data movements.
      */
    static void executeLSD(Element * arr, size_t size, bool reverse, Result * destination)
    {
#if USE_MULTITARGET_CODE
        if (DB::isArchSupported(DB::TargetArch::GenuineIntel))
        {
            radixSortLSDInternal<true, true>(arr, size, reverse, destination);
            return;
        }
#endif
        radixSortLSDInternal<true, false>(arr, size, reverse, destination);
    }

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
