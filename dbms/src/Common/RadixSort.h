#pragma once


#include <string.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <type_traits>

#include <ext/bit_cast.h>
#include <Core/Types.h>
#include <Core/Defines.h>


/** Radix sort, has the following functionality:
  * Can sort unsigned, signed numbers, and floats.
  * Can sort an array of fixed length elements that contain something else besides the key.
  * Customizable radix size.
  *
  * LSB, stable.
  * NOTE For some applications it makes sense to add MSB-radix-sort,
  *  as well as radix-select, radix-partial-sort, radix-get-permutation algorithms based on it.
  */


/** Used as a template parameter. See below.
  */
struct RadixSortMallocAllocator
{
    void * allocate(size_t size)
    {
        return malloc(size);
    }

    void deallocate(void * ptr, size_t /*size*/)
    {
        return free(ptr);
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
    using Element = TElement;     /// The type of the element. It can be a structure with a key and some other payload. Or just a key.
    using Key = Element;          /// The key to sort by.
    using CountType = uint32_t;   /// Type for calculating histograms. In the case of a known small number of elements, it can be less than size_t.

    /// The type to which the key is transformed to do bit operations. This UInt is the same size as the key.
    using KeyBits = std::conditional_t<sizeof(Key) == 8, uint64_t, uint32_t>;

    static constexpr size_t PART_SIZE_BITS = 8;    /// With what pieces of the key, in bits, to do one pass - reshuffle of the array.

    /// Converting a key into KeyBits is such that the order relation over the key corresponds to the order relation over KeyBits.
    using Transform = RadixSortFloatTransform<KeyBits>;

    /// An object with the functions allocate and deallocate.
    /// Can be used, for example, to allocate memory for a temporary array on the stack.
    /// To do this, the allocator itself is created on the stack.
    using Allocator = RadixSortMallocAllocator;

    /// The function to get the key from an array element.
    static Key & extractKey(Element & elem) { return elem; }

    /// Used when fallback to comparison based sorting is needed.
    /// TODO: Correct handling of NaNs, NULLs, etc
    static bool less(Key x, Key y)
    {
        return x < y;
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
    using Key = Element;
    using CountType = uint32_t;
    using KeyBits = Key;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortIdentityTransform<KeyBits>;
    using Allocator = RadixSortMallocAllocator;

    static Key & extractKey(Element & elem) { return elem; }

    static bool less(Key x, Key y)
    {
        return x < y;
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
    using Key = Element;
    using CountType = uint32_t;
    using KeyBits = std::make_unsigned_t<Key>;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortSignedTransform<KeyBits>;
    using Allocator = RadixSortMallocAllocator;

    static Key & extractKey(Element & elem) { return elem; }

    static bool less(Key x, Key y)
    {
        return x < y;
    }
};


template <typename T>
using RadixSortNumTraits =
    std::conditional_t<std::is_integral_v<T>,
        std::conditional_t<std::is_unsigned_v<T>,
            RadixSortUIntTraits<T>,
            RadixSortIntTraits<T>>,
        RadixSortFloatTraits<T>>;


template <typename Traits>
struct RadixSort
{
private:
    using Element     = typename Traits::Element;
    using Key         = typename Traits::Key;
    using CountType   = typename Traits::CountType;
    using KeyBits     = typename Traits::KeyBits;

    // Use insertion sort if the size of the array is less than equal to this threshold
    static constexpr size_t INSERTION_SORT_THRESHOLD = 64;

    static constexpr size_t HISTOGRAM_SIZE = 1 << Traits::PART_SIZE_BITS;
    static constexpr size_t PART_BITMASK = HISTOGRAM_SIZE - 1;
    static constexpr size_t KEY_BITS = sizeof(Key) * 8;
    static constexpr size_t NUM_PASSES = (KEY_BITS + (Traits::PART_SIZE_BITS - 1)) / Traits::PART_SIZE_BITS;

    static ALWAYS_INLINE KeyBits getPart(size_t N, KeyBits x)
    {
        if (Traits::Transform::transform_is_simple)
            x = Traits::Transform::forward(x);

        return (x >> (N * Traits::PART_SIZE_BITS)) & PART_BITMASK;
    }

    static KeyBits keyToBits(Key x) { return ext::bit_cast<KeyBits>(x); }
    static Key bitsToKey(KeyBits x) { return ext::bit_cast<Key>(x); }

    static void insertionSortInternal(Element *arr, size_t size)
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

    /* Main MSD radix sort subroutine
     * Puts elements to buckets based on PASS-th digit, then recursively calls insertion sort or itself on the buckets
     */
    template <size_t PASS>
    static inline void radixSortMSDInternal(Element * arr, size_t size, size_t limit)
    {
        Element * last_list[HISTOGRAM_SIZE + 1];
        Element ** last = last_list + 1;
        size_t count[HISTOGRAM_SIZE] = {0};

        for (Element * i = arr; i < arr + size; ++i)
            ++count[getPart(PASS, *i)];

        last_list[0] = last_list[1] = arr;

        size_t buckets_for_recursion = HISTOGRAM_SIZE;
        Element * finish = arr + size;
        for (size_t i = 1; i < HISTOGRAM_SIZE; ++i)
        {
            last[i] = last[i - 1] + count[i - 1];
            if (last[i] >= arr + limit)
            {
                buckets_for_recursion = i;
                finish = last[i];
            }
        }

        /* At this point, we have the following variables:
         * count[i] is the size of i-th bucket
         * last[i] is a pointer to the beginning of i-th bucket, last[-1] == last[0]
         * buckets_for_recursion is the number of buckets that should be sorted, the last of them only partially
         * finish is a pointer to the end of the first buckets_for_recursion buckets
         */

        // Scatter array elements to buckets until the first buckets_for_recursion buckets are full
        for (size_t i = 0; i < buckets_for_recursion; ++i)
        {
            Element * end = last[i - 1] + count[i];
            if (end == finish)
            {
                last[i] = end;
                break;
            }
            while (last[i] != end)
            {
                Element swapper = *last[i];
                KeyBits tag = getPart(PASS, swapper);
                if (tag != i)
                {
                    do
                    {
                        std::swap(swapper, *last[tag]++);
                    } while ((tag = getPart(PASS, swapper)) != i);
                    *last[i] = swapper;
                }
                ++last[i];
            }
        }

        if constexpr (PASS > 0)
        {
            // Recursively sort buckets, except the last one
            for (size_t i = 0; i < buckets_for_recursion - 1; ++i)
            {
                Element * start = last[i - 1];
                size_t subsize = last[i] - last[i - 1];
                radixSortMSDInternalHelper<PASS - 1>(start, subsize, subsize);
            }

            // Sort last necessary bucket with limit
            Element * start = last[buckets_for_recursion - 2];
            size_t subsize = last[buckets_for_recursion - 1] - last[buckets_for_recursion - 2];
            size_t sublimit = limit - (last[buckets_for_recursion - 1] - arr);
            radixSortMSDInternalHelper<PASS - 1>(start, subsize, sublimit);
        }
    }

    // A helper to choose sorting algorithm based on array length
    template <size_t PASS>
    static inline void radixSortMSDInternalHelper(Element * arr, size_t size, size_t limit)
    {
        if (size <= INSERTION_SORT_THRESHOLD)
            insertionSortInternal(arr, size);
        else
            radixSortMSDInternal<PASS>(arr, size, limit);
    }

public:
    /// Least significant digit radix sort (stable)
    static void executeLSD(Element * arr, size_t size)
    {
        /// If the array is smaller than 256, then it is better to use another algorithm.

        /// There are loops of NUM_PASSES. It is very important that they are unfolded at compile-time.

        /// For each of the NUM_PASSES bit ranges of the key, consider how many times each value of this bit range met.
        CountType histograms[HISTOGRAM_SIZE * NUM_PASSES] = {0};

        typename Traits::Allocator allocator;

        /// We will do several passes through the array. On each pass, the data is transferred to another array. Let's allocate this temporary array.
        Element * swap_buffer = reinterpret_cast<Element *>(allocator.allocate(size * sizeof(Element)));

        /// Transform the array and calculate the histogram.
        /// NOTE This is slightly suboptimal. Look at https://github.com/powturbo/TurboHist
        for (size_t i = 0; i < size; ++i)
        {
            if (!Traits::Transform::transform_is_simple)
                Traits::extractKey(arr[i]) = bitsToKey(Traits::Transform::forward(keyToBits(Traits::extractKey(arr[i]))));

            for (size_t pass = 0; pass < NUM_PASSES; ++pass)
                ++histograms[pass * HISTOGRAM_SIZE + getPart(pass, keyToBits(Traits::extractKey(arr[i])))];
        }

        {
            /// Replace the histograms with the accumulated sums: the value in position i is the sum of the previous positions minus one.
            size_t sums[NUM_PASSES] = {0};

            for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
            {
                for (size_t pass = 0; pass < NUM_PASSES; ++pass)
                {
                    size_t tmp = histograms[pass * HISTOGRAM_SIZE + i] + sums[pass];
                    histograms[pass * HISTOGRAM_SIZE + i] = sums[pass] - 1;
                    sums[pass] = tmp;
                }
            }
        }

        /// Move the elements in the order starting from the least bit piece, and then do a few passes on the number of pieces.
        for (size_t pass = 0; pass < NUM_PASSES; ++pass)
        {
            Element * writer = pass % 2 ? arr : swap_buffer;
            Element * reader = pass % 2 ? swap_buffer : arr;

            for (size_t i = 0; i < size; ++i)
            {
                size_t pos = getPart(pass, keyToBits(Traits::extractKey(reader[i])));

                /// Place the element on the next free position.
                auto & dest = writer[++histograms[pass * HISTOGRAM_SIZE + pos]];
                dest = reader[i];

                /// On the last pass, we do the reverse transformation.
                if (!Traits::Transform::transform_is_simple && pass == NUM_PASSES - 1)
                    Traits::extractKey(dest) = bitsToKey(Traits::Transform::backward(keyToBits(Traits::extractKey(reader[i]))));
            }
        }

        /// If the number of passes is odd, the result array is in a temporary buffer. Copy it to the place of the original array.
        /// NOTE Sometimes it will be more optimal to provide non-destructive interface, that will not modify original array.
        if (NUM_PASSES % 2)
            memcpy(arr, swap_buffer, size * sizeof(Element));

        allocator.deallocate(swap_buffer, size * sizeof(Element));
    }

    /* Most significant digit radix sort
     * Usually slower than LSD and is not stable, but allows partial sorting
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
void radixSortLSD(T *arr, size_t size)
{
    RadixSort<RadixSortNumTraits<T>>::executeLSD(arr, size);
}

template <typename T>
void radixSortMSD(T *arr, size_t size, size_t limit)
{
    RadixSort<RadixSortNumTraits<T>>::executeMSD(arr, size, limit);
}
