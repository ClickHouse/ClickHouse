#pragma once
/**
  * This file contains some using-declarations that define various kinds of
  * PODArray.
  */

#include <Core/Defines.h>
#include <base/types.h>
#include <Common/Allocator_fwd.h>

namespace DB
{

constexpr size_t integerRoundUp(size_t value, size_t dividend)
{
    return ((value + dividend - 1) / dividend) * dividend;
}

template <typename T, size_t initial_bytes = 4096,
          typename TAllocator = Allocator<false>, size_t pad_right_ = 0,
          size_t pad_left_ = 0>
class PODArray;

/** For columns. Padding is enough to read and write xmm-register at the address of the last element. */
template <typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>>
using PaddedPODArray = PODArray<T, initial_bytes, TAllocator, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

/** A helper for declaring PODArray that uses inline memory.
  * The initial size is set to use all the inline bytes, since using less would
  * only add some extra allocation calls.
  */
template <typename T, size_t inline_bytes,
          size_t rounded_bytes = integerRoundUp(inline_bytes, sizeof(T))>
using PODArrayWithStackMemory = PODArray<T, rounded_bytes,
    AllocatorWithStackMemory<Allocator<false>, rounded_bytes, alignof(T)>>;

}
