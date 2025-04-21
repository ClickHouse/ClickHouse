#pragma once

#include <Core/Defines.h>
#include <base/types.h>
#include <Common/Allocator_fwd.h>

namespace DB
{

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right, size_t pad_left>
class PODArrayOwning;

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right, size_t pad_left>
class PODArrayView;

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right, size_t pad_left>
class PODArrayLimitView;

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_> // NOLINT
class IBuffer;

template <typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>>
using PaddedBuffer = IBuffer<T, initial_bytes, TAllocator, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

template<typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>>
using OwningBuffer = PODArrayOwning<T, initial_bytes, TAllocator, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

template<typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>>
using ViewBuffer = PODArrayView<T, initial_bytes, TAllocator, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

template<typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>>
using LimitViewBuffer = PODArrayLimitView<T, initial_bytes, TAllocator, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

}
