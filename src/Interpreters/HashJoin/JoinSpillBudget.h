#pragma once

#include <Columns/IColumn.h>
#include <Core/Defines.h>
#include <Common/BitHelpers.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <limits>

namespace DB
{

/// The admission rule of a hash join that may spill: an insert that would add `bytes` is admitted only if what the join
/// holds, what other inserts in progress reserved and `bytes` stay under the limit (`max_bytes_before_external_join`).
/// An insert reserves first and loads what the join holds after that; the other side adds what it allocated to what the
/// join holds first and releases its reservation after that. So a check sees every byte at least once.
class JoinSpillBudget
{
public:
    using Limit = std::function<size_t()>;

    explicit JoinSpillBudget(Limit limit_) : limit(std::move(limit_)) {}

    template <typename LoadHeld>
    bool tryReserve(size_t bytes, LoadHeld && load_held)
    {
        const size_t current_limit = limit();
        if (bytes >= current_limit)
            return false;

        const size_t prev = reserved.fetch_add(bytes, std::memory_order_acq_rel);
        const size_t held = load_held();
        if (add(add(held, prev), bytes) >= current_limit)
        {
            reserved.fetch_sub(bytes, std::memory_order_release);
            return false;
        }
        return true;
    }

    /// For an insert that goes ahead whatever the limit says. Returns the bytes reserved, to pass to `release`.
    size_t reserveUnchecked(size_t bytes)
    {
        bytes = std::min(bytes, limit());
        reserved.fetch_add(bytes, std::memory_order_acq_rel);
        return bytes;
    }

    /// Call after the bytes are added to what the join holds, or after they are freed.
    void release(size_t bytes) { reserved.fetch_sub(bytes, std::memory_order_release); }

    size_t getReserved() const { return reserved.load(std::memory_order_acquire); }
    size_t getLimit() const { return limit(); }

    static size_t add(size_t a, size_t b)
    {
        size_t res = 0;
        return common::addOverflow(a, b, res) ? std::numeric_limits<size_t>::max() : res;
    }

    static size_t mul(size_t a, size_t b)
    {
        size_t res = 0;
        return common::mulOverflow(a, b, res) ? std::numeric_limits<size_t>::max() : res;
    }

    /// The most a `PODArray` of `elements` elements allocates: the requested size with padding, rounded up to a power of two.
    static size_t podArrayBytes(size_t elements, size_t element_size, size_t pad_left = 0, size_t pad_right = 0)
    {
        if (!elements)
            return 0;
        const size_t bytes = add(mul(elements, element_size), add(pad_left, pad_right));
        if (bytes > std::numeric_limits<size_t>::max() / 2)
            return std::numeric_limits<size_t>::max();
        return roundUpToPowerOfTwoOrZero(bytes);
    }

    template <typename T>
    static size_t paddedPodArrayBytes(size_t elements)
    {
        return podArrayBytes(elements, sizeof(T), /*pad_left=*/PADDING_FOR_SIMD, /*pad_right=*/PADDING_FOR_SIMD - 1);
    }

    /// The most a copy of columns of `bytes` bytes allocates when it grows by appending into `destinations` new columns:
    /// appends end at no more than twice the elements, the array being reallocated also holds its old buffer, and a first
    /// append allocates 4096 bytes for each of the `arrays` arrays of each destination.
    static size_t appendCopyBytes(size_t bytes, size_t largest_column_bytes, size_t arrays, size_t destinations)
    {
        static constexpr size_t initial_bytes = 4096;
        return add(add(mul(bytes, 2), largest_column_bytes), mul(mul(destinations, arrays), initial_bytes));
    }

    /// `appendCopyBytes` of columns, whose sizes are `byteSize`. Two arrays per column and subcolumn.
    template <typename ColumnsRange>
    static size_t columnsCopyBytes(const ColumnsRange & columns, size_t destinations)
    {
        size_t bytes = 0;
        size_t largest = 0;
        size_t arrays = 0;
        for (const auto & column : columns)
        {
            const size_t column_bytes = column->byteSize();
            bytes = add(bytes, column_bytes);
            largest = std::max(largest, column_bytes);
            arrays += 2;
            column->forEachSubcolumnRecursively([&](const IColumn &) { arrays += 2; });
        }
        return appendCopyBytes(bytes, largest, arrays, destinations);
    }

private:
    Limit limit;
    std::atomic<size_t> reserved{0};
};

}
