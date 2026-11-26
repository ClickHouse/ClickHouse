#pragma once

#include <Core/Defines.h>
#include <Common/BitHelpers.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <atomic>
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
    explicit JoinSpillBudget(size_t limit_) : limit(limit_) {}

    template <typename LoadHeld>
    bool tryReserve(size_t bytes, LoadHeld && load_held)
    {
        if (bytes >= limit)
            return false;

        const size_t prev = reserved.fetch_add(bytes, std::memory_order_acq_rel);
        const size_t held = load_held();
        if (add(add(held, prev), bytes) >= limit)
        {
            reserved.fetch_sub(bytes, std::memory_order_release);
            return false;
        }
        return true;
    }

    /// For an insert that goes ahead whatever the limit says. Returns the bytes reserved, to pass to `release`.
    size_t reserveUnchecked(size_t bytes)
    {
        bytes = std::min(bytes, limit);
        reserved.fetch_add(bytes, std::memory_order_acq_rel);
        return bytes;
    }

    /// Call after the bytes are added to what the join holds, or after they are freed.
    void release(size_t bytes) { reserved.fetch_sub(bytes, std::memory_order_release); }

    size_t getReserved() const { return reserved.load(std::memory_order_acquire); }
    size_t getLimit() const { return limit; }

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

    /// The most a `PaddedPODArray<T>` of `elements` elements allocates: the size with padding, rounded up to a power of two.
    template <typename T>
    static size_t paddedPodArrayBytes(size_t elements)
    {
        if (!elements)
            return 0;
        const size_t bytes = add(mul(elements, sizeof(T)), PADDING_FOR_SIMD + PADDING_FOR_SIMD - 1);
        if (bytes > std::numeric_limits<size_t>::max() / 2)
            return std::numeric_limits<size_t>::max();
        return roundUpToPowerOfTwoOrZero(bytes);
    }

private:
    const size_t limit;
    std::atomic<size_t> reserved{0};
};

}
