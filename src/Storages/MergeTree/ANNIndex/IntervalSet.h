#pragma once

#include <base/types.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
}

/// A set of closed integer intervals `[lo, hi]` kept sorted and pairwise disjoint.
/// Overlapping or touching intervals (`r + 1 == next.l`) are merged automatically.
///
/// The main usage is to record coverage of `_block_number` ranges per partition in
/// `ANNGroupCoverage`. The template parameter `T` is expected to be an unsigned
/// integral type.
template <typename T>
class IntervalSet
{
public:
    struct Interval
    {
        T lo;
        T hi;

        bool operator==(const Interval & rhs) const noexcept = default;
    };

    /// Add interval `[lo, hi]`. Throws `BAD_ARGUMENTS` if `hi < lo`.
    /// Complexity: O(n) worst case (one shift). Amortised acceptable for the build path.
    void addInterval(T lo, T hi)
    {
        if (hi < lo)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "IntervalSet::addInterval: reversed interval [{}, {}]",
                UInt64(lo), UInt64(hi));

        /// Find the first interval with `lo_i > hi + 1` — anything before that potentially merges.
        /// We treat "touching" (hi + 1 == next.lo) as mergeable for T that supports it.
        const bool can_touch_merge = hi != std::numeric_limits<T>::max();
        const T merge_upper = can_touch_merge ? static_cast<T>(hi + 1) : hi;

        auto it = std::lower_bound(
            data.begin(), data.end(), lo,
            [](const Interval & iv, T value) { return iv.hi + 1 < value; });

        /// `it` is the first interval that could merge with or follow `[lo, hi]`.
        if (it == data.end() || it->lo > merge_upper)
        {
            /// No overlap/touch — insert standalone.
            data.insert(it, Interval{lo, hi});
            return;
        }

        /// Merge `it` and possibly subsequent intervals into a single interval.
        T new_lo = std::min(lo, it->lo);
        T new_hi = std::max(hi, it->hi);

        auto last = it + 1;
        while (last != data.end())
        {
            const bool touching = new_hi != std::numeric_limits<T>::max() && last->lo <= new_hi + 1;
            const bool overlapping = last->lo <= new_hi;
            if (!touching && !overlapping)
                break;
            new_hi = std::max(new_hi, last->hi);
            ++last;
        }

        *it = Interval{new_lo, new_hi};
        if (last != it + 1)
            data.erase(it + 1, last);
    }

    /// True if the query range `[lo, hi]` is *entirely contained* in one of the stored intervals.
    bool containsRange(T lo, T hi) const
    {
        if (hi < lo)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "IntervalSet::containsRange: reversed interval [{}, {}]",
                UInt64(lo), UInt64(hi));

        /// Find the first interval whose `hi >= lo` (candidate container).
        auto it = std::lower_bound(
            data.begin(), data.end(), lo,
            [](const Interval & iv, T value) { return iv.hi < value; });

        return it != data.end() && it->lo <= lo && hi <= it->hi;
    }

    const std::vector<Interval> & intervals() const { return data; }
    bool empty() const { return data.empty(); }
    size_t size() const { return data.size(); }

    /// Binary serialisation: UInt32 count followed by `count` × (T lo, T hi) in native endianness.
    /// Values are written verbatim; callers owning the stream decide endianness policy.
    void serialize(WriteBuffer & out) const
    {
        if (data.size() > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "IntervalSet::serialize: too many intervals ({})", data.size());

        const UInt32 count = static_cast<UInt32>(data.size());
        writePODBinary(count, out);
        for (const auto & iv : data)
        {
            writePODBinary(iv.lo, out);
            writePODBinary(iv.hi, out);
        }
    }

    void deserialize(ReadBuffer & in)
    {
        data.clear();

        UInt32 count = 0;
        readPODBinary(count, in);

        data.reserve(count);
        T prev_hi = 0;
        for (UInt32 i = 0; i < count; ++i)
        {
            T lo = 0;
            T hi = 0;
            readPODBinary(lo, in);
            readPODBinary(hi, in);
            if (hi < lo)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "IntervalSet::deserialize: reversed interval [{}, {}]",
                    UInt64(lo), UInt64(hi));
            if (i > 0 && lo <= prev_hi)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "IntervalSet::deserialize: intervals not sorted or overlapping");
            data.push_back(Interval{lo, hi});
            prev_hi = hi;
        }
    }

private:
    std::vector<Interval> data;
};

extern template class IntervalSet<UInt64>;

}
