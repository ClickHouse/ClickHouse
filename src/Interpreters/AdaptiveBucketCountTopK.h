#pragma once

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

/// The k largest `count()` values of one merged bucket, tracked while the drain and the bucket
/// merge increment them, so that the bucket-local Top-K conversion needs no scan of the table.
/// Counts only grow and the threshold (the smallest tracked count once k groups are tracked)
/// only grows with them, so a group whose final count exceeds the final threshold exceeded the
/// threshold of its time at some increment and was recorded then; a recorded group's later
/// increments update its entry. The key is kept as its staged byte image plus the table hash
/// (see `AdaptiveAggregationDetail::emplaceStagedKey`) so the conversion can look the group up
/// once the counts are final. Ties at the threshold keep the first-seen group, which is exact
/// for LIMIT semantics. `complete` is cleared when some path that changes a count cannot report
/// to the tracker; the conversion then falls back to the scan.
struct AdaptiveBucketCountTopK
{
    struct Entry
    {
        std::string key;
        UInt64 hash;
        UInt64 count;
    };

    explicit AdaptiveBucketCountTopK(size_t k_) : k(k_) { entries.reserve(k_); }

    ALWAYS_INLINE bool above(UInt64 count) const { return count > threshold; }

    void NO_INLINE consider(UInt64 count, std::string_view key, UInt64 hash)
    {
        for (auto & entry : entries)
        {
            if (entry.hash == hash && entry.key == key)
            {
                entry.count = count;
                updateThreshold();
                return;
            }
        }
        if (entries.size() < k)
        {
            entries.push_back({std::string(key), hash, count});
            updateThreshold();
            return;
        }
        auto weakest = std::min_element(entries.begin(), entries.end(), [](const Entry & a, const Entry & b) { return a.count < b.count; });
        weakest->key.assign(key.data(), key.size());
        weakest->hash = hash;
        weakest->count = count;
        updateThreshold();
    }

    size_t k;
    UInt64 threshold = 0;
    bool complete = true;
    std::vector<Entry> entries;

private:
    void updateThreshold()
    {
        if (entries.size() < k)
            return;
        threshold = std::min_element(entries.begin(), entries.end(), [](const Entry & a, const Entry & b) { return a.count < b.count; })->count;
    }
};

}
