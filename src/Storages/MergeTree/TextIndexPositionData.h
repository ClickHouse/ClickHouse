#pragma once

#include <base/types.h>
#include <Common/HashTable/StringHashMap.h>

#include <vector>

namespace DB
{

/// Roaringish encoding for positional phrase queries.
///
/// Each token occurrence is packed into a UInt128:
///   [doc_id:32][group:32][bitmap:64]
///
/// where group = position / 64, bitmap = 1ULL << (position % 64).
/// Multiple occurrences in the same (doc_id, group) are OR'd into one bitmap,
/// so a single entry can represent up to 64 adjacent positions.
///
/// Phrase matching reduces to sorted-array intersection with bitmap shifts.
struct RoaringishEntry
{
    UInt128 value;

    static constexpr UInt32 BITMAP_BITS = 64;

    static RoaringishEntry make(UInt32 doc_id, UInt32 position)
    {
        UInt32 group = position / BITMAP_BITS;
        UInt64 bitmap = 1ULL << (position % BITMAP_BITS);

        UInt128 v = (static_cast<UInt128>(doc_id) << 96)
                  | (static_cast<UInt128>(group) << 64)
                  | static_cast<UInt128>(bitmap);
        return {v};
    }

    /// Extract the upper 64 bits (doc_id + group) used as the intersection key.
    UInt64 key() const { return static_cast<UInt64>(value >> 64); }
    UInt32 docId() const { return static_cast<UInt32>(value >> 96); }
    UInt32 group() const { return static_cast<UInt32>(value >> 64); }
    UInt64 bitmap() const { return static_cast<UInt64>(value); }

    bool operator<(const RoaringishEntry & other) const { return value < other.value; }
    bool operator==(const RoaringishEntry & other) const { return value == other.value; }

    /// Two entries have the same bucket if they share (doc_id, group).
    bool sameBucket(const RoaringishEntry & other) const
    {
        return key() == other.key();
    }

    void mergeBitmap(const RoaringishEntry & other)
    {
        value |= static_cast<UInt128>(other.bitmap());
    }
};

/// Builder that accumulates Roaringish entries for a single token during index construction.
/// Entries are appended in (doc_id, position) order and same-bucket entries are merged on the fly.
class PositionListBuilder
{
public:
    void add(UInt32 doc_id, UInt32 position)
    {
        auto entry = RoaringishEntry::make(doc_id, position);

        if (!entries.empty() && entries.back().sameBucket(entry))
        {
            entries.back().mergeBitmap(entry);
        }
        else
        {
            entries.push_back(entry);
        }
    }

    size_t size() const { return entries.size(); }
    bool empty() const { return entries.empty(); }
    size_t memoryUsageBytes() const { return entries.capacity() * sizeof(RoaringishEntry); }
    const std::vector<RoaringishEntry> & getEntries() const { return entries; }
    std::vector<RoaringishEntry> & getEntries() { return entries; }

    /// Merge another builder's entries into this one.
    /// Both must be sorted. Result is sorted with same-bucket entries merged.
    void mergeFrom(const PositionListBuilder & other)
    {
        if (other.empty())
            return;

        if (entries.empty())
        {
            entries = other.entries;
            return;
        }

        std::vector<RoaringishEntry> merged;
        merged.reserve(entries.size() + other.entries.size());

        size_t i = 0;
        size_t j = 0;
        while (i < entries.size() && j < other.entries.size())
        {
            if (entries[i].sameBucket(other.entries[j]))
            {
                auto combined = entries[i];
                combined.mergeBitmap(other.entries[j]);
                merged.push_back(combined);
                ++i;
                ++j;
            }
            else if (entries[i] < other.entries[j])
            {
                merged.push_back(entries[i++]);
            }
            else
            {
                merged.push_back(other.entries[j++]);
            }
        }
        while (i < entries.size())
            merged.push_back(entries[i++]);
        while (j < other.entries.size())
            merged.push_back(other.entries[j++]);

        entries = std::move(merged);
    }

private:
    std::vector<RoaringishEntry> entries;
};

using TokenToPositionListMap = StringHashMap<PositionListBuilder>;

}
