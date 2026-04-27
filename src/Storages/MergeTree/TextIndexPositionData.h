#pragma once

#include <base/types.h>
#include <Common/HashTable/StringHashMap.h>

#include <tuple>
#include <vector>

namespace DB
{

/// Roaringish encoding for positional phrase queries.
///
/// Each token occurrence is stored as a 96-bit entry with three UInt32 fields:
///   [doc_id:32][group:32][bitmap:32]
///
/// where group = position / 32, bitmap = 1 << (position % 32).
/// Multiple occurrences in the same (doc_id, group) are OR'd into one bitmap,
/// so a single entry can represent up to 32 adjacent positions.
///
/// Phrase matching reduces to sorted-array intersection with bitmap shifts.
/// The (doc_id, group) pair serves as the intersection key.
struct RoaringishEntry
{
    UInt32 doc_id;
    UInt32 group;
    UInt32 bitmap;

    static constexpr UInt32 BITMAP_BITS = 32;

    static RoaringishEntry make(UInt32 doc_id_, UInt32 position)
    {
        return {doc_id_, position / BITMAP_BITS, 1U << (position % BITMAP_BITS)};
    }

    /// The (doc_id, group) pair used as the intersection key.
    UInt64 key() const { return (static_cast<UInt64>(doc_id) << 32) | group; }

    bool operator<(const RoaringishEntry & other) const
    {
        return std::tie(doc_id, group, bitmap) < std::tie(other.doc_id, other.group, other.bitmap);
    }

    bool operator==(const RoaringishEntry & other) const
    {
        return std::tie(doc_id, group, bitmap) == std::tie(other.doc_id, other.group, other.bitmap);
    }

    /// Two entries have the same bucket if they share (doc_id, group).
    bool sameBucket(const RoaringishEntry & other) const
    {
        return doc_id == other.doc_id && group == other.group;
    }

    void mergeBitmap(const RoaringishEntry & other)
    {
        chassert(sameBucket(other));
        bitmap |= other.bitmap;
    }

    /// Return a copy with a different doc_id, preserving group and bitmap.
    RoaringishEntry withDocId(UInt32 new_doc_id) const
    {
        return {new_doc_id, group, bitmap};
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
    std::vector<RoaringishEntry> & getEntries() { return entries; }
    const std::vector<RoaringishEntry> & getEntries() const { return entries; }

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
