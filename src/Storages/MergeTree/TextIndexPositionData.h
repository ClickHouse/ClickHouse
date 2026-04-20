#pragma once

#include <base/types.h>
#include <Common/HashTable/StringHashMap.h>

#include <vector>

namespace DB
{

/// Roaringish encoding for positional phrase queries.
///
/// Each token occurrence is packed into a UInt64:
///   [doc_id:32][group:16][bitmap:16]
///
/// where group = position / 16, bitmap = 1 << (position % 16).
/// Multiple occurrences in the same (doc_id, group) are OR'd into one bitmap,
/// so a single entry can represent up to 16 adjacent positions.
///
/// Phrase matching reduces to sorted-array intersection with bitmap shifts.
/// The upper 48 bits (doc_id + group) serve as the intersection key.
/// This layout is a natural operand for AVX-512 VP2INTERSECTQ.
struct RoaringishEntry
{
    UInt64 value;

    static constexpr UInt32 BITMAP_BITS = 16;
    static constexpr UInt64 BITMAP_MASK = (1ULL << BITMAP_BITS) - 1;
    static constexpr UInt32 GROUP_BITS = 16;
    static constexpr UInt32 DOC_ID_BITS = 32;

    static RoaringishEntry make(UInt32 doc_id, UInt32 position)
    {
        UInt16 group = static_cast<UInt16>(position / BITMAP_BITS);
        UInt16 bitmap = static_cast<UInt16>(1U << (position % BITMAP_BITS));

        UInt64 v = (static_cast<UInt64>(doc_id) << 32)
                 | (static_cast<UInt64>(group) << 16)
                 | static_cast<UInt64>(bitmap);
        return {v};
    }

    /// Extract the upper 48 bits (doc_id + group) used as the intersection key.
    UInt64 key() const { return value >> BITMAP_BITS; }
    UInt32 docId() const { return static_cast<UInt32>(value >> 32); }
    UInt16 group() const { return static_cast<UInt16>(value >> 16); }
    UInt16 bitmap() const { return static_cast<UInt16>(value & BITMAP_MASK); }

    bool operator<(const RoaringishEntry & other) const { return value < other.value; }
    bool operator==(const RoaringishEntry & other) const { return value == other.value; }

    /// Two entries have the same bucket if they share (doc_id, group).
    bool sameBucket(const RoaringishEntry & other) const
    {
        return key() == other.key();
    }

    void mergeBitmap(const RoaringishEntry & other)
    {
        value |= (other.value & BITMAP_MASK);
    }

    /// Return a copy with a different doc_id, preserving group and bitmap.
    RoaringishEntry withDocId(UInt32 new_doc_id) const
    {
        return {(static_cast<UInt64>(new_doc_id) << 32) | (value & 0xFFFFFFFF)};
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
