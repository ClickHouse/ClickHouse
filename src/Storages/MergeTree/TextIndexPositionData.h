#pragma once

#include <base/types.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/PODArray.h>

#include <algorithm>
#include <limits>
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
            return;
        }

        /// Array/Map columns restart positions per element, so a later add() can go backwards.
        if (!entries.empty() && entry.key() < entries.back().key())
            sorted = false;

        entries.push_back(entry);
    }

    void finalizeOrdering()
    {
        if (sorted)
            return;

        std::sort(entries.begin(), entries.end(),
            [](const RoaringishEntry & a, const RoaringishEntry & b) { return a.key() < b.key(); });

        size_t write = 0;
        for (size_t read = 1; read < entries.size(); ++read)
        {
            if (entries[write].sameBucket(entries[read]))
                entries[write].mergeBitmap(entries[read]);
            else
                entries[++write] = entries[read];
        }
        entries.resize(write + 1);
        sorted = true;
    }

    size_t size() const { return entries.size(); }
    bool empty() const { return entries.empty(); }
    size_t allocatedBytes() const { return entries.capacity() * sizeof(RoaringishEntry); }
    std::vector<RoaringishEntry> & getEntries() { return entries; }
    const std::vector<RoaringishEntry> & getEntries() const { return entries; }

private:
    std::vector<RoaringishEntry> entries;
    bool sorted = true;
};

using TokenToPositionListMap = StringHashMap<PositionListBuilder>;

/// Struct-of-arrays form of a roaringish position list, used on the query/decode path.
///
/// Decoding de-interleaves the on-disk AoS entries straight into these three lanes — no temp
/// buffers, no SoA->AoS copy — so the decoded footprint is just the data (12 bytes/entry).
/// Phrase search consumes this form directly, computing the (doc_id, group) merge key inline.
/// Entries are sorted ascending by key() and unique per (doc_id, group).
struct PositionList
{
    PaddedPODArray<UInt32> doc;
    PaddedPODArray<UInt32> group;
    PaddedPODArray<UInt32> bitmap;

    size_t size() const { return doc.size(); }
    bool empty() const { return doc.empty(); }
    void clear() { doc.clear(); group.clear(); bitmap.clear(); }
    void resize(size_t n) { doc.resize(n); group.resize(n); bitmap.resize(n); }
    void reserve(size_t n) { doc.reserve(n); group.reserve(n); bitmap.reserve(n); }

    /// (doc_id, group) intersection key.
    UInt64 key(size_t i) const { return (static_cast<UInt64>(doc[i]) << 32) | group[i]; }

    void pushBack(UInt32 d, UInt32 g, UInt32 b) { doc.push_back(d); group.push_back(g); bitmap.push_back(b); }
};

}
