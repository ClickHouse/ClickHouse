#pragma once

#include <bit>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <Common/HashTable/HashTableKeyHolder.h>

namespace DB
{

/// The `limit` groups with the largest counts, kept up to date while the counts grow, so that no pass over all the
/// counted groups selects them afterwards. Counts only grow, so the smallest count of a full top only rises: an
/// update whose count does not exceed it cannot change the top and costs one comparison. A growing count leaves
/// the smallest counts, so an ascending order cannot be kept this way.
///
/// The caller keeps the count of every group in a 64-bit word and passes the word to `update` after each growth.
/// The low `count_bits` bits hold the count. The high bits belong to the top and are zero in the word of a new
/// group: there the top keeps the index of the group's entry plus one, so it finds the entry of an updated group
/// without a search. A group evicted from the top keeps its stale index, so the entry an index names is checked by
/// the full key: a hash does not identify a group. A key comes in a key holder (see `keyHolderGetKey`), and a new
/// entry keeps it made persistent, so a key that views memory gone before the updates end comes in a holder that
/// copies it. `max_limit` is the largest `limit` the top takes, which sizes the index bits.
template <typename Key, size_t max_limit>
class GrowingCountTopK
{
public:
    /// A group of the top: its key, its hash, which the top only keeps for the caller, and its count.
    struct Group
    {
        Key key;
        UInt64 hash;
        UInt64 count;
    };

    /// The index bits hold every index of the largest limit plus one, and the count bits more rows than an
    /// aggregation reads.
    static constexpr size_t count_bits = 64 - std::bit_width(max_limit);
    static constexpr UInt64 count_mask = (UInt64{1} << count_bits) - 1;

    explicit GrowingCountTopK(size_t limit_)
        : limit(limit_)
    {
        chassert(limit > 0 && limit <= max_limit);
    }

    /// Starts over with no groups. The words of the groups passed before must not be passed again: their indices
    /// name entries that are gone.
    void clear()
    {
        groups.clear();
        heap.clear();
        heap_positions.clear();
        min_count = 0;
    }

    /// Takes the word of a group whose count grew, with the group's key in a key holder.
    template <typename KeyHolder>
    void ALWAYS_INLINE update(UInt64 & word, KeyHolder && key_holder, UInt64 hash)
    {
        const UInt64 count = word & count_mask;
        /// Until the top is full, `min_count` is zero, so every count enters, being positive.
        chassert(count > 0);
        if (count > min_count)
            enter(word, count, key_holder, hash);
    }

    /// The groups with the `limit` largest counts so far, or all of them while there are fewer, in no particular
    /// order. Which of the groups with equal counts at the smallest are kept is unspecified.
    const std::vector<Group> & getGroups() const { return groups; }

private:
    template <typename KeyHolder>
    NO_INLINE void enter(UInt64 & word, UInt64 count, KeyHolder & key_holder, UInt64 hash)
    {
        /// The high bits of the word name the group's entry by its index plus one, or no entry by zero.
        const UInt64 entry = word >> count_bits;
        chassert(entry <= groups.size());
        if (entry && groups[entry - 1].key == keyHolderGetKey(key_holder))
        {
            /// A grown count can only move its index away from the root of the min-heap.
            groups[entry - 1].count = count;
            siftDown(heap_positions[entry - 1]);
        }
        else
        {
            keyHolderPersistKey(key_holder);
            const Key & key = keyHolderGetKey(key_holder);
            UInt32 index = 0;
            if (heap.size() < limit)
            {
                index = static_cast<UInt32>(groups.size());
                groups.push_back({key, hash, count});
                heap.push_back(index);
                heap_positions.push_back(static_cast<UInt32>(heap.size() - 1));
                siftUp(heap.size() - 1);
            }
            else
            {
                /// The group with the smallest count leaves the top, and the new one takes its entry.
                index = heap.front();
                groups[index] = {key, hash, count};
                siftDown(0);
            }
            word = count | (UInt64{index + 1} << count_bits);
        }
        if (heap.size() == limit)
            min_count = groups[heap.front()].count;
    }

    /// Moves the index at `position` of the min-heap toward the root while its count is below its parent's.
    void siftUp(size_t position)
    {
        const UInt32 index = heap[position];
        while (position > 0)
        {
            const size_t parent = (position - 1) / 2;
            if (groups[heap[parent]].count <= groups[index].count)
                break;
            place(position, heap[parent]);
            position = parent;
        }
        place(position, index);
    }

    /// Moves the index at `position` of the min-heap away from the root while its count is above a child's.
    void siftDown(size_t position)
    {
        const UInt32 index = heap[position];
        while (true)
        {
            size_t child = 2 * position + 1;
            if (child >= heap.size())
                break;
            if (child + 1 < heap.size() && groups[heap[child + 1]].count < groups[heap[child]].count)
                ++child;
            if (groups[index].count <= groups[heap[child]].count)
                break;
            place(position, heap[child]);
            position = child;
        }
        place(position, index);
    }

    /// Puts `index` at `position` of the min-heap.
    void place(size_t position, UInt32 index)
    {
        heap[position] = index;
        heap_positions[index] = static_cast<UInt32>(position);
    }

    const size_t limit;
    /// The groups by their indices, the indices as a min-heap by count, and the position of every index in it.
    std::vector<Group> groups;
    std::vector<UInt32> heap;
    std::vector<UInt32> heap_positions;
    /// The smallest count of the top once it is full, zero before.
    UInt64 min_count = 0;
};

}
