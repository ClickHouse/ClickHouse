#pragma once

#include <Interpreters/RowRefs.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>

#include <cstring>

namespace DB
{

/** Appends the refs one build pass collected for a key to that key's `RowRefList` word, producing the
  * pair, run and list layouts documented on `RowRefList`. One writer per build worker: it allocates from
  * that worker's arena and keeps that worker's pair free list. The words it writes are read only after
  * the build's publication barrier, and a key is only ever appended to by the owner of its partition or
  * by the serial drain, so nothing here synchronizes.
  *
  * Sizing rules:
  *  - `grouped == false`, the build is one pass per key: every block is exact (`capacity == count`), so a
  *    key costs 8 bytes per row, 16 as a pair, nothing inline.
  *  - `grouped == true`: a contribution of fewer than `SMALL_BLOCK_REFS` rows goes into a 64-byte block
  *    whose free slots later passes fill in place, so a key of at most 8 rows never costs more than a
  *    `RowRefList::Batch` however its rows are spread over passes; larger contributions stay exact.
  * A key chains (gains a descriptor) the first time its refs no longer fit its block, or as soon as its
  * count reaches `COUNT_SAT`, because the word's count field is exact only below that. Chaining moves at
  * most one ref, once per key, and copies nothing else; the only slack a key can hold is the free part of
  * its one tail block, reused by the next append.
  */
class DuplicateRunWriter
{
public:
    static constexpr size_t SMALL_BLOCK_BYTES = 64;
    /// Slots of a standalone 64-byte block, and of one whose trailing link has not been written yet.
    static constexpr size_t SMALL_BLOCK_REFS = SMALL_BLOCK_BYTES / sizeof(UInt64);
    /// Slots of a 64-byte block behind a leading link word.
    static constexpr size_t SMALL_NODE_REFS = SMALL_BLOCK_REFS - 1;
    static constexpr size_t PAIR_BYTES = 2 * sizeof(UInt64);
    static constexpr size_t DESCRIPTOR_BYTES = sizeof(RowRefList::RunListDescriptor);

    /// Signed: a later group's writer, or the drain's, fills the free slots of a block another writer
    /// allocated and frees pairs it never counted, so a single writer's `pairs` and `slack_slots` can go
    /// negative. Only the sum over all writers of a build is meaningful.
    struct Stats
    {
        Int64 pairs = 0; /// pair blocks currently referenced by a word
        Int64 runs = 0; /// headerless runs created (a run stays counted once it chains)
        Int64 descriptors = 0;
        Int64 appended_nodes = 0; /// nodes linked behind a first run
        Int64 small_blocks = 0; /// 64-byte blocks, standalone or appended
        Int64 in_place_fills = 0; /// appends that (partly) landed in an existing block's free slots
        Int64 moved_refs = 0; /// refs moved out of a full block when it chained
        Int64 arena_bytes = 0; /// bytes taken from the arena for refs, links and descriptors
        Int64 slack_slots = 0; /// free ref slots in the current tail blocks
    };

    DuplicateRunWriter(Arena & arena_, bool grouped_)
        : arena(arena_)
        , grouped(grouped_)
    {
    }

    /// Appends `count >= 1` refs, in this order, to the key whose word is `mapped` (0 when the key has no
    /// row yet). Every ref must be an encoded inline `RowRef` word.
    void append(RowRefList & mapped, const UInt64 * refs, size_t count)
    {
        chassert(count > 0);

        if (mapped.word == 0)
        {
            if (count == 1)
            {
                mapped.word = refs[0];
                return;
            }
            startRun(mapped, nullptr, 0, refs, count);
            return;
        }

        if (mapped.isInline())
        {
            const UInt64 first = mapped.word;
            startRun(mapped, &first, 1, refs, count);
            return;
        }

        if (mapped.isPair())
        {
            UInt64 * pair = mapped.runRefsMutable();
            const UInt64 old[2] = {pair[0], pair[1]};
            freePair(pair);
            startRun(mapped, old, 2, refs, count);
            return;
        }

        if (mapped.isRun())
        {
            UInt64 * block = mapped.runRefsMutable();
            const size_t have = mapped.countField();
            const size_t capacity = standaloneCapacity(have);
            if (have + count <= capacity && have + count < RowRefList::COUNT_SAT)
            {
                memcpy(block + have, refs, count * sizeof(UInt64));
                st.slack_slots -= count;
                ++st.in_place_fills;
                mapped = RowRefList::makeRun(block, have + count);
                return;
            }
            chainFirstRun(mapped, block, have, capacity, refs, count);
            return;
        }

        chassert(mapped.isList());
        appendToList(mapped, refs, count);
    }

    const Stats & stats() const { return st; }

    /// Arena bytes a reader can still reach: allocated bytes minus the pair slots parked in the free list.
    UInt64 liveBytes() const { return static_cast<UInt64>(st.arena_bytes) - free_pairs.size() * PAIR_BYTES; }

private:
    Arena & arena;
    const bool grouped;
    PODArray<UInt64 *> free_pairs;
    Stats st;

    UInt64 * allocWords(size_t words)
    {
        st.arena_bytes += words * sizeof(UInt64);
        return reinterpret_cast<UInt64 *>(arena.alignedAlloc(words * sizeof(UInt64), alignof(UInt64)));
    }

    UInt64 * allocPair()
    {
        ++st.pairs;
        if (!free_pairs.empty())
        {
            UInt64 * pair = free_pairs.back();
            free_pairs.pop_back();
            return pair;
        }
        return allocWords(2);
    }

    void freePair(UInt64 * pair)
    {
        --st.pairs;
        free_pairs.push_back(pair);
    }

    /// A standalone run of `count` refs (the first block of a key). Small under grouped scatter when the
    /// refs fit 64 bytes; exact otherwise.
    UInt64 * allocStandaloneBlock(size_t count)
    {
        if (grouped && count <= SMALL_BLOCK_REFS)
        {
            ++st.small_blocks;
            st.slack_slots += SMALL_BLOCK_REFS - count;
            return allocWords(SMALL_BLOCK_REFS);
        }
        return allocWords(count);
    }

    size_t standaloneCapacity(size_t count) const { return grouped && count <= SMALL_BLOCK_REFS ? SMALL_BLOCK_REFS : count; }

    /// The key's first block, from a prefix of refs already held (an inline ref or a pair's two) plus
    /// the new ones. Above `COUNT_SAT` the word cannot carry the count, so the run gets a descriptor
    /// right away and stays the list's only node until something chains behind it.
    void startRun(RowRefList & mapped, const UInt64 * prefix, size_t prefix_count, const UInt64 * refs, size_t count)
    {
        const size_t total = prefix_count + count;
        chassert(total >= 2);

        if (total == 2)
        {
            UInt64 * pair = allocPair();
            if (prefix_count == 2)
            {
                pair[0] = prefix[0];
                pair[1] = prefix[1];
            }
            else if (prefix_count == 1)
            {
                pair[0] = prefix[0];
                pair[1] = refs[0];
            }
            else
            {
                pair[0] = refs[0];
                pair[1] = refs[1];
            }
            mapped = RowRefList::makeRun(pair, 2);
            return;
        }

        ++st.runs;
        UInt64 * block = total < RowRefList::COUNT_SAT ? allocStandaloneBlock(total) : allocWords(total);
        if (prefix_count)
            memcpy(block, prefix, prefix_count * sizeof(UInt64));
        memcpy(block + prefix_count, refs, count * sizeof(UInt64));

        if (total < RowRefList::COUNT_SAT)
        {
            mapped = RowRefList::makeRun(block, total);
            return;
        }

        auto * descriptor = allocDescriptor();
        descriptor->first = block;
        descriptor->tail_link = nullptr;
        descriptor->total = static_cast<UInt32>(total);
        descriptor->first_count = static_cast<UInt32>(total);
        mapped = RowRefList::makeList(descriptor);
    }

    RowRefList::RunListDescriptor * allocDescriptor()
    {
        ++st.descriptors;
        st.arena_bytes += DESCRIPTOR_BYTES;
        return reinterpret_cast<RowRefList::RunListDescriptor *>(arena.alignedAlloc(DESCRIPTOR_BYTES, alignof(RowRefList::RunListDescriptor)));
    }

    /** The first block of a key gains a successor: its last slot becomes the trailing link. A block with
      * room left first takes as many new refs as fit before that slot; a full block hands its last ref to
      * the new node instead. Either way the refs stay in insertion order and at most one ref moves.
      */
    void chainFirstRun(RowRefList & mapped, UInt64 * block, size_t have, size_t capacity, const UInt64 * refs, size_t count)
    {
        const size_t total = have + count;
        const size_t link_slot = capacity - 1;
        if (grouped && capacity == SMALL_BLOCK_REFS)
            st.slack_slots -= capacity - have;

        UInt64 moved = 0;
        size_t moved_count = 0;
        size_t kept = have;
        if (have <= link_slot)
        {
            const size_t fill = std::min(count, link_slot - have);
            if (fill)
            {
                memcpy(block + have, refs, fill * sizeof(UInt64));
                refs += fill;
                count -= fill;
                kept += fill;
                ++st.in_place_fills;
            }
        }
        else
        {
            chassert(have == capacity);
            moved = block[link_slot];
            moved_count = 1;
            kept = link_slot;
            ++st.moved_refs;
        }
        chassert(count + moved_count > 0);

        auto * descriptor = allocDescriptor();
        descriptor->first = block;
        descriptor->tail_link = nullptr;
        descriptor->first_count = static_cast<UInt32>(kept);
        descriptor->total = static_cast<UInt32>(total);

        UInt64 * node = appendNodes(*descriptor, moved_count ? &moved : nullptr, moved_count, refs, count);
        block[link_slot] = RowRefList::makeLink(node, 0, false);
        mapped = RowRefList::makeList(descriptor);
    }

    void appendToList(RowRefList & mapped, const UInt64 * refs, size_t count)
    {
        auto * descriptor = mapped.listDescriptorMutable();
        const size_t total = static_cast<size_t>(descriptor->total) + count;
        if (total > std::numeric_limits<UInt32>::max()) [[unlikely]]
            throwRowRefOutOfRange(0, total);

        if (!descriptor->tail_link)
        {
            /// The first run is still the only node (a key above COUNT_SAT rows in one pass); its last
            /// slot becomes the trailing link and its last ref moves into the new node.
            auto * first = const_cast<UInt64 *>(descriptor->first);
            const size_t link_slot = descriptor->first_count - 1;
            const UInt64 moved = first[link_slot];
            descriptor->first_count = static_cast<UInt32>(link_slot);
            ++st.moved_refs;
            UInt64 * node = appendNodes(*descriptor, &moved, 1, refs, count);
            first[link_slot] = RowRefList::makeLink(node, 0, false);
        }
        else
        {
            UInt64 * tail = descriptor->tail_link;
            const UInt64 link = *tail;
            const size_t have = RowRefList::linkCount(link);
            const bool small = RowRefList::linkIsSmall(link);
            const size_t free = small ? SMALL_NODE_REFS - have : 0;
            const size_t fill = std::min(count, free);
            if (fill)
            {
                memcpy(tail + 1 + have, refs, fill * sizeof(UInt64));
                *tail = RowRefList::makeLink(nullptr, static_cast<UInt32>(have + fill), small);
                st.slack_slots -= fill;
                ++st.in_place_fills;
                refs += fill;
                count -= fill;
            }
            if (count)
                appendNodes(*descriptor, nullptr, 0, refs, count);
        }

        descriptor->total = static_cast<UInt32>(total);
        mapped = RowRefList::makeList(descriptor);
    }

    /** Appends one or more nodes holding `prefix` then `refs`, in order, each behind a leading link word
      * and each within `LINK_MAX_COUNT` refs. Links them from the current tail when there is one and
      * makes the last of them the tail; returns the first node created so a caller that chains a first
      * run can write its trailing link.
      */
    UInt64 * appendNodes(RowRefList::RunListDescriptor & descriptor, const UInt64 * prefix, size_t prefix_count, const UInt64 * refs, size_t count)
    {
        UInt64 * first_created = nullptr;
        size_t remaining = prefix_count + count;
        chassert(remaining > 0);
        while (remaining)
        {
            const size_t n = std::min<size_t>(remaining, RowRefList::LINK_MAX_COUNT);
            const bool small = grouped && n < SMALL_BLOCK_REFS;
            UInt64 * node = small ? allocWords(SMALL_BLOCK_REFS) : allocWords(1 + n);
            if (small)
            {
                ++st.small_blocks;
                st.slack_slots += SMALL_NODE_REFS - n;
            }
            ++st.appended_nodes;
            node[0] = RowRefList::makeLink(nullptr, static_cast<UInt32>(n), small);

            UInt64 * out = node + 1;
            size_t to_write = n;
            if (prefix_count)
            {
                const size_t take = std::min(prefix_count, to_write);
                memcpy(out, prefix, take * sizeof(UInt64));
                prefix += take;
                prefix_count -= take;
                out += take;
                to_write -= take;
            }
            if (to_write)
            {
                memcpy(out, refs, to_write * sizeof(UInt64));
                refs += to_write;
                count -= to_write;
            }
            remaining -= n;

            if (descriptor.tail_link)
            {
                const UInt64 tail_link = *descriptor.tail_link;
                *descriptor.tail_link = RowRefList::makeLink(node, RowRefList::linkCount(tail_link), RowRefList::linkIsSmall(tail_link));
            }
            else
                first_created = node;
            descriptor.tail_link = node;
        }
        return first_created;
    }
};

}
