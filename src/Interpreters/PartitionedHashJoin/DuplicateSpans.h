#pragma once

#include <Interpreters/RowRefs.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>

#include <limits>

namespace DB
{

/** Per-pass scratch for the duplicate rows of a `PartitionedHashJoin` build. A key's first duplicate
  * of the pass stores the cell's previous word (an inline ref, or a run/chain from an earlier pass)
  * as the key's first item, then every later row of the key appends its ref. The zero key has no
  * bucket: its items live in `zero_items`. At the pass's finish, `SpanWriter` turns each key's items
  * into one exact arena span.
  */
struct PassScratch
{
    PaddedPODArray<UInt32> bucket; /// per item: the cell's index in the buffer
    PaddedPODArray<UInt64> item; /// per item: a ref word, or the key's previous published word
    PaddedPODArray<UInt32> keys; /// per duplicated key of the pass: its bucket, first-duplicate order
    PaddedPODArray<UInt64> zero_items; /// the zero key's items of the pass; it has no bucket
    UInt64 spanning_keys = 0; /// keys whose previous word was a run or a chain

    bool empty() const { return keys.empty() && zero_items.empty(); }

    /// Logical occupancy: 12 bytes per item (`bucket` + `item`) plus 4 bytes per duplicated key,
    /// plus 8 bytes per zero-key item. Capacity slack is not included; the gate charges this shape.
    size_t usedBytes() const
    {
        return bucket.size() * sizeof(UInt32) + item.size() * sizeof(UInt64) + keys.size() * sizeof(UInt32)
            + zero_items.size() * sizeof(UInt64);
    }

    size_t allocatedBytes() const
    {
        return bucket.allocated_bytes() + item.allocated_bytes() + keys.allocated_bytes() + zero_items.allocated_bytes();
    }

    void clear()
    {
        bucket.clear();
        item.clear();
        keys.clear();
        zero_items.clear();
        spanning_keys = 0;
    }
};

ALWAYS_INLINE inline void appendRow(RowRefList & mapped, UInt64 ref, UInt32 bucket, PassScratch & scratch)
{
    if (mapped.isCount())
        mapped.addItem();
    else
    {
        const bool has_prev = !mapped.isInline();
        scratch.bucket.push_back(bucket);
        scratch.item.push_back(mapped.word);
        scratch.keys.push_back(bucket);
        scratch.spanning_keys += has_prev;
        mapped = RowRefList::makeCount(/*n_items=*/2, has_prev);
    }
    scratch.bucket.push_back(bucket);
    scratch.item.push_back(ref);
}

ALWAYS_INLINE inline void appendRowZero(RowRefList & mapped, UInt64 ref, PassScratch & scratch)
{
    if (mapped.isCount())
        mapped.addItem();
    else
    {
        scratch.zero_items.push_back(mapped.word);
        scratch.spanning_keys += !mapped.isInline();
        mapped = RowRefList::makeCount(/*n_items=*/2, !mapped.isInline());
    }
    scratch.zero_items.push_back(ref);
}

/** Turns one pass's scratch into exact arena spans: a headerless `TAG_RUN` for a key's first range,
  * a 16-byte header in front of every later range, newest range first. One writer per build worker
  * (and one for the drain), allocating from that worker's arena. A key is only ever appended to by
  * the owner of its partition or by the serial drain, so nothing here synchronizes.
  */
class SpanWriter
{
public:
    struct Stats
    {
        UInt64 ranges = 0;
        UInt64 headers = 0;
        UInt64 spanning_keys = 0;
        UInt64 ref_words = 0;
        UInt64 arena_bytes = 0;

        Stats & operator+=(const Stats & other)
        {
            ranges += other.ranges;
            headers += other.headers;
            spanning_keys += other.spanning_keys;
            ref_words += other.ref_words;
            arena_bytes += other.arena_bytes;
            return *this;
        }
    };

    explicit SpanWriter(Arena & arena_)
        : arena(arena_)
    {
    }

    const Stats & stats() const { return st; }

    template <typename MappedAt>
    void finish(PassScratch & scratch, MappedAt && mapped_at, RowRefList * zero_mapped = nullptr)
    {
        if (scratch.empty())
            return;

        st.spanning_keys += scratch.spanning_keys;

        for (const auto bucket : scratch.keys)
            openKey(mapped_at(bucket), arena, st);
        if (!scratch.zero_items.empty())
        {
            chassert(zero_mapped);
            openKey(*zero_mapped, arena, st);
        }

        const size_t n = scratch.item.size();
        for (size_t i = 0; i < n; ++i)
        {
            if (i + 16 < n)
                __builtin_prefetch(&mapped_at(scratch.bucket[i + 16]), 1, 3);
            placeItem(mapped_at(scratch.bucket[i]), scratch.item[i]);
        }
        for (const auto it : scratch.zero_items)
            placeItem(*zero_mapped, it);

        for (const auto bucket : scratch.keys)
            closeKey(mapped_at(bucket));
        if (!scratch.zero_items.empty())
            closeKey(*zero_mapped);

        scratch.clear();
    }

private:
    Arena & arena;
    Stats st;

    static void openKey(RowRefList & mapped, Arena & arena, Stats & st)
    {
        chassert(mapped.isCount());
        const UInt64 n_items = mapped.countItems();
        const bool has_prev = mapped.hasPrev();
        const UInt64 n_new = n_items - has_prev;
        const UInt64 chunks = (n_new + RowRefList::MAX_RANGE_REFS - 1) / RowRefList::MAX_RANGE_REFS;
        const UInt64 headers = chunks - (has_prev ? 0 : 1);
        const UInt64 words = n_new + 2 * headers;
        auto * span = reinterpret_cast<UInt64 *>(arena.alignedAlloc(words * sizeof(UInt64), alignof(UInt64)));
        st.arena_bytes += words * sizeof(UInt64);
        st.ranges += chunks;
        st.headers += headers;
        st.ref_words += n_new;
        mapped = RowRefList::makeFill(span, /*placed=*/0, /*has_header=*/false);
    }

    static void placeItem(RowRefList & mapped, UInt64 it)
    {
        chassert(mapped.isFill());
        UInt64 * cur = mapped.fillCursor();
        UInt32 placed = mapped.fillPlaced();
        bool hdr = mapped.fillHasHeader();
        if (!refWordIsInline(it))
        {
            cur[0] = RowRefList::fromWord(it).rows();
            cur[1] = RowRefList::makePrevWordFrom(it);
            cur += 2;
            placed = 0;
            hdr = true;
        }
        else
        {
            if (placed == RowRefList::MAX_RANGE_REFS)
            {
                UInt64 * chunk = cur - RowRefList::MAX_RANGE_REFS;
                UInt64 total = 0;
                if (hdr)
                {
                    UInt64 * header = chunk - 2;
                    total = (header[0] & RowRefList::PTR_MASK) + RowRefList::MAX_RANGE_REFS;
                    if (total > std::numeric_limits<UInt32>::max()) [[unlikely]]
                        throwRowRefOutOfRange(0, static_cast<size_t>(total));
                    header[0] = total | (static_cast<UInt64>(RowRefList::MAX_RANGE_REFS) << RowRefList::COUNT_SHIFT);
                }
                else
                    total = RowRefList::MAX_RANGE_REFS;
                cur[0] = total;
                cur[1] = RowRefList::makePrevWord(
                    hdr ? static_cast<const void *>(chunk - 2) : static_cast<const void *>(chunk), RowRefList::MAX_RANGE_REFS, hdr);
                cur += 2;
                placed = 0;
                hdr = true;
            }
            *cur++ = it;
            ++placed;
        }
        mapped = RowRefList::makeFill(cur, placed, hdr);
    }

    static void closeKey(RowRefList & mapped)
    {
        UInt64 * cur = mapped.fillCursor();
        const UInt32 placed = mapped.fillPlaced();
        UInt64 * start = cur - placed;
        if (!mapped.fillHasHeader())
            mapped = RowRefList::makeRun(start, placed);
        else
        {
            UInt64 * header = start - 2;
            const UInt64 total = (header[0] & RowRefList::PTR_MASK) + placed;
            if (total > std::numeric_limits<UInt32>::max()) [[unlikely]]
                throwRowRefOutOfRange(0, static_cast<size_t>(total));
            header[0] = total | (static_cast<UInt64>(placed) << RowRefList::COUNT_SHIFT);
            mapped = RowRefList::makeChain(reinterpret_cast<RowRefList::RangeHeader *>(header), total);
        }
    }
};

}
