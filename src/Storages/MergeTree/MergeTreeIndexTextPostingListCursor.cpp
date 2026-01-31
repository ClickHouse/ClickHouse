#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace DB
{

void PostingListCursor::addSegment(size_t s)
{
    auto it = std::find(segments.begin(), segments.end(), s);
    if (it == segments.end())
    {
        segments.push_back(s);
        is_valid = true;
    }
}

void PostingListCursor::prepare(size_t segment)
{
    if (current_segment == segment && current_segment != std::numeric_limits<size_t>::max())
        return;

    current_values.reserve(POSTING_LIST_UNIT_SIZE);
    if (info.embedded_postings)
    {
        chassert(!stream);
        current_values.resize(info.embedded_postings->cardinality());
        info.embedded_postings->toUint32Array(current_values.data());
        current_block = 0;
        block_count = 1;
        current_segment = segment;
        is_valid = true;
        is_embedded = true;
        density_val = static_cast<double>(header.cardinality) / static_cast<double>(current_values.back() - current_values.front());
        return;
    }


    chassert(segment < info.offsets.size());
    size_t offset_in_file = info.offsets[segment];
    stream->seekToMark({offset_in_file, 0});
    auto & in = *(stream->getDataBuffer());
    header.read(in);

    chassert(header.has_block_skip_index);
    CodecUtil::readArrayU32(in, block_row_ends);
    CodecUtil::readArrayU32(in, block_offsets);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);

    in.readStrict(compressed_data.data(), header.payload_bytes);
    size_t full_block_count = header.cardinality / POSTING_LIST_UNIT_SIZE;
    tail_size = header.cardinality % POSTING_LIST_UNIT_SIZE;
    block_count = full_block_count + (tail_size > 0 ? 1 : 0);
    current_block = 0;
    density_val = static_cast<double>(header.cardinality) / static_cast<double>(block_row_ends.back() - header.first_row_id);
    current_segment = segment;

}

void PostingListCursor::seek(uint32_t target)
{
    if (!seekImpl(target))
    {
        int unused_segment_index = -1;
        bool found = false;
        for (size_t i = 0; i < segments.size(); ++i)
        {
            auto segment = segments[i];
            if (segment < current_segment)
            {
                unused_segment_index = static_cast<int>(i);
                continue;
            }
            const auto &range = info.ranges[segment];
            if (range.end > target)
            {
                prepare(segment);
                if (seekImpl(target))
                {
                    found = true;
                    break;
                }
            }
        }

        if (unused_segment_index > 0)
            maybeEraseUnusedSegments(unused_segment_index);
        is_valid = found;
    }
}

bool PostingListCursor::seekImpl(uint32_t target)
{
    if (index < current_values.size())
    {
        auto it = std::lower_bound(current_values.begin(),  current_values.end(), target);
        if (it != current_values.end() && *it >= target)
        {
            index = static_cast<size_t>(it - current_values.begin());
            return true;
        }
        if (is_embedded)
            return false;
    }
    size_t block_start = 0;
    size_t block_end = block_count;
    size_t offset = 0;

    const auto &row_ends = block_row_ends;
    const auto &offsets = block_offsets;

    auto it = std::lower_bound(row_ends.begin(), row_ends.end(), target);
    if (it == row_ends.end())
        return false;
    size_t skip_block_begin = static_cast<size_t>(it - row_ends.begin());

    block_start = skip_block_begin;
    offset = offsets[block_start];
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()), compressed_data.size());
    compressed_data_span = compressed_data_span.subspan(offset);

    for (size_t i = block_start; i < block_end; i++)
    {
        size_t block_size = (i + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_UNIT_SIZE;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        auto values_it = std::lower_bound(current_values.begin(),  current_values.begin() + block_size, target);
        if (values_it != current_values.begin() + block_size && *values_it >= target)
        {
            index = static_cast<size_t>(values_it - current_values.begin());
            current_block = i;
            return true;
        }
    }
    return false;
}

void PostingListCursor::next()
{
    if (!is_valid)
        return;

    ++index;

    if (index >= current_values.size())
    {
        ++current_block;
        if (current_block == block_count)
        {
            is_valid = false;
            return;
        }
        const auto & offsets = block_offsets;
        size_t offset = offsets[current_block];
        std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()),compressed_data.size());
        compressed_data_span = compressed_data_span.subspan(offset);

        size_t block_size = (current_block + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_UNIT_SIZE;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        index = 0;
    }
}

inline void padColumnForOr(UInt8 * __restrict out, const std::vector<uint32_t> & current_values, size_t row_begin, size_t begin, size_t length)
{
    const uint32_t * data = current_values.data();
    const uint32_t * data_begin = data + begin;
    const uint32_t * data_end = data + length;

    if (data_begin >= data_end)
        return;

    const uint32_t * p = data_begin;
    const size_t count = data_end - data_begin;

    const uint32_t * loop_end = data_begin + (count / 4) * 4;

    for (; p < loop_end; p += 4)
    {
        __builtin_prefetch(p + 16, 0, 3);
        if (p + 4 < data_end)
            __builtin_prefetch(&out[p[4] - row_begin], 1, 0);

        out[p[0] - row_begin] = 1;
        out[p[1] - row_begin] = 1;
        out[p[2] - row_begin] = 1;
        out[p[3] - row_begin] = 1;
    }

    switch (data_end - p)
    {
        case 3: out[p[2] - row_begin] = 1; [[fallthrough]];
        case 2: out[p[1] - row_begin] = 1; [[fallthrough]];
        case 1: out[p[0] - row_begin] = 1; [[fallthrough]];
        default: break;
    }
}

void PostingListCursor::linearOrImpl(size_t segment, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    chassert(segment < info.ranges.size());

    if (unlikely(is_embedded))
    {
        auto it = std::lower_bound(current_values.begin(), current_values.end(), row_begin);
        if (it == current_values.end())
            return;
        size_t idx = static_cast<size_t>(it - current_values.begin());
        auto it_end = std::upper_bound(current_values.begin(), current_values.end(), row_end);
        size_t length = it_end - current_values.begin();
        padColumnForOr(out, current_values, row_begin, idx, length);
        return;
    }

    size_t block_start = 0;
    size_t block_end = block_count;
    size_t offset = 0;

    const auto &row_ends = block_row_ends;
    const auto &offsets = block_offsets;
    size_t skip_block_begin = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_begin) - row_ends.begin());
    if (skip_block_begin >= block_count)
        return;
    size_t skip_block_end = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_end) - row_ends.begin());

    block_start = skip_block_begin;
    block_end = std::min(skip_block_end + 1, block_count);
    offset = offsets[block_start];

    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()),compressed_data.size());
    compressed_data_span = compressed_data_span.subspan(offset);

    for (size_t i = block_start; i < block_end; i++)
    {
        size_t block_size = (i + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_UNIT_SIZE;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        auto it = std::lower_bound(current_values.begin(), current_values.end(), row_begin);
        if (it == current_values.end())
            continue;
        size_t idx = static_cast<size_t>(it - current_values.begin());
        auto it_end = std::upper_bound(current_values.begin(), current_values.end(), row_end);
        size_t length = it_end - current_values.begin();

        padColumnForOr(out, current_values, row_begin, idx, length);

        if (length < block_size)
            return;
    }
}

void PostingListCursor::linearOr(UInt8 * data, size_t row_offset, size_t num_rows)
{
    int unused_segment_index = -1;
    for (size_t i = 0; i < segments.size(); ++i)
    {
        auto segment = segments[i];
        size_t begin = info.ranges[segment].begin;
        size_t end = info.ranges[segment].end;

        if (row_offset > end || (row_offset + num_rows) < begin)
        {
            unused_segment_index = static_cast<int>(i);
            continue;
        }
        end = std::min(end, row_offset + num_rows - 1);
        prepare(segment);
        linearOrImpl(segment, data, row_offset, end);
    }
    if (unused_segment_index > 0)
        maybeEraseUnusedSegments(unused_segment_index);
}

inline void padColumnForAnd(UInt8 * __restrict out, const std::vector<uint32_t> & current_values, size_t row_begin, size_t begin, size_t length)
{
    const uint32_t *p = current_values.data() + begin;
    const uint32_t *end = current_values.data() + length;

    for (; p + 4 <= end; p += 4)
    {
        __builtin_prefetch(p + 16, 0, 3);
        if (p + 8 < end)
            __builtin_prefetch(&out[p[8] - row_begin], 1, 0);

        ++out[p[0] - row_begin];
        ++out[p[1] - row_begin];
        ++out[p[2] - row_begin];
        ++out[p[3] - row_begin];
    }

    switch (end - p)
    {
        case 3: ++out[p[2] - row_begin];
            [[fallthrough]];
        case 2: ++out[p[1] - row_begin];
            [[fallthrough]];
        case 1: ++out[p[0] - row_begin];
            [[fallthrough]];
        default: break;
    }
}

void PostingListCursor::linearAndImpl(size_t segment, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    chassert(segment < info.ranges.size());

    if (unlikely(is_embedded))
    {
        auto it = std::lower_bound(current_values.begin(), current_values.end(), row_begin);
        if (it == current_values.end())
            return;
        size_t idx = static_cast<size_t>(it - current_values.begin());
        auto it_end = std::upper_bound(current_values.begin(), current_values.end(), row_end);
        size_t length = it_end - current_values.begin();
        padColumnForAnd(out, current_values, row_begin, idx, length);
        return;
    }

    size_t block_start = 0;
    size_t block_end = block_count;
    size_t offset = 0;

    const auto & row_ends = block_row_ends;
    const auto & offsets = block_offsets;

    size_t skip_block_begin = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_begin) - row_ends.begin());
    if (skip_block_begin >= block_count)
        return;
    size_t skip_block_end = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_end) - row_ends.begin());

    block_start = skip_block_begin;
    block_end = std::min(skip_block_end + 1, block_count);
    offset = offsets[block_start];

    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()),compressed_data.size());
    compressed_data_span = compressed_data_span.subspan(offset);

    for (size_t i = block_start; i < block_end; i++)
    {
        size_t block_size = (i + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_UNIT_SIZE;
        current_block = i;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        auto it = std::lower_bound(current_values.begin(), current_values.end(), row_begin);
        if (it == current_values.end())
            continue;
        size_t idx = static_cast<size_t>(it - current_values.begin());
        auto it_end = std::upper_bound(current_values.begin(), current_values.end(), row_end);
        size_t length = it_end - current_values.begin();

        padColumnForAnd(out, current_values, row_begin, idx, length);

        if (length < block_size)
            return;
    }
}

void PostingListCursor::linearAnd(UInt8 * data, size_t row_offset, size_t num_rows)
{
    int unused_segment_index = -1;
    for (size_t i = 0; i < segments.size(); ++i)
    {
        auto segment = segments[i];
        size_t begin = info.ranges[segment].begin;
        size_t end = info.ranges[segment].end;

        if (row_offset > end || (row_offset + num_rows) < begin)
        {
            unused_segment_index = static_cast<int>(i);
            continue;
        }
        end = std::min(end, row_offset + num_rows - 1);
        prepare(segments[i]);
        linearAndImpl(segment, data, row_offset, end);
    }

    if (unused_segment_index > 0)
        maybeEraseUnusedSegments(unused_segment_index);
}

namespace
{

/// Helper struct for min-heap based intersection algorithm.
struct HeapItem
{
    uint32_t val = 0;
    uint32_t idx = 0;

    HeapItem() = default;
    HeapItem(uint32_t val_, uint32_t idx_) : val(val_), idx(idx_) {}
    bool operator>(const HeapItem & other) const { return val > other.val; }
};

/// Two-way merge intersection using skip-list style seek.
/// Optimized for the common case of 2 posting lists.
/// Time complexity: O(min(|L1|, |L2|) * log(max(|L1|, |L2|)))
void intersectTwo(UInt8 * out, PostingListCursorPtr c0, PostingListCursorPtr c1, size_t row_offset, size_t effective_end)
{
    while (c0->valid() && c1->valid())
    {
        uint32_t v0 = c0->value();
        uint32_t v1 = c1->value();
        if (v0 >= effective_end || v1 >= effective_end)
            return;

        if (v0 == v1)
        {
            out[v0 - row_offset] = 1;
            c0->next();
            c1->next();
        }
        else if (v0 < v1)
        {
            c0->seek(v1);
        }
        else
        {
            c1->seek(v0);
        }
    }
}

/// Three-way merge intersection.
/// All cursors seek to max value when they differ.
void intersectThree(UInt8 * out, PostingListCursorPtr c0, PostingListCursorPtr c1, PostingListCursorPtr c2, size_t row_offset, size_t effective_end)
{
    uint32_t v0 = 0;
    uint32_t v1 = 0;
    uint32_t v2 = 0;
    while (c0->valid() && c1->valid() && c2->valid())
    {
        v0 = c0->value();
        v1 = c1->value();
        v2 = c2->value();

        uint32_t max_val = std::max({v0, v1, v2});
        if (max_val >= effective_end)
            return;

        if (v0 == v1 && v1 == v2)
        {
            out[v0 - row_offset] = 1;

            c0->next();
            c1->next();
            c2->next();

        }
        else
        {
            if (v0 < max_val)
                c0->seek(max_val);
            if (v1 < max_val)
                c1->seek(max_val);
            if (v2 < max_val)
                c2->seek(max_val);
        }
    }
}

/// Four-way merge intersection.
/// Optimized unrolled version for exactly 4 posting lists.
void intersectFour(UInt8 * out, PostingListCursorPtr c0, PostingListCursorPtr c1, PostingListCursorPtr c2, PostingListCursorPtr c3, size_t row_offset, size_t effective_end)
{
    uint32_t v0 = 0;
    uint32_t v1 = 0;
    uint32_t v2 = 0;
    uint32_t v3 = 0;
    while (c0->valid() && c1->valid() && c2->valid() && c3->valid())
    {
        v0 = c0->value();
        v1 = c1->value();
        v2 = c2->value();
        v3 = c3->value();

        uint32_t max_val = std::max({v0, v1, v2, v3});
        if (max_val >= effective_end)
            return;

        if (v0 == v1 && v1 == v2 && v2 == v3)
        {
            out[v0 - row_offset] = 1;

            c0->next();
            c1->next();
            c2->next();
            c3->next();
        }
        else
        {
            if (v0 < max_val)
                c0->seek(max_val);
            if (v1 < max_val)
                c1->seek(max_val);
            if (v2 < max_val)
                c2->seek(max_val);
            if (v3 < max_val)
                c3->seek(max_val);
        }
    }
}

/// Leapfrog intersection with linear min/max scan.
/// Used for 5-8 posting lists where heap overhead isn't worth it.
/// Algorithm: find min/max in O(n), seek min cursor to max value.
void intersectLeapfrogLinear(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_end)
{
    const size_t n = cursors.size();
    std::vector<uint32_t> vals(n);
    for (size_t i = 0; i < n; ++i)
    {
        vals[i] = cursors[i]->value();
    }

    while (true)
    {
        /// Find min and max values across all cursors
        uint32_t min_val = vals[0];
        uint32_t max_val = vals[0];
        size_t min_idx = 0;

        for (size_t i = 1; i < n; ++i)
        {
            if (vals[i] < min_val)
            {
                min_val = vals[i];
                min_idx = i;
            }
            else if (vals[i] > max_val)
            {
                max_val = vals[i];
            }
        }

        if (max_val >= effective_end)
            return;

        if (min_val == max_val)
        {
            // All cursors point to same value => intersection found
            out[min_val - row_offset] = 1;
            for (size_t i = 0; i < n; ++i)
            {
                cursors[i]->next();
                if (!cursors[i]->valid())
                    return;
                vals[i] = cursors[i]->value();
            }
        }
        else
        {
            /// Advance the cursor with minimum value to catch up
            cursors[min_idx]->seek(max_val);
            if (!cursors[min_idx]->valid())
                return;
            vals[min_idx] = cursors[min_idx]->value();
        }
    }
}

/// Leapfrog intersection using min-heap for efficient min finding.
/// Used for 9+ posting lists where O(n) linear scan becomes expensive.
/// Min-heap gives O(log n) for finding and updating minimum.
void intersectLeapfrogHeap(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_end)
{
    const size_t n = cursors.size();

    /// Build min-heap and track global maximum
    std::vector<HeapItem> heap(n);
    uint32_t max_val = 0;

    for (size_t i = 0; i < n; ++i)
    {
        uint32_t val = cursors[i]->value();
        heap[i] = {val, static_cast<uint32_t>(i)};
        max_val = std::max(max_val, val);
    }
    std::make_heap(heap.begin(), heap.end(), std::greater<>{});

    while (true)
    {
        if (max_val >= effective_end)
            return;

        uint32_t min_val = heap.front().val;

        if (min_val == max_val)
        {
            /// All cursors at same position => intersection found
            out[min_val - row_offset] = 1;

            /// Advance all cursors and rebuild heap
            max_val = 0;
            size_t heap_size = n;

            for (size_t i = 0; i < heap_size; ++i)
            {
                uint32_t idx = heap[i].idx;
                cursors[idx]->next();

                if (!cursors[idx]->valid())
                    return;

                uint32_t val = cursors[idx]->value();
                if (val >= effective_end)
                    return;

                heap[i].val = val;
                max_val = std::max(max_val, val);
            }
            std::make_heap(heap.begin(), heap.end(), std::greater<>{});
        }
        else
        {
            /// Pop min cursor, seek it to max_val, push back
            uint32_t min_idx = heap.front().idx;
            std::pop_heap(heap.begin(), heap.end(), std::greater<>{});

            cursors[min_idx]->seek(max_val);
            if (!cursors[min_idx]->valid())
                return;

            uint32_t new_val = cursors[min_idx]->value();
            if (new_val >= effective_end)
                return;

            max_val = std::max(max_val, new_val);
            heap.back() = {new_val, min_idx};
            std::push_heap(heap.begin(), heap.end(), std::greater<>{});
        }
    }
}

/// Dispatcher for skip-list based intersection algorithms.
/// Selects optimal algorithm based on number of posting lists:
///   - 2 lists: two-pointer merge
///   - 3 lists: three-way merge
///   - 4 lists: four-way merge
///   - 5-8 lists: linear leapfrog
///   - 9+ lists: heap-based leapfrog
void intersectLeapfrog(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_end)
{
    if (cursors.size() == 2)
        return intersectTwo(out, cursors[0], cursors[1], row_offset, effective_end);

    if (cursors.size() == 3)
        return intersectThree(out, cursors[0], cursors[1], cursors[2], row_offset, effective_end);

    if (cursors.size() == 4)
        return intersectFour(out, cursors[0], cursors[1], cursors[2], cursors[3], row_offset, effective_end);

    if (cursors.size() <= 8)
        return intersectLeapfrogLinear(out, cursors, row_offset, effective_end);

    return intersectLeapfrogHeap(out, cursors, row_offset, effective_end);
}

/// Brute-force intersection using bitmap counting.
/// Algorithm:
///   1. First cursor sets bits via linearOr (marks candidates)
///   2. Remaining cursors increment via linearAnd
///   3. Final pass: keep only positions where count == n (all lists matched)
///
/// Preferred when posting lists are dense (high density), as it avoids
/// the random-access overhead of skip-list based algorithms.
void intersectBruteForce(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t num_rows)
{
    /// Step 1: First cursor marks initial candidates
    cursors[0]->linearOr(out, row_offset, num_rows);

    /// Step 2: Remaining cursors increment counts
    for (size_t i = 1; i < cursors.size(); ++i)
        cursors[i]->linearAnd(out, row_offset, num_rows);

    /// Step 3: Finalize - keep only positions where all n cursors matched
    size_t n = cursors.size();
    if (n > 1)
    {
        UInt8 * p = out;
        UInt8 * end = out + num_rows;
        UInt8 * end_loop = out + (num_rows / 4) * 4;
        UInt8 n8 = static_cast<UInt8>(n);

        /// Unrolled loop with prefetch for better performance
        for (; p < end_loop; p += 4)
        {
            __builtin_prefetch(p + 64, 0, 3);
            __builtin_prefetch(p + 64, 1, 0);

            p[0] = (p[0] == n8);
            p[1] = (p[1] == n8);
            p[2] = (p[2] == n8);
            p[3] = (p[3] == n8);
        }

        /// Handle remainder
        while (p < end)
        {
            *p = (*p == n8);
            ++p;
        }
    }
}

} // anonymous namespace

/// Union (OR) of multiple posting lists for TextSearchMode::Any.
/// Iterates through each cursor's posting list and sets output bits for all matching row IDs.
/// Parameters brute_force_apply and density_threshold are unused - union always uses
/// linear scan since skip-list optimization provides no benefit for OR operations.
void lazyUnionPostingLists(IColumn & column, const PostingListCursorMap & postings, const std::vector<String> & search_tokens, size_t column_offset, size_t row_offset, size_t num_rows, bool /*brute_force_apply*/, float /*density_threshold*/)
{
    auto & data = assert_cast<DB::ColumnUInt8 &>(column).getData();
    UInt8 * out = data.data() + column_offset;

    std::vector<PostingListCursorPtr> cursors;
    cursors.reserve(postings.size());
    for (const auto & token : search_tokens)
    {
        auto it = postings.find(token);
        if (it != postings.end())
            cursors.emplace_back(it->second);
    }
    for (const auto & cursor : cursors)
        cursor->linearOr(out, row_offset, num_rows);
}

/// Intersection (AND) of multiple posting lists for TextSearchMode::All.
///
/// Uses adaptive algorithm selection based on posting list density:
///   1. Single list (n=1): direct linear scan (same as union)
///   2. Dense lists (avg density >= threshold) or brute_force_apply=true:
///      brute-force bitmap counting with sequential memory access
///   3. Sparse lists: skip-list based leapfrog intersection with lazy block decoding
///
/// Performance characteristics:
///   - Sparse lists benefit from skip-list (fewer elements to decode and process)
///   - Dense lists benefit from brute-force (better cache locality, no branch misprediction)
void lazyIntersectPostingLists(IColumn & column, const PostingListCursorMap & postings, const std::vector<String> & search_tokens, size_t column_offset, size_t row_offset, size_t num_rows, bool brute_force_apply, float density_threshold)
{
    auto & data = assert_cast<DB::ColumnUInt8 &>(column).getData();
    UInt8 * __restrict out = data.data() + column_offset;

    std::vector<PostingListCursorPtr> cursors;
    cursors.reserve(postings.size());
    for (const auto & token : search_tokens)
    {
        auto it = postings.find(token);
        if (it != postings.end())
            cursors.emplace_back(it->second);
    }
    const size_t n = cursors.size();
    const size_t end = row_offset + num_rows;

    if (n == 0)
        return;

    /// Edge case: single cursor - just mark its row IDs
    if (n == 1)
    {
        cursors.front()->linearOr(out, row_offset, num_rows);
        return;
    }

    /// Compute average density across all posting lists
    double density = 0;
    for (size_t i = 0; i < n; ++i)
        density += cursors[i]->density();
    density = density / n;

    /// Use brute-force when:
    ///   - Lists are dense (density >= threshold), OR
    ///   - User explicitly requested brute-force
    /// Note: n < 256 check ensures we don't overflow UInt8 counter in brute-force
    if (n < 256 && (density >= density_threshold || brute_force_apply))
        return intersectBruteForce(out, cursors, row_offset, num_rows);

    /// Skip-list based intersection: position all cursors to start of range
    for (size_t i = 0; i < n; ++i)
    {
        cursors[i]->seek(static_cast<uint32_t>(row_offset));
        /// Early exit if any cursor is exhausted or past our range
        if (!cursors[i]->valid() || cursors[i]->value() >= end)
            return;
    }

    return intersectLeapfrog(out, cursors, row_offset, end);
}

}
