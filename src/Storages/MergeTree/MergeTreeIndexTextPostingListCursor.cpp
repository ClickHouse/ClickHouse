#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
    extern const Event TextIndexPostingsDecodeTotalMicroseconds;
    extern const Event TextIndexPostingsApplyTotalMicroseconds;
    extern const Event TextIndexPostingsDecodeIOTotalMicroseconds;
    extern const Event TextIndexPostingsDecodeConstructContainerTotalMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

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

    chassert(segment < info.offsets.size());
    size_t offset_in_file = info.offsets[segment];
    stream->seekToMark({offset_in_file, 0});
    auto & in = *(stream->getDataBuffer());
    header.read(in);

    CodecUtil::readArrayU32(in, block_row_ends);
    CodecUtil::readArrayU32(in, block_offsets);
    ///
    current_values.reserve(POSTING_LIST_CHUNK_SIZE);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);

    in.readStrict(compressed_data.data(), header.payload_bytes);
    size_t full_block_count = header.cardinality / POSTING_LIST_CHUNK_SIZE;
    tail_size = header.cardinality % POSTING_LIST_CHUNK_SIZE;
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
                prepare(i);
                if (seekImpl(target))
                {
                    found = true;
                    break;
                }
            }
        }

        if (unused_segment_index > 0)
        {
            std::memmove(segments.data(), segments.data() + unused_segment_index, (segments.size() - unused_segment_index) * sizeof(uint32_t));
            segments.resize(segments.size() - unused_segment_index);
        }
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
    }
    size_t block_start = 0;
    size_t block_end = block_count;
    size_t offset = 0;

    if (block_count > 8)
    {
        const auto & row_ends = block_row_ends;
        const auto & offsets = block_offsets;

        auto it = std::lower_bound(row_ends.begin(), row_ends.end(), target);
        if (it == row_ends.end())
            return false;
        size_t skip_block_begin = static_cast<size_t>(it - row_ends.begin());

        block_start = skip_block_begin;
        offset = offsets[block_start];
    }
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()),compressed_data.size());
    compressed_data_span = compressed_data_span.subspan(offset);

    for (size_t i = block_start; i < block_end; i++)
    {
        size_t block_size = (i + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_CHUNK_SIZE;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        auto it = std::lower_bound(current_values.begin(),  current_values.begin() + block_size, target);
        if (it != current_values.begin() + block_size && *it >= target)
        {
            index = static_cast<size_t>(it - current_values.begin());
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

        size_t block_size = (current_block + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_CHUNK_SIZE;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        index = 0;
    }
}

void PostingListCursor::linearOrImpl(size_t segment, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    chassert(segment < info.ranges.size());

    size_t block_start = 0;
    size_t block_end = block_count;
    size_t offset = 0;

    if (block_count > 8)
    {
        const auto & row_ends = block_row_ends;
        const auto & offsets = block_offsets;
        size_t skip_block_begin = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_begin) - row_ends.begin());
        if (skip_block_begin >= block_count)
        {
            return;
        }
        size_t skip_block_end = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_end) - row_ends.begin());

        block_start = skip_block_begin;
        block_end = std::min(skip_block_end + 1, block_count);
        offset = offsets[block_start];
    }
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()),compressed_data.size());
    compressed_data_span = compressed_data_span.subspan(offset);

    for (size_t i = block_start; i < block_end; i++)
    {
        size_t block_size = (i + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_CHUNK_SIZE;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        auto it = std::lower_bound(current_values.begin(), current_values.end(), row_begin);
        if (it == current_values.end())
            continue;
        size_t idx = static_cast<size_t>(it - current_values.begin());
        auto it_end = std::upper_bound(current_values.begin(), current_values.end(), row_end);
        size_t end_k = it_end - current_values.begin();
#if 0
        for (size_t k = idx; k < end_k; ++k)
            out[current_values[k] - row_begin] = 1;
        if (end_k < block_size)
            return;
#endif
        const uint32_t * data = current_values.data();
        const uint32_t * data_begin = data + idx;
        const uint32_t * data_end = data + end_k;

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

        if (end_k < block_size)
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
        prepare(i);
        linearOrImpl(segment, data, row_offset, end);
    }
    if (unused_segment_index > 0)
    {
        std::memmove(segments.data(), segments.data() + unused_segment_index, (segments.size() - unused_segment_index) * sizeof(uint32_t));
        segments.resize(segments.size() - unused_segment_index);
    }
}

void PostingListCursor::linearAndImpl(size_t segment, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    chassert(segment < info.ranges.size());

    size_t block_start = 0;
    size_t block_end = block_count;
    size_t offset = 0;

    if (block_count > 8)
    {
        const auto & row_ends = block_row_ends;
        const auto & offsets = block_offsets;

        size_t skip_block_begin = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_begin) - row_ends.begin());
        if (skip_block_begin >= block_count)
        {
            return;
        }
        size_t skip_block_end = static_cast<size_t>(std::lower_bound(row_ends.begin(), row_ends.end(), row_end) - row_ends.begin());

        block_start = skip_block_begin;
        block_end = std::min(skip_block_end + 1, block_count);
        offset = offsets[block_start];
    }
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte *>(compressed_data.data()),compressed_data.size());
    compressed_data_span = compressed_data_span.subspan(offset);

    for (size_t i = block_start; i < block_end; i++)
    {
        size_t block_size = (i + 1 == block_count && tail_size) ? tail_size : POSTING_LIST_CHUNK_SIZE;
        current_block = i;
        PostingListCodecBitpackingImpl::decodeBlock(compressed_data_span, block_size, current_values);
        auto it = std::lower_bound(current_values.begin(), current_values.end(), row_begin);
        if (it == current_values.end())
            continue;
        size_t idx = static_cast<size_t>(it - current_values.begin());
#if 0
        for (size_t k = idx; k < block_size; ++k)
        {
            if (current_values[k] > row_end)
                return;
            ++out[current_values[k] - row_begin];
        }
#endif
        auto it_end = std::upper_bound(current_values.begin(), current_values.end(), row_end);
        size_t end_k = it_end - current_values.begin();
#if 0
        for (size_t k = idx; k < end_k; ++k)
            ++data[current_values[k] - row_begin];
#endif
        const uint32_t * p = current_values.data() + idx;
        const uint32_t * end = current_values.data() + end_k;

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
            case 3: ++out[p[2] - row_begin]; [[fallthrough]];
            case 2: ++out[p[1] - row_begin]; [[fallthrough]];
            case 1: ++out[p[0] - row_begin]; [[fallthrough]];
            default: break;
        }

        if (end_k < block_size)
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
        prepare(i);
        linearAndImpl(segment, data, row_offset, end);
    }

    if (unused_segment_index > 0)
    {
        std::memmove(segments.data(), segments.data() + unused_segment_index, (segments.size() - unused_segment_index) * sizeof(uint32_t));
        segments.resize(segments.size() - unused_segment_index);
    }
}

namespace
{
struct HeapItem
{
    uint32_t val = 0;
    uint32_t idx = 0;

    HeapItem() = default;
    HeapItem(uint32_t val_, uint32_t idx_) : val(val_), idx(idx_) {}
    bool operator>(const HeapItem & other) const { return val > other.val; }
};

void intersectTwo(UInt8 * out, PostingListCursorPtr c0, PostingListCursorPtr c1, size_t row_offset, size_t effective_end)
{
    while (c0->valid() && c1->valid())
    {
        uint32_t v0 = c0->value();
        uint32_t v1 = c1->value();
        if (v0 >= effective_end || v1 >= effective_end)
            return;

        if (v0 == v1) {
            out[v0 - row_offset] = 1;
            c0->next();
            c1->next();
        } else if (v0 < v1) {
            c0->seek(v1);
        } else {
            c1->seek(v0);
        }
    }
}

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

        if (v0 == v1 && v1 == v2)
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
            cursors[min_idx]->seek(max_val);
            if (!cursors[min_idx]->valid())
                return;
            vals[min_idx] = cursors[min_idx]->value();
        }
    }
}

void intersectLeapfrogHeap(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_end)
{
    const size_t n = cursors.size();


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
            out[min_val - row_offset] = 1;

            max_val = 0;
            size_t heap_size = n;

            for (size_t i = 0; i < heap_size; )
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
                ++i;
            }
            std::make_heap(heap.begin(), heap.end(), std::greater<>{});
        }
        else
        {
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
#if 0
void intersectChunkedBitmap(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_begin, size_t effective_end)
{
    const size_t n = cursors.size();
    if (n == 0)
        return;

    constexpr uint32_t CHUNK_BITS = 64;
    uint32_t chunk_begin = (static_cast<uint32_t>(effective_begin) / CHUNK_BITS) * CHUNK_BITS;
    uint32_t effective_chunk_end = (static_cast<uint32_t>(effective_end) / CHUNK_BITS) * CHUNK_BITS + 1;

    while (chunk_begin < effective_chunk_end)
    {
        uint32_t chunk_next = chunk_begin + CHUNK_BITS;

        uint64_t intersection = ~0ULL;

        for (size_t i = 0; i < n; ++i)
        {
            uint64_t bitmap = 0;
            while (cursors[i]->valid())
            {
                uint32_t val = cursors[i]->value();
                if (val >= chunk_next)
                    break;

                if (val >= chunk_begin)
                {
                    uint32_t bit_pos = val - chunk_begin;
                    bitmap |= (1ULL << bit_pos);
                }

                cursors[i]->next();
            }

            intersection &= bitmap;

            if (intersection == 0)
                break;
        }

        while (intersection != 0)
        {
            uint32_t bit_pos = __builtin_ctzll(intersection);
            uint32_t rowid = chunk_begin + bit_pos;

            if (rowid >= row_offset && rowid < effective_end)
                out[rowid - row_offset] = 1;

            intersection &= (intersection - 1);
        }

        for (size_t i = 0; i < n; ++i)
        {
            if (!cursors[i]->valid() || cursors[i]->value() >= effective_end)
                return;
        }

        chunk_begin = chunk_next;
    }
}
#endif
void intersectBruteForce(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t num_rows)
{
    cursors[0]->linearOr(out, row_offset, num_rows);

    for (size_t i = 1; i < cursors.size(); ++i)
        cursors[i]->linearAnd(out, row_offset, num_rows);

    size_t n = cursors.size();
#if 0
    if (n > 1)
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            out[i] = out[i] == n;
        }
    }
#endif
    if (n > 1)
    {
        UInt8 * p = out;
        UInt8 * end = out + num_rows;
        UInt8 * end_loop = out + (num_rows / 4) * 4;
        UInt8 n8 = static_cast<UInt8>(n);

        for (; p < end_loop; p += 4)
        {
            __builtin_prefetch(p + 64, 0, 3);
            __builtin_prefetch(p + 64, 1, 0);

            p[0] = (p[0] == n8);
            p[1] = (p[1] == n8);
            p[2] = (p[2] == n8);
            p[3] = (p[3] == n8);
        }

        while (p < end)
        {
            *p = (*p == n8);
            ++p;
        }
    }
}
}

void streamApplyPostingsAny(IColumn & column, std::vector<PostingListCursorPtr> & cursors, size_t column_offset, size_t row_offset, size_t num_rows, UInt64)
{
    auto & data = assert_cast<DB::ColumnUInt8 &>(column).getData();
    UInt8 * out = data.data() + column_offset;

    for (const auto & cursor : cursors)
        cursor->linearOr(out, row_offset, num_rows);
}

void streamApplyPostingsAll(IColumn & column, std::vector<PostingListCursorPtr> & cursors, size_t column_offset, size_t row_offset, size_t num_rows, UInt64 text_index_intersect_algorithm)
{
    auto & data = assert_cast<DB::ColumnUInt8 &>(column).getData();
    UInt8 * __restrict out = data.data() + column_offset;

    const size_t n = cursors.size();
    const size_t end = row_offset + num_rows;

    if (n == 0)
        return;

    if (n == 1)
    {
        cursors.front()->linearOr(out, row_offset, num_rows);
        return;
    }

    double density = 0;
    for (size_t i = 0; i < n; ++i)
        density += cursors[i]->density();
    density = density / n;

    if (n < 256 && (density >= 0.2 || text_index_intersect_algorithm == 1))
        return intersectBruteForce(out, cursors, row_offset, num_rows);

    for (size_t i = 0; i < n; ++i)
    {
        cursors[i]->seek(static_cast<uint32_t>(row_offset));
        if (!cursors[i]->valid() || cursors[i]->value() >= end)
            return;
    }

    return intersectLeapfrog(out, cursors, row_offset, end);
}

}
