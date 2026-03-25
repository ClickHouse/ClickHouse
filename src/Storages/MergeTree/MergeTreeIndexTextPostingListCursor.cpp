#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/ProfileEvents.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/TargetSpecific.h>
#include <algorithm>
#include <cstring>
#include <numeric>

namespace ProfileEvents
{
    extern const Event TextIndexLazyPackedBlocksDecoded;
    extern const Event TextIndexLazySeekCount;
    extern const Event TextIndexLazySegmentsPrepared;
    extern const Event TextIndexLazyBruteForceIntersections;
    extern const Event TextIndexLazyLeapfrogIntersections;
    extern const Event TextIndexLazySegmentsSkippedDense;
    extern const Event TextIndexLazySegmentsSkippedCovered;
    extern const Event TextIndexLazyBlocksSkippedCovered;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

PostingListCursor::PostingListCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_)
    : stream(&stream_)
    , info(info_)
    , total_segments(info.offsets.size())
{
    /// Compute global density once: cardinality / total_range_span.
    if (!info.ranges.empty())
    {
        uint32_t min_row = static_cast<uint32_t>(info.ranges.front().begin);
        uint32_t max_row = static_cast<uint32_t>(info.ranges.back().end);
        uint32_t span = max_row - min_row + 1;
        density_val = span > 0 ? static_cast<double>(info.cardinality) / static_cast<double>(span) : 0.0;
    }
}

PostingListCursor::PostingListCursor(const TokenPostingsInfo & info_)
    : info(info_)
    , total_segments(info.offsets.size())
    , is_embedded(true)
{
    if (info.embedded_postings)
    {
        /// Embedded postings must fit in a single decoded block.
        /// If this ever fires, the token should use compressed postings instead.
        if (info.cardinality > BLOCK_SIZE)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Embedded posting list cardinality ({}) exceeds BLOCK_SIZE ({})",
                info.cardinality, BLOCK_SIZE);

        /// Decode all embedded postings into decoded_values.
        decoded_count = static_cast<size_t>(info.cardinality);
        if (decoded_count > 0)
        {
            std::vector<uint32_t> buf(info.cardinality);
            info.embedded_postings->toUint32Array(buf.data());
            for (size_t i = 0; i < decoded_count; ++i)
                decoded_values[i] = buf[i];
        }
        is_valid = decoded_count > 0;

        if (!info.ranges.empty())
        {
            uint32_t min_row = static_cast<uint32_t>(info.ranges.front().begin);
            uint32_t max_row = static_cast<uint32_t>(info.ranges.back().end);
            uint32_t span = max_row - min_row + 1;
            density_val = span > 0 ? static_cast<double>(info.cardinality) / static_cast<double>(span) : 0.0;
        }
    }
    else
    {
        is_valid = false;
    }
}

UInt32 PostingListCursor::cardinality() const
{
    return info.cardinality;
}

void PostingListCursor::prepareSegment(size_t segment_idx)
{
    ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsPrepared);

    current_segment_idx = segment_idx;
    has_prepared_first_segment = true;

    if (is_embedded)
        return;

    chassert(segment_idx < total_segments);

    UInt64 segment_file_offset = info.offsets[segment_idx];

    /// Seek to segment start and read the header.
    stream->seekToMark({segment_file_offset, 0});
    auto * data_buffer = stream->getDataBuffer();

    /// Read the segment header.
    UInt64 codec_type;
    readVarUInt(codec_type, *data_buffer);
    UInt64 payload_bytes;
    readVarUInt(payload_bytes, *data_buffer);
    UInt64 seg_cardinality;
    readVarUInt(seg_cardinality, *data_buffer);
    UInt64 first_row_id;
    readVarUInt(first_row_id, *data_buffer);

    segment_doc_count = static_cast<UInt32>(seg_cardinality);
    last_decoded_doc_id = static_cast<UInt32>(first_row_id);
    segment_first_row_id = static_cast<UInt32>(first_row_id);

    /// Bulk-read the entire payload into memory.
    payload_buffer.resize(payload_bytes);
    data_buffer->readStrict(reinterpret_cast<char *>(payload_buffer.data()), payload_bytes);

    /// The Index Section follows immediately after the payload in the .pst stream.
    /// No additional seek needed — just continue reading.

    UInt64 num_blocks;
    readVarUInt(num_blocks, *data_buffer);

    block_last_row_ids.resize(num_blocks);
    block_offsets.resize(num_blocks);

    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v;
        readVarUInt(v, *data_buffer);
        block_last_row_ids[i] = static_cast<UInt32>(v);
    }

    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v;
        readVarUInt(v, *data_buffer);
        block_offsets[i] = v;
    }

    block_count = num_blocks;
    tail_size = segment_doc_count % BLOCK_SIZE;
    current_block = 0;
    decoded_count = 0;
    index = 0;
}

void PostingListCursor::decodeBlock(size_t block_idx)
{
    ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded);

    chassert(block_idx < block_count);
    current_block = block_idx;

    /// Determine the base doc_id for delta decoding.
    /// For the first block, use the segment's first_row_id (already in last_decoded_doc_id
    /// if this is the first block decoded in sequence). For arbitrary seeks, we need to
    /// figure out the correct base from the previous block's last_row_id.
    if (block_idx == 0)
    {
        /// Use cached first_row_id from prepareSegment.
        last_decoded_doc_id = segment_first_row_id;
    }
    else
    {
        last_decoded_doc_id = block_last_row_ids[block_idx - 1];
    }

    /// Determine block element count: BLOCK_SIZE for full blocks, tail_size for the last block.
    size_t count = BLOCK_SIZE;
    if (block_idx == block_count - 1 && tail_size > 0)
        count = tail_size;

    /// Read from payload buffer at the relative offset.
    size_t payload_offset = static_cast<size_t>(block_offsets[block_idx]);
    std::span<const std::byte> block_data(
        reinterpret_cast<const std::byte *>(payload_buffer.data() + payload_offset),
        payload_buffer.size() - payload_offset);

    /// Decode: [1 byte bits][bitpacked payload]
    if (block_data.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: empty block at index {}", block_idx);

    uint8_t bits = static_cast<uint8_t>(block_data[0]);
    block_data = block_data.subspan(1);

    if (bits > 32)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected bits <= 32, but got {}", bits);

    std::span<uint32_t> out_span(decoded_values, count);
    BitpackingBlockCodec::decode(block_data, count, bits, out_span);

    /// Restore absolute row ids from deltas directly in decoded_values.
    std::inclusive_scan(decoded_values, decoded_values + count, decoded_values, std::plus<uint32_t>{}, last_decoded_doc_id);
    last_decoded_doc_id = count > 0 ? decoded_values[count - 1] : last_decoded_doc_id;

    decoded_count = count;
    index = 0;
}

void PostingListCursor::seek(uint32_t target)
{
    ProfileEvents::increment(ProfileEvents::TextIndexLazySeekCount);

    if (!is_valid)
        return;

    if (is_embedded)
    {
        auto it = std::lower_bound(decoded_values, decoded_values + decoded_count, target);
        if (it != decoded_values + decoded_count)
        {
            index = static_cast<size_t>(it - decoded_values);
        }
        else
        {
            is_valid = false;
        }
        return;
    }

    /// Try current segment first.
    if (has_prepared_first_segment)
    {
        if (target <= static_cast<uint32_t>(info.ranges[current_segment_idx].end))
        {
            if (seekImpl(target))
                return;
        }
    }

    /// Search across segments.
    for (size_t i = has_prepared_first_segment ? current_segment_idx + 1 : 0; i < total_segments; ++i)
    {
        if (target > static_cast<uint32_t>(info.ranges[i].end))
            continue;

        prepareSegment(i);
        if (seekImpl(target))
            return;
    }

    is_valid = false;
}

bool PostingListCursor::seekImpl(uint32_t target)
{
    /// If current block contains the target, search within it.
    if (decoded_count > 0 && target <= decoded_values[decoded_count - 1])
    {
        auto it = std::lower_bound(decoded_values + index, decoded_values + decoded_count, target);
        if (it != decoded_values + decoded_count)
        {
            index = static_cast<size_t>(it - decoded_values);
            return true;
        }
    }

    /// Binary search on block_last_row_ids.
    auto it = std::lower_bound(block_last_row_ids.begin(), block_last_row_ids.end(), target);
    if (it == block_last_row_ids.end())
        return false;

    size_t j = static_cast<size_t>(it - block_last_row_ids.begin());

    if (j != current_block || decoded_count == 0)
        decodeBlock(j);

    /// Binary search within the decoded packed block.
    auto found_it = std::lower_bound(decoded_values, decoded_values + decoded_count, target);
    if (found_it != decoded_values + decoded_count)
    {
        index = static_cast<size_t>(found_it - decoded_values);
        return true;
    }

    return false;
}

void PostingListCursor::next()
{
    if (!is_valid)
        return;

    ++index;

    if (is_embedded)
    {
        if (index >= decoded_count)
            is_valid = false;
        return;
    }

    if (index >= decoded_count)
    {
        ++current_block;
        if (current_block < block_count)
        {
            decodeBlock(current_block);
            return;
        }

        /// Current segment exhausted — advance to next one.
        size_t next_segment = current_segment_idx + 1;
        if (next_segment >= total_segments)
        {
            is_valid = false;
            return;
        }

        prepareSegment(next_segment);
        decodeBlock(0);
    }
}

/// Scatter-write into `out` for doc_ids in values[begin..length).
/// PadOp::Or assigns 1, PadOp::And increments the counter.
namespace
{

enum class PadOp { Or, And };

template <PadOp op>
inline void padColumn(UInt8 * __restrict out, const uint32_t * values, size_t row_begin, size_t begin, size_t length)
{
    for (size_t i = begin; i < length; ++i)
    {
        size_t relative = values[i] - row_begin;
        if constexpr (op == PadOp::Or)
            out[relative] = 1;
        else
            ++out[relative];
    }
}

#if USE_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(
/// Check whether a byte range contains no zero bytes (i.e., all positions are already set).
/// Uses 256-bit (AVX2) loads with 4x loop unrolling for the hot path (128 bytes/iter).
bool hasNoZeros(const UInt8 * data, size_t count)
{
    const __m256i * ptr = reinterpret_cast<const __m256i *>(data);

    /// Process 128 bytes (4 × 32-byte vectors) per iteration.
    size_t i = 0;
    for (; i + 128 <= count; i += 128, ptr += 4)
    {
        __m256i v0 = _mm256_loadu_si256(ptr);
        __m256i v1 = _mm256_loadu_si256(ptr + 1);
        __m256i v2 = _mm256_loadu_si256(ptr + 2);
        __m256i v3 = _mm256_loadu_si256(ptr + 3);

        __m256i combined = _mm256_and_si256(_mm256_and_si256(v0, v1), _mm256_and_si256(v2, v3));
        __m256i zero = _mm256_setzero_si256();
        __m256i cmp = _mm256_cmpeq_epi8(combined, zero);
        if (_mm256_movemask_epi8(cmp) != 0)
            return false;
    }

    /// Scalar tail.
    const UInt8 * tail = data + i;
    size_t remaining = count - i;
    return memchr(tail, 0, remaining) == nullptr;
}
) /// DECLARE_X86_64_V3_SPECIFIC_CODE
#endif

/// Runtime-dispatched `hasNoZeros`: returns true if all bytes in [data, data + count) are non-zero.
bool hasNoZeros(const UInt8 * data, size_t count)
{
    if (count == 0)
        return true;

#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::hasNoZeros(data, count);
#endif

    return memchr(data, 0, count) == nullptr;
}

} // anonymous namespace

void PostingListCursor::linearOr(UInt8 * data, size_t row_offset, size_t num_rows)
{
    if (is_embedded)
    {
        /// Level 1 (dense memset): if every row in the range is in the posting list, just memset.
        if (!info.ranges.empty())
        {
            size_t range_begin = info.ranges.front().begin;
            size_t range_end = info.ranges.back().end;
            size_t range_span = range_end - range_begin + 1;

            if (info.cardinality == range_span)
            {
                size_t clip_begin = std::max(range_begin, row_offset);
                size_t clip_end = std::min(range_end + 1, row_offset + num_rows);
                if (clip_begin < clip_end)
                {
                    ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsSkippedDense);
                    memset(data + (clip_begin - row_offset), 1, clip_end - clip_begin);
                    return;
                }
            }
        }

        /// Find range within decoded_values.
        auto * begin_it = std::lower_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_offset));
        auto * end_it = std::lower_bound(begin_it, decoded_values + decoded_count, static_cast<uint32_t>(row_offset + num_rows));
        size_t begin_idx = static_cast<size_t>(begin_it - decoded_values);
        size_t end_idx = static_cast<size_t>(end_it - decoded_values);
        padColumn<PadOp::Or>(data, decoded_values, row_offset, begin_idx, end_idx);
        return;
    }

    for (size_t i = current_segment_idx; i < total_segments; ++i)
    {
        size_t seg_begin = info.ranges[i].begin;
        size_t seg_end = info.ranges[i].end;

        if (row_offset > seg_end)
            continue;
        if (row_offset + num_rows <= seg_begin)
            break;

        /// Level 2a: segment-level "already-covered" skip.
        /// If the output region for this segment is already all-ones, skip entirely
        /// (saves the I/O cost of prepareSegment).
        {
            size_t clip_begin = std::max(seg_begin, row_offset);
            size_t clip_end = std::min(seg_end + 1, row_offset + num_rows);
            if (clip_begin < clip_end)
            {
                size_t clip_off = clip_begin - row_offset;
                size_t clip_count = clip_end - clip_begin;
                if (hasNoZeros(data + clip_off, clip_count))
                {
                    ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsSkippedCovered);
                    continue;
                }
            }
        }

        /// Skip re-preparing the segment if it is already loaded.
        if (i != current_segment_idx || !has_prepared_first_segment)
            prepareSegment(i);

        /// Level 1: dense segment memset.
        /// If every row in the segment range has a posting, we can memset instead of decoding blocks.
        {
            size_t range_span = seg_end - seg_begin + 1;
            if (segment_doc_count == range_span)
            {
                size_t clip_begin = std::max(seg_begin, row_offset);
                size_t clip_end = std::min(seg_end + 1, row_offset + num_rows);
                if (clip_begin < clip_end)
                {
                    ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsSkippedDense);
                    memset(data + (clip_begin - row_offset), 1, clip_end - clip_begin);
                    continue;
                }
            }
        }

        /// Decode all blocks in this segment that overlap with [row_offset, row_offset + num_rows).
        for (size_t b = 0; b < block_count; ++b)
        {
            uint32_t block_last = block_last_row_ids[b];
            uint32_t block_first = (b == 0)
                ? static_cast<uint32_t>(seg_begin)
                : (block_last_row_ids[b - 1] + 1);

            if (block_last < row_offset)
                continue;
            if (block_first >= row_offset + num_rows)
                break;

            /// Level 2b: block-level "already-covered" skip.
            {
                size_t blk_clip_begin = std::max(static_cast<size_t>(block_first), row_offset);
                size_t blk_clip_end = std::min(static_cast<size_t>(block_last) + 1, row_offset + num_rows);
                if (blk_clip_begin < blk_clip_end)
                {
                    size_t blk_off = blk_clip_begin - row_offset;
                    size_t blk_cnt = blk_clip_end - blk_clip_begin;
                    if (hasNoZeros(data + blk_off, blk_cnt))
                    {
                        ProfileEvents::increment(ProfileEvents::TextIndexLazyBlocksSkippedCovered);
                        continue;
                    }
                }
            }

            decodeBlock(b);

            auto * begin_it = std::lower_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_offset));
            auto * end_it = std::lower_bound(begin_it, decoded_values + decoded_count, static_cast<uint32_t>(row_offset + num_rows));
            size_t begin_idx = static_cast<size_t>(begin_it - decoded_values);
            size_t end_idx = static_cast<size_t>(end_it - decoded_values);
            padColumn<PadOp::Or>(data, decoded_values, row_offset, begin_idx, end_idx);
        }
    }
}

void PostingListCursor::linearAnd(UInt8 * data, size_t row_offset, size_t num_rows)
{
    if (is_embedded)
    {
        auto * begin_it = std::lower_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_offset));
        auto * end_it = std::lower_bound(begin_it, decoded_values + decoded_count, static_cast<uint32_t>(row_offset + num_rows));
        size_t begin_idx = static_cast<size_t>(begin_it - decoded_values);
        size_t end_idx = static_cast<size_t>(end_it - decoded_values);
        padColumn<PadOp::And>(data, decoded_values, row_offset, begin_idx, end_idx);
        return;
    }

    for (size_t i = current_segment_idx; i < total_segments; ++i)
    {
        size_t seg_begin = info.ranges[i].begin;
        size_t seg_end = info.ranges[i].end;

        if (row_offset > seg_end)
            continue;
        if (row_offset + num_rows <= seg_begin)
            break;

        /// Skip re-preparing the segment if it is already loaded.
        if (i != current_segment_idx || !has_prepared_first_segment)
            prepareSegment(i);

        for (size_t b = 0; b < block_count; ++b)
        {
            uint32_t block_last = block_last_row_ids[b];
            uint32_t block_first = (b == 0)
                ? static_cast<uint32_t>(seg_begin)
                : (block_last_row_ids[b - 1] + 1);

            if (block_last < row_offset)
                continue;
            if (block_first >= row_offset + num_rows)
                break;

            decodeBlock(b);

            auto * begin_it = std::lower_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_offset));
            auto * end_it = std::lower_bound(begin_it, decoded_values + decoded_count, static_cast<uint32_t>(row_offset + num_rows));
            size_t begin_idx = static_cast<size_t>(begin_it - decoded_values);
            size_t end_idx = static_cast<size_t>(end_it - decoded_values);
            padColumn<PadOp::And>(data, decoded_values, row_offset, begin_idx, end_idx);
        }
    }
}

namespace
{

/// Two-cursor intersection. The lagging cursor seeks to the leading cursor's doc_id.
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

/// Three-cursor intersection. All cursors behind the maximum seek forward.
void intersectThree(UInt8 * out, PostingListCursorPtr c0, PostingListCursorPtr c1, PostingListCursorPtr c2, size_t row_offset, size_t effective_end)
{
    while (c0->valid() && c1->valid() && c2->valid())
    {
        uint32_t v0 = c0->value();
        uint32_t v1 = c1->value();
        uint32_t v2 = c2->value();

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
            if (v0 < max_val) c0->seek(max_val);
            if (v1 < max_val) c1->seek(max_val);
            if (v2 < max_val) c2->seek(max_val);
        }
    }
}

/// Four-cursor intersection.
void intersectFour(UInt8 * out, PostingListCursorPtr c0, PostingListCursorPtr c1, PostingListCursorPtr c2, PostingListCursorPtr c3, size_t row_offset, size_t effective_end)
{
    while (c0->valid() && c1->valid() && c2->valid() && c3->valid())
    {
        uint32_t v0 = c0->value();
        uint32_t v1 = c1->value();
        uint32_t v2 = c2->value();
        uint32_t v3 = c3->value();

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
            if (v0 < max_val) c0->seek(max_val);
            if (v1 < max_val) c1->seek(max_val);
            if (v2 < max_val) c2->seek(max_val);
            if (v3 < max_val) c3->seek(max_val);
        }
    }
}

/// N-way leapfrog intersection (N <= 8): linear scan for min/max.
void intersectLeapfrogLinear(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_end)
{
    const size_t n = cursors.size();
    std::vector<uint32_t> vals(n);
    for (size_t i = 0; i < n; ++i)
        vals[i] = cursors[i]->value();

    while (true)
    {
        uint32_t min_val = vals[0];
        uint32_t max_val = vals[0];

        for (size_t i = 1; i < n; ++i)
        {
            if (vals[i] < min_val) min_val = vals[i];
            else if (vals[i] > max_val) max_val = vals[i];
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
            for (size_t i = 0; i < n; ++i)
            {
                if (vals[i] < max_val)
                {
                    cursors[i]->seek(max_val);
                    if (!cursors[i]->valid())
                        return;
                    vals[i] = cursors[i]->value();
                }
            }
        }
    }
}

/// Element for the min-heap used in N-way intersection.
struct HeapItem
{
    uint32_t val = 0;
    uint32_t idx = 0;

    HeapItem() = default;
    HeapItem(uint32_t val_, uint32_t idx_) : val(val_), idx(idx_) {}
    bool operator>(const HeapItem & other) const { return val > other.val; }
};

/// N-way leapfrog intersection (N > 8): min-heap.
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

            for (size_t i = 0; i < n; ++i)
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

/// Dispatch to the best leapfrog variant based on cursor count.
void intersectLeapfrog(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t effective_end)
{
    if (cursors.size() == 2)
    {
        intersectTwo(out, cursors[0], cursors[1], row_offset, effective_end);
        return;
    }

    if (cursors.size() == 3)
    {
        intersectThree(out, cursors[0], cursors[1], cursors[2], row_offset, effective_end);
        return;
    }

    if (cursors.size() == 4)
    {
        intersectFour(out, cursors[0], cursors[1], cursors[2], cursors[3], row_offset, effective_end);
        return;
    }

    if (cursors.size() <= 8)
    {
        intersectLeapfrogLinear(out, cursors, row_offset, effective_end);
        return;
    }

    intersectLeapfrogHeap(out, cursors, row_offset, effective_end);
}

/// Brute-force intersection via bitmap counting.
/// First cursor sets bits (linearOr), remaining cursors increment counters (linearAnd),
/// then a final pass converts count == n into 1, everything else into 0.
void intersectBruteForce(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t num_rows)
{
    cursors[0]->linearOr(out, row_offset, num_rows);

    for (size_t i = 1; i < cursors.size(); ++i)
        cursors[i]->linearAnd(out, row_offset, num_rows);

    size_t n = cursors.size();
    if (n > 1)
    {
        UInt8 n8 = static_cast<UInt8>(n);
        for (size_t i = 0; i < num_rows; ++i)
            out[i] = (out[i] == n8);
    }
}

} // anonymous namespace

void lazyUnionPostingLists(
    IColumn & column,
    const PostingListCursorMap & postings,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows)
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

    /// Sort by descending density so the densest cursor fills the output buffer first.
    std::stable_sort(cursors.begin(), cursors.end(),
        [](const PostingListCursorPtr & a, const PostingListCursorPtr & b)
        { return a->density() > b->density(); });

    for (auto & cursor : cursors)
        cursor->linearOr(out, row_offset, num_rows);
}

void lazyIntersectPostingLists(
    IColumn & column,
    const PostingListCursorMap & postings,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows,
    float density_threshold)
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

    if (n == 1)
    {
        cursors.front()->linearOr(out, row_offset, num_rows);
        return;
    }

    /// Algorithm selection uses the MINIMUM density across all cursors.
    double min_density = std::numeric_limits<double>::max();
    for (size_t i = 0; i < n; ++i)
        min_density = std::min(min_density, cursors[i]->density());

    /// n < 256: brute-force uses UInt8 counters per row — would overflow with 256+ cursors.
    if (n < 256 && min_density >= density_threshold)
    {
        ProfileEvents::increment(ProfileEvents::TextIndexLazyBruteForceIntersections);
        intersectBruteForce(out, cursors, row_offset, num_rows);
        return;
    }

    /// Sort cursors by ascending cardinality so the sparsest cursor leads the leapfrog.
    std::sort(cursors.begin(), cursors.end(),
        [](const PostingListCursorPtr & a, const PostingListCursorPtr & b)
        { return a->cardinality() < b->cardinality(); });

    for (size_t i = 0; i < n; ++i)
    {
        cursors[i]->seek(static_cast<uint32_t>(row_offset));
        if (!cursors[i]->valid() || cursors[i]->value() >= end)
            return;
    }

    ProfileEvents::increment(ProfileEvents::TextIndexLazyLeapfrogIntersections);
    intersectLeapfrog(out, cursors, row_offset, end);
}

}
