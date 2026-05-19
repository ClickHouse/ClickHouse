#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/ProfileEvents.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/TargetSpecific.h>
#include <algorithm>
#include <cstring>
#include <numeric>

namespace ProfileEvents
{
    extern const Event TextIndexLazyPackedBlocksDecoded;
    extern const Event TextIndexLazyAdvanceCount;
    extern const Event TextIndexLazySegmentsPrepared;
    extern const Event TextIndexLazyBruteForceIntersections;
    extern const Event TextIndexLazyLeapfrogIntersections;
    extern const Event TextIndexLazySegmentsSkippedDense;
    extern const Event TextIndexLazySegmentsSkippedCovered;
    extern const Event TextIndexLazyBlocksSkippedCovered;
    extern const Event TextIndexLazyAndSegmentsSkippedZero;
    extern const Event TextIndexLazyAndBlocksSkippedZero;
    extern const Event TextIndexLazyAndSegmentsSkippedDense;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

double computeDensity(const TokenPostingsInfo & info)
{
    if (info.ranges.empty())
        return 0.0;

    double span = static_cast<double>(info.ranges.back().end) - static_cast<double>(info.ranges.front().begin) + 1.0;
    return span > 0.0 ? static_cast<double>(info.cardinality) / span : 0.0;
}

/// Narrow an on-disk UInt64 field to UInt32, throwing CORRUPTED_DATA if the value exceeds
/// the representable range. Used only on cold per-segment paths (`prepareSegment`); the
/// hot per-block decode path doesn't validate again.
inline UInt32 requireUInt32(UInt64 value, std::string_view field_name)
{
    if (value > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: {} value {} exceeds UInt32 max",
            field_name, value);
    }

    return static_cast<UInt32>(value);
}

}

PostingListCursor::PostingListCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_)
    : stream(&stream_)
    , info(&info_)
    , total_segments(info_.offsets.size())
    , density_val(computeDensity(info_))
{
}

PostingListCursor::PostingListCursor(const TokenPostingsInfo & info_)
    : owned_info(std::make_shared<TokenPostingsInfo>(info_))
    , info(owned_info.get())
    , total_segments(info_.offsets.size())
    , is_embedded(true)
    , density_val(computeDensity(info_))
{
    if (!info->embedded_postings || info->cardinality == 0)
    {
        is_valid = false;
        return;
    }

    embedded_values.resize(info->cardinality);
    info->embedded_postings->toUint32Array(embedded_values.data());

    /// Zero-copy: iteration reads directly from `embedded_values`. Works for embedded
    /// lists larger than BLOCK_SIZE because the array lives in the cursor itself.
    decoded_count = embedded_values.size();
    decoded_values_ptr = embedded_values.data();
}

UInt32 PostingListCursor::cardinality() const
{
    return info->cardinality;
}

PostingListCursor::~PostingListCursor()
{
    if (counters.blocks_decoded)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded, counters.blocks_decoded);
    if (counters.advance_count)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyAdvanceCount, counters.advance_count);
    if (counters.segments_prepared)
        ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsPrepared, counters.segments_prepared);
    if (counters.segments_skipped_dense)
        ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsSkippedDense, counters.segments_skipped_dense);
    if (counters.segments_skipped_covered)
        ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsSkippedCovered, counters.segments_skipped_covered);
    if (counters.blocks_skipped_covered)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyBlocksSkippedCovered, counters.blocks_skipped_covered);
    if (counters.and_segments_skipped_dense)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyAndSegmentsSkippedDense, counters.and_segments_skipped_dense);
    if (counters.and_segments_skipped_zero)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyAndSegmentsSkippedZero, counters.and_segments_skipped_zero);
    if (counters.and_blocks_skipped_zero)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyAndBlocksSkippedZero, counters.and_blocks_skipped_zero);
}

void PostingListCursor::prepareSegment(size_t segment_idx)
{
    ++counters.segments_prepared;

    current_segment_idx = segment_idx;
    has_prepared_first_segment = true;

    if (is_embedded)
        return;

    chassert(segment_idx < total_segments);
    UInt64 segment_file_offset = info->offsets[segment_idx];

    /// Seek to segment start and read the header.
    stream->seekToMark({segment_file_offset, 0});
    auto * data_buffer = stream->getDataBuffer();

    /// Read the segment header.
    UInt64 codec_type;
    readVarUInt(codec_type, *data_buffer);

    if (codec_type != static_cast<UInt64>(IPostingListCodec::Type::Bitpacking))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy cursor: expected codec type Bitpacking, got {}", codec_type);

    UInt64 payload_bytes;
    readVarUInt(payload_bytes, *data_buffer);
    UInt64 seg_cardinality;
    readVarUInt(seg_cardinality, *data_buffer);
    UInt64 first_row_id;
    readVarUInt(first_row_id, *data_buffer);

    segment_doc_count = requireUInt32(seg_cardinality, "seg_cardinality");
    last_decoded_doc_id = requireUInt32(first_row_id, "first_row_id");
    segment_first_row_id = last_decoded_doc_id;

    /// Bulk-read the entire payload into memory.
    payload_buffer.resize(payload_bytes);
    data_buffer->readStrict(reinterpret_cast<char *>(payload_buffer.data()), payload_bytes);

    if (!(info->header & PostingsSerialization::Flags::HasBlockIndex))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Lazy posting list cursor requires the per-segment block index (HasBlockIndex flag); "
            "the reader must have disabled lazy mode for indexes without it");
    }

    /// Index Section follows immediately after the payload in the .pst stream.
    /// No additional seek needed — just continue reading.
    UInt64 num_blocks;
    readVarUInt(num_blocks, *data_buffer);

    UInt64 max_blocks = (segment_doc_count + BLOCK_SIZE - 1) / BLOCK_SIZE;

    if (num_blocks > max_blocks)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Posting list num_blocks {} exceeds maximum {} for segment with {} documents",
            num_blocks, max_blocks, segment_doc_count);
    }

    if (num_blocks == 0)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Posting list num_blocks is 0 for segment with {} documents",
            segment_doc_count);
    }

    block_last_row_ids.resize(num_blocks);
    block_offsets.resize(num_blocks);

    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v;
        readVarUInt(v, *data_buffer);
        block_last_row_ids[i] = static_cast<UInt32>(v);

        chassert(v <= std::numeric_limits<UInt32>::max());
        chassert(i == 0 || block_last_row_ids[i] > block_last_row_ids[i - 1]);
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
    ++counters.blocks_decoded;

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

    if (payload_offset >= payload_buffer.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data: block offset {} is out of payload bounds {}", payload_offset, payload_buffer.size());

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

    /// The packed payload must fit in the remaining block bytes
    chassert(BitpackingBlockCodec::bitpackingCompressedBytes(count, bits) <= block_data.size());

    std::span<uint32_t> out_span(decoded_values, count);
    BitpackingBlockCodec::decode(block_data, count, bits, out_span);

    /// Restore absolute row ids from deltas directly in decoded_values.
    std::inclusive_scan(decoded_values, decoded_values + count, decoded_values, std::plus<uint32_t>{}, last_decoded_doc_id);
    last_decoded_doc_id = count > 0 ? decoded_values[count - 1] : last_decoded_doc_id;

    decoded_count = count;
    index = 0;
}

void PostingListCursor::advance(uint32_t target)
{
    ++counters.advance_count;

    if (!is_valid)
        return;

    if (is_embedded)
    {
        const auto * it = std::lower_bound(decoded_values_ptr, decoded_values_ptr + decoded_count, target);
        if (it != decoded_values_ptr + decoded_count)
        {
            index = static_cast<size_t>(it - decoded_values_ptr);
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
        if (target <= static_cast<uint32_t>(info->ranges[current_segment_idx].end))
        {
            if (advanceImpl(target))
                return;
        }
    }

    /// Binary search across segments.
    size_t start = has_prepared_first_segment ? current_segment_idx + 1 : 0;
    const auto * it = std::lower_bound(
        info->ranges.begin() + start, info->ranges.end(), static_cast<size_t>(target),
        [](const RowsRange & range, size_t t) { return range.end < t; });

    for (size_t i = static_cast<size_t>(it - info->ranges.begin()); i < total_segments; ++i)
    {
        prepareSegment(i);
        if (advanceImpl(target))
            return;
    }

    is_valid = false;
}

bool PostingListCursor::advanceImpl(uint32_t target)
{
    /// If current block contains the target, search within it.
    if (decoded_count > 0 && target <= decoded_values_ptr[decoded_count - 1])
    {
        const auto * it = std::lower_bound(decoded_values_ptr + index, decoded_values_ptr + decoded_count, target);
        if (it != decoded_values_ptr + decoded_count)
        {
            index = static_cast<size_t>(it - decoded_values_ptr);
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
    const auto * found_it = std::lower_bound(decoded_values_ptr, decoded_values_ptr + decoded_count, target);
    if (found_it != decoded_values_ptr + decoded_count)
    {
        index = static_cast<size_t>(found_it - decoded_values_ptr);
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

/// Iterator to the first row_id >= row_offset + num_rows. Returns `end` directly when the
/// exclusive bound exceeds UInt32::max — saturating would drop a match at the boundary.
inline const uint32_t * findRowRangeEnd(const uint32_t * begin, const uint32_t * end, size_t row_offset, size_t num_rows)
{
    size_t exclusive_end = row_offset + num_rows;
    if (exclusive_end > std::numeric_limits<uint32_t>::max())
        return end;
    return std::lower_bound(begin, end, static_cast<uint32_t>(exclusive_end));
}

/// Posting-list doc IDs are 32-bit, so `row_offset > UInt32::max` cannot legitimately
/// occur and would underflow `out[v - row_offset]` indexing in `padColumn` / leapfrog
/// writers. Throw rather than silently emit a zero filter and drop matches.
inline void requireRowOffsetRepresentable(size_t row_offset)
{
    if (row_offset > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Posting-list cursor doesn't support row_offset larger than UINT32_MAX, got {}", row_offset);
}

enum class PadOp { Or, And };

template <PadOp op>
inline void padColumn(UInt8 * __restrict out, const uint32_t * values, size_t row_begin, size_t begin, size_t end)
{
    for (size_t i = begin; i < end; ++i)
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

        /// "Any byte in the 4 vectors is zero" — compare each vector to zero independently and OR the masks.
        __m256i zero = _mm256_setzero_si256();
        __m256i any_zero = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(v0, zero), _mm256_cmpeq_epi8(v1, zero)),
            _mm256_or_si256(_mm256_cmpeq_epi8(v2, zero), _mm256_cmpeq_epi8(v3, zero)));

        if (_mm256_movemask_epi8(any_zero) != 0)
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
    requireRowOffsetRepresentable(row_offset);

    if (is_embedded)
    {
        if (decoded_count == 0)
            return;

        /// Level 1 (dense memset): if every row in the range is in the posting list, just memset.
        if (!info->ranges.empty())
        {
            size_t range_begin = info->ranges.front().begin;
            size_t range_end = info->ranges.back().end;
            size_t range_span = range_end - range_begin + 1;

            if (info->cardinality == range_span)
            {
                size_t clip_begin = std::max(range_begin, row_offset);
                size_t clip_end = std::min(range_end + 1, row_offset + num_rows);
                if (clip_begin < clip_end)
                {
                    ++counters.segments_skipped_dense;
                    memset(data + (clip_begin - row_offset), 1, clip_end - clip_begin);
                    return;
                }
            }
        }

        /// Find range within decoded_values.
        const auto * begin_it = std::lower_bound(decoded_values_ptr, decoded_values_ptr + decoded_count, static_cast<uint32_t>(row_offset));
        const auto * end_it = findRowRangeEnd(begin_it, decoded_values_ptr + decoded_count, row_offset, num_rows);
        size_t begin_idx = static_cast<size_t>(begin_it - decoded_values_ptr);
        size_t end_idx = static_cast<size_t>(end_it - decoded_values_ptr);
        padColumn<PadOp::Or>(data, decoded_values_ptr, row_offset, begin_idx, end_idx);
        return;
    }

    if (info->ranges.empty() || total_segments == 0)
        return;

    for (size_t i = current_segment_idx; i < total_segments; ++i)
    {
        size_t seg_begin = info->ranges[i].begin;
        size_t seg_end = info->ranges[i].end;

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
                    ++counters.segments_skipped_covered;
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
                    ++counters.segments_skipped_dense;
                    memset(data + (clip_begin - row_offset), 1, clip_end - clip_begin);
                    continue;
                }
            }
        }

        /// Decode all blocks in this segment that overlap with [row_offset, row_offset + num_rows).
        for (size_t block_idx = 0; block_idx < block_count; ++block_idx)
        {
            uint32_t block_last = block_last_row_ids[block_idx];
            uint32_t block_first = (block_idx == 0)
                ? static_cast<uint32_t>(seg_begin)
                : (block_last_row_ids[block_idx - 1] + 1);

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
                        ++counters.blocks_skipped_covered;
                        continue;
                    }
                }
            }

            decodeBlock(block_idx);

            const auto * begin_it = std::lower_bound(decoded_values_ptr, decoded_values_ptr + decoded_count, static_cast<uint32_t>(row_offset));
            const auto * end_it = findRowRangeEnd(begin_it, decoded_values_ptr + decoded_count, row_offset, num_rows);
            size_t begin_idx = static_cast<size_t>(begin_it - decoded_values_ptr);
            size_t end_idx = static_cast<size_t>(end_it - decoded_values_ptr);
            padColumn<PadOp::Or>(data, decoded_values_ptr, row_offset, begin_idx, end_idx);
        }
    }
}

void PostingListCursor::linearAnd(UInt8 * data, size_t row_offset, size_t num_rows)
{
    requireRowOffsetRepresentable(row_offset);

    if (is_embedded)
    {
        /// Dense shortcut: if every row in the range is in the posting list,
        /// just increment the entire clipped region without binary search.
        if (!info->ranges.empty())
        {
            size_t range_begin = info->ranges.front().begin;
            size_t range_end = info->ranges.back().end;
            size_t range_span = range_end - range_begin + 1;

            if (info->cardinality == range_span)
            {
                size_t clip_begin = std::max(range_begin, row_offset);
                size_t clip_end = std::min(range_end + 1, row_offset + num_rows);

                if (clip_begin < clip_end)
                {
                    ++counters.and_segments_skipped_dense;
                    UInt8 * out = data + (clip_begin - row_offset);
                    size_t count = clip_end - clip_begin;
                    for (size_t i = 0; i < count; ++i)
                        ++out[i];
                    return;
                }
            }
        }

        const auto * begin_it = std::lower_bound(decoded_values_ptr, decoded_values_ptr + decoded_count, static_cast<uint32_t>(row_offset));
        const auto * end_it = findRowRangeEnd(begin_it, decoded_values_ptr + decoded_count, row_offset, num_rows);
        size_t begin_idx = static_cast<size_t>(begin_it - decoded_values_ptr);
        size_t end_idx = static_cast<size_t>(end_it - decoded_values_ptr);
        padColumn<PadOp::And>(data, decoded_values_ptr, row_offset, begin_idx, end_idx);
        return;
    }

    for (size_t i = current_segment_idx; i < total_segments; ++i)
    {
        size_t seg_begin = info->ranges[i].begin;
        size_t seg_end = info->ranges[i].end;

        if (row_offset > seg_end)
            continue;

        if (row_offset + num_rows <= seg_begin)
            break;

        /// Level 2a: segment-level "all-zeros" skip.
        /// If the output region for this segment is already all-zeros, incrementing
        /// will not help — the final pass requires count == n, so skip entirely.
        {
            size_t clip_begin = std::max(seg_begin, row_offset);
            size_t clip_end = std::min(seg_end + 1, row_offset + num_rows);

            if (clip_begin < clip_end)
            {
                size_t clip_off = clip_begin - row_offset;
                size_t clip_count = clip_end - clip_begin;

                if (memoryIsZero(data + clip_off, 0, clip_count))
                {
                    ++counters.and_segments_skipped_zero;
                    continue;
                }
            }
        }

        /// Skip re-preparing the segment if it is already loaded.
        if (i != current_segment_idx || !has_prepared_first_segment)
            prepareSegment(i);

        /// Level 1: dense segment shortcut.
        /// If every row in the segment range has a posting, increment the entire clipped range.
        {
            size_t range_span = seg_end - seg_begin + 1;
            if (segment_doc_count == range_span)
            {
                size_t clip_begin = std::max(seg_begin, row_offset);
                size_t clip_end = std::min(seg_end + 1, row_offset + num_rows);

                if (clip_begin < clip_end)
                {
                    ++counters.and_segments_skipped_dense;
                    UInt8 * out = data + (clip_begin - row_offset);
                    size_t count = clip_end - clip_begin;
                    for (size_t j = 0; j < count; ++j)
                        ++out[j];
                    continue;
                }
            }
        }

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

            /// Level 2b: block-level "all-zeros" skip.
            {
                size_t blk_clip_begin = std::max(static_cast<size_t>(block_first), row_offset);
                size_t blk_clip_end = std::min(static_cast<size_t>(block_last) + 1, row_offset + num_rows);

                if (blk_clip_begin < blk_clip_end)
                {
                    size_t blk_off = blk_clip_begin - row_offset;
                    size_t blk_cnt = blk_clip_end - blk_clip_begin;

                    if (memoryIsZero(data + blk_off, 0, blk_cnt))
                    {
                        ++counters.and_blocks_skipped_zero;
                        continue;
                    }
                }
            }

            decodeBlock(b);

            const auto * begin_it = std::lower_bound(decoded_values_ptr, decoded_values_ptr + decoded_count, static_cast<uint32_t>(row_offset));
            const auto * end_it = findRowRangeEnd(begin_it, decoded_values_ptr + decoded_count, row_offset, num_rows);
            size_t begin_idx = static_cast<size_t>(begin_it - decoded_values_ptr);
            size_t end_idx = static_cast<size_t>(end_it - decoded_values_ptr);
            padColumn<PadOp::And>(data, decoded_values_ptr, row_offset, begin_idx, end_idx);
        }
    }
}

namespace
{

/// Two-cursor intersection. The lagging cursor advances to the leading cursor's doc_id.
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
            c0->advance(v1);
        }
        else
        {
            c1->advance(v0);
        }
    }
}

/// Three-cursor intersection. All cursors behind the maximum advance forward.
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
            if (v0 < max_val) c0->advance(max_val);
            if (v1 < max_val) c1->advance(max_val);
            if (v2 < max_val) c2->advance(max_val);
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
            if (v0 < max_val) c0->advance(max_val);
            if (v1 < max_val) c1->advance(max_val);
            if (v2 < max_val) c2->advance(max_val);
            if (v3 < max_val) c3->advance(max_val);
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
            if (vals[i] < min_val)
                min_val = vals[i];
            else if (vals[i] > max_val)
                max_val = vals[i];
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
                    cursors[i]->advance(max_val);
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

            cursors[min_idx]->advance(max_val);
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

#if USE_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(
void finalizeCounters(UInt8 * out, size_t num_rows, UInt8 target)
{
    __m256i t = _mm256_set1_epi8(static_cast<char>(target));
    __m256i one = _mm256_set1_epi8(1);
    size_t i = 0;
    for (; i + 32 <= num_rows; i += 32)
    {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(out + i));
        __m256i eq = _mm256_cmpeq_epi8(v, t);
        __m256i result = _mm256_and_si256(eq, one);
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(out + i), result);
    }
    for (; i < num_rows; ++i)
        out[i] = (out[i] == target);
}
) /// DECLARE_X86_64_V3_SPECIFIC_CODE
#endif

void finalizeCounters(UInt8 * out, size_t num_rows, UInt8 target)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::finalizeCounters(out, num_rows, target);
        return;
    }
#endif

    for (size_t i = 0; i < num_rows; ++i)
        out[i] = (out[i] == target);
}

void intersectBruteForce(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t num_rows)
{
    cursors[0]->linearOr(out, row_offset, num_rows);

    for (size_t i = 1; i < cursors.size(); ++i)
        cursors[i]->linearAnd(out, row_offset, num_rows);

    size_t n = cursors.size();
    if (n > 1)
    {
        UInt8 n8 = static_cast<UInt8>(n);
        finalizeCounters(out, num_rows, n8);
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
    requireRowOffsetRepresentable(row_offset);

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
    requireRowOffsetRepresentable(row_offset);

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
        cursors[i]->advance(static_cast<uint32_t>(row_offset));
        if (!cursors[i]->valid() || cursors[i]->value() >= end)
            return;
    }

    ProfileEvents::increment(ProfileEvents::TextIndexLazyLeapfrogIntersections);
    intersectLeapfrog(out, cursors, row_offset, end);
}

}
