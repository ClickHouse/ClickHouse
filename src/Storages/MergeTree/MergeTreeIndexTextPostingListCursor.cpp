#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/ProfileEvents.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/TargetSpecific.h>
#include <config.h>
#include <algorithm>
#include <cstring>
#include <numeric>

namespace ProfileEvents
{
    extern const Event TextIndexLazyPackedBlocksDecoded;
    extern const Event TextIndexLazyAdvanceCount;
    extern const Event TextIndexLazySegmentsPrepared;
    extern const Event TextIndexLazySegmentsBuilt;
    extern const Event TextIndexLazyBruteForceIntersections;
    extern const Event TextIndexLazyLeapfrogIntersections;
    extern const Event TextIndexLazyBruteForceEarlyExits;
    extern const Event TextIndexLazySegmentsSkippedDense;
    extern const Event TextIndexLazySegmentsSkippedResolved;
    extern const Event TextIndexLazyBlocksSkippedResolved;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

/// Posting-list doc IDs are 32-bit, so `row_offset > UInt32::max` cannot legitimately
/// occur and would underflow `out[v - row_offset]` indexing in `padColumn` / leapfrog
/// writers (and the direct-fill path in `MergeTreeReaderTextIndex`). Throw rather than
/// silently emit a zero filter and drop matches.
void requireRowOffsetRepresentable(size_t row_offset)
{
    if (row_offset > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Posting-list cursor doesn't support row_offset larger than UINT32_MAX, got {}", row_offset);
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

PostingListCursor::PostingListCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_, TextIndexPostingsCache * postings_cache_, const String & index_id_for_cache_)
    : stream(&stream_)
    , info(&info_)
    , postings_cache(postings_cache_)
    , index_id_for_cache(index_id_for_cache_)
    , total_segments(info_.offsets.size())
    , density_val(computeDensity(info_))
{
}

PostingListCursor::PostingListCursor(FlatPostingsPtr shared_values_)
    : is_embedded(true)
    , shared_values(std::move(shared_values_))
{
    if (!shared_values || shared_values->empty())
    {
        is_valid = false;
        return;
    }

    chassert(std::ranges::is_sorted(*shared_values));
    /// Iterate directly over the shared, immutable, pre-flattened array.
    decoded_count = shared_values->size();
    decoded_values_ptr = shared_values->data();

    double span = static_cast<double>(shared_values->back()) - static_cast<double>(shared_values->front()) + 1.0;
    density_val = span > 0.0 ? static_cast<double>(decoded_count) / span : 0.0;
}

UInt32 PostingListCursor::cardinality() const
{
    return is_embedded ? static_cast<UInt32>(decoded_count) : info->cardinality;
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
    if (counters.segments_skipped_resolved)
        ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsSkippedResolved, counters.segments_skipped_resolved);
    if (counters.blocks_skipped_resolved)
        ProfileEvents::increment(ProfileEvents::TextIndexLazyBlocksSkippedResolved, counters.blocks_skipped_resolved);
}

void PostingListCursor::prepareSegment(size_t segment_idx)
{
    ++counters.segments_prepared;
    current_segment_idx = segment_idx;
    chassert(!is_embedded);
    chassert(segment_idx < total_segments);

    /// Obtain the decoded segment, sharing it via the cache (keyed by index id + segment offset) when one
    /// is configured so it is parsed once; either way it is shared_ptr-held for the cursor's lifetime.
    if (!postings_cache)
    {
        current_segment = std::make_shared<PostingListSegment>(buildPostingSegment(segment_idx));
    }
    else
    {
        UInt64 segment_file_offset = info->offsets[segment_idx];
        auto key = TextIndexPostingsCache::hash(index_id_for_cache, segment_file_offset, static_cast<UInt8>(TextIndexPostingsCacheKind::Segment));

        auto cell = postings_cache->getOrSet(key, [&]
        {
            return std::make_shared<TextIndexPostingsCacheCell>(std::make_shared<PostingListSegment>(buildPostingSegment(segment_idx)));
        });

        current_segment = std::get<PostingListSegmentPtr>(cell->value);
    }

    last_decoded_doc_id = current_segment->first_row_id;
    current_block = 0;
    decoded_count = 0;
    index = 0;
}

PostingListSegment PostingListCursor::buildPostingSegment(size_t segment_idx)
{
    ProfileEvents::increment(ProfileEvents::TextIndexLazySegmentsBuilt);

    chassert(segment_idx < total_segments);
    PostingListSegment segment;

    UInt64 segment_file_offset = info->offsets[segment_idx];

    /// Seek to segment start and read the header.
    stream->seekToMark({segment_file_offset, 0});
    auto * data_buffer = stream->getDataBuffer();

    /// Read the segment header.
    UInt64 codec_type = 0;
    readVarUInt(codec_type, *data_buffer);

    if (!isValidPostingListBlockCodecType(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy cursor: unknown posting list block codec type {}", codec_type);

    segment.codec_type = static_cast<IPostingListCodec::Type>(codec_type);

    UInt64 payload_bytes = 0;
    readVarUInt(payload_bytes, *data_buffer);
    UInt64 seg_cardinality = 0;
    readVarUInt(seg_cardinality, *data_buffer);
    UInt64 first_row_id = 0;
    readVarUInt(first_row_id, *data_buffer);

    segment.doc_count = requireUInt32(seg_cardinality, "seg_cardinality");
    segment.first_row_id = requireUInt32(first_row_id, "first_row_id");

    const auto & segment_range = info->ranges[segment_idx];

    if (segment_range.begin > segment_range.end)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: segment row range has begin {} > end {} for segment {}",
            segment_range.begin, segment_range.end, segment_idx);
    }

    const UInt64 range_span = static_cast<UInt64>(segment_range.end) - static_cast<UInt64>(segment_range.begin) + 1;

    if (segment.doc_count > range_span)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: segment cardinality {} exceeds segment row range span {} for segment [{}, {}]",
            segment.doc_count, range_span, segment_range.begin, segment_range.end);
    }

    /// The row range of a segment in the dictionary is its first and its last row id (see `checkSegmentRowRange`
    /// on the eager path). `advance` and the skip heuristics choose segments by the range, while `decodeBlock`
    /// uses `first_row_id` as the delta base of the first block, so the two must agree.
    if (segment.first_row_id != segment_range.begin)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: segment {} starts at row id {} while its row range is [{}, {}]",
            segment_idx, segment.first_row_id, segment_range.begin, segment_range.end);
    }

    /// Create the per-block codec for this segment's codec type now and reuse it for decoding (see
    /// `decodeBlock`). It owns the codec-specific per-block worst-case size, so the cursor can bound
    /// `payload_bytes` against corrupted metadata without naming a concrete codec.
    if (!block_codec || block_codec->type() != segment.codec_type)
        block_codec = createPostingListBlockCodec(segment.codec_type);

    /// Cap `payload_bytes` before resizing so corrupted metadata can't force a huge allocation.
    const UInt64 max_blocks_count = (static_cast<UInt64>(segment.doc_count) + IPostingListBlockCodec::BLOCK_SIZE - 1) / IPostingListBlockCodec::BLOCK_SIZE;
    const UInt64 per_block_cap = block_codec->maxBlockBytes();
    const UInt64 max_payload_bytes = max_blocks_count * per_block_cap;

    if (payload_bytes > max_payload_bytes)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: payload_bytes {} exceeds upper bound {} "
            "for segment with {} documents",
            payload_bytes, max_payload_bytes, segment.doc_count);
    }

    /// Bulk-read the entire payload into memory.
    segment.payload_buffer.resize(payload_bytes);
    data_buffer->readStrict(reinterpret_cast<char *>(segment.payload_buffer.data()), payload_bytes);

    if (!(info->header & PostingsSerialization::Flags::HasBlockIndex))
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: per-segment block index is missing "
            "(HasBlockIndex flag not set in posting list header)");
    }

    /// Index Section follows immediately after the payload in the .pst stream.
    /// No additional seek needed — just continue reading.
    UInt64 num_blocks = 0;
    readVarUInt(num_blocks, *data_buffer);

    if (num_blocks == 0)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Posting list number of blocks is 0 for segment with {} documents",
            segment.doc_count);
    }

    if (num_blocks != max_blocks_count)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: number of blocks {} does not match "
            "the expected {} for segment with {} documents",
            num_blocks, max_blocks_count, segment.doc_count);
    }

    segment.block_last_row_ids.resize(num_blocks);
    segment.block_offsets.resize(num_blocks);

    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v = 0;
        readVarUInt(v, *data_buffer);
        segment.block_last_row_ids[i] = requireUInt32(v, "block_last_row_id");

        if (i > 0 && segment.block_last_row_ids[i] <= segment.block_last_row_ids[i - 1])
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data in lazy posting list cursor: block_last_row_ids not strictly "
                "monotonic at block {}: previous = {}, current = {}",
                i, segment.block_last_row_ids[i - 1], segment.block_last_row_ids[i]);
        }
    }

    /// The block index must end at the last row id of the segment range: `advance` and the skip heuristics
    /// look up blocks by `block_last_row_ids`, and `decodeBlock` uses them as delta bases of the next blocks.
    /// Together with the strict monotonicity above this keeps every block boundary inside the segment range.
    if (segment.block_last_row_ids.front() < segment.first_row_id)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: the first block of segment {} ends at row id {} before the segment starts at row id {}",
            segment_idx, segment.block_last_row_ids.front(), segment.first_row_id);
    }

    if (segment.block_last_row_ids.back() != segment_range.end)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: segment {} ends at row id {} while its row range is [{}, {}]",
            segment_idx, segment.block_last_row_ids.back(), segment_range.begin, segment_range.end);
    }

    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v = 0;
        readVarUInt(v, *data_buffer);
        segment.block_offsets[i] = v;

        if (segment.block_offsets[i] >= payload_bytes)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data in lazy posting list cursor: block_offsets[{}] = {} is outside payload of {} bytes",
                i, segment.block_offsets[i], payload_bytes);
        }

        if (i > 0 && segment.block_offsets[i] <= segment.block_offsets[static_cast<ssize_t>(i) - 1])
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data in lazy posting list cursor: block_offsets not strictly monotonic at block {}: previous = {}, current = {}",
                i, segment.block_offsets[static_cast<ssize_t>(i) - 1], segment.block_offsets[i]);
        }
    }

    segment.block_count = num_blocks;
    segment.tail_size = segment.doc_count % IPostingListBlockCodec::BLOCK_SIZE;
    return segment;
}

void PostingListCursor::decodeBlock(size_t block_idx)
{
    ++counters.blocks_decoded;

    const auto & segment = *current_segment;
    chassert(block_idx < segment.block_count);
    current_block = block_idx;

    /// Determine the base doc_id for delta decoding.
    /// For the first block, use the segment's first_row_id (already in last_decoded_doc_id
    /// if this is the first block decoded in sequence). For arbitrary seeks, we need to
    /// figure out the correct base from the previous block's last_row_id.
    if (block_idx == 0)
    {
        /// First block decodes from the segment's first row id.
        last_decoded_doc_id = segment.first_row_id;
    }
    else
    {
        last_decoded_doc_id = segment.block_last_row_ids[block_idx - 1];
    }

    /// Determine block element count: a full block, or `tail_size` for the last block.
    size_t count = IPostingListBlockCodec::BLOCK_SIZE;
    if (block_idx == segment.block_count - 1 && segment.tail_size > 0)
        count = segment.tail_size;

    /// Read from payload buffer at the relative offset.
    size_t payload_offset = static_cast<size_t>(segment.block_offsets[block_idx]);

    if (payload_offset >= segment.payload_buffer.size())
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data: block offset {} is out of payload bounds {}",
            payload_offset, segment.payload_buffer.size());
    }

    const size_t next_offset = (block_idx + 1 < segment.block_count)
        ? static_cast<size_t>(segment.block_offsets[block_idx + 1])
        : segment.payload_buffer.size();

    const size_t block_size = next_offset - payload_offset;

    std::span<const std::byte> block_data(
        reinterpret_cast<const std::byte *>(segment.payload_buffer.data() + payload_offset),
        block_size);

    if (block_data.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: empty block at index {}", block_idx);

    std::span<uint32_t> out_span(decoded_values, count);

    /// Lazily create the per-block payload codec for this segment's codec type and reuse it across blocks.
    /// A posting list is written with a single codec, so this is effectively created once per cursor.
    if (!block_codec || block_codec->type() != segment.codec_type)
        block_codec = createPostingListBlockCodec(segment.codec_type);

    /// The block span comes from the Index Section offsets and must be consumed in full.
    const size_t expected_bytes = block_data.size();
    const size_t consumed_bytes = block_codec->decodeBlock(block_data, count, out_span);

    if (consumed_bytes != expected_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: block {} consumed {} bytes but its "
            "Index Section span is {} bytes",
            block_idx, consumed_bytes, expected_bytes);

    /// Restore absolute row ids from deltas directly in decoded_values.
    std::inclusive_scan(decoded_values, decoded_values + count, decoded_values, std::plus<uint32_t>{}, last_decoded_doc_id);
    last_decoded_doc_id = count > 0 ? decoded_values[count - 1] : last_decoded_doc_id;

    /// The decoded block must end at the row id the Index Section claims for it: `advance` and the skip
    /// heuristics position by `block_last_row_ids`, and the next block decodes its deltas from it.
    if (last_decoded_doc_id != segment.block_last_row_ids[block_idx])
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in lazy posting list cursor: block {} ends at row id {} while its Index Section entry is {}",
            block_idx, last_decoded_doc_id, segment.block_last_row_ids[block_idx]);
    }

    decoded_count = count;
    index = 0;
}

/// Lower bound by galloping search: cheap when the answer is close to `first`, which is the common
/// case for a cursor advancing over dense posting lists, and O(log n) otherwise.
static const uint32_t * gallopingLowerBound(const uint32_t * first, const uint32_t * last, uint32_t target)
{
    const size_t size = static_cast<size_t>(last - first);
    if (size == 0 || *first >= target)
        return first;

    size_t bound = 1;
    while (bound < size && first[bound] < target)
        bound *= 2;

    /// first[bound / 2] < target, so the answer is in (bound / 2, bound], clamped to the range.
    return std::lower_bound(first + bound / 2 + 1, first + std::min(bound + 1, size), target);
}

void PostingListCursor::advance(uint32_t target)
{
    ++counters.advance_count;

    if (!is_valid)
        return;

    /// The target lies within the decoded block: the common case of a leapfrog over dense posting lists.
    if (index < decoded_count && target <= decoded_values_ptr[decoded_count - 1])
    {
        const auto * it = gallopingLowerBound(decoded_values_ptr + index, decoded_values_ptr + decoded_count, target);
        index = static_cast<size_t>(it - decoded_values_ptr);
        return;
    }

    /// An embedded list is a single decoded block, and the target is past its last doc_id.
    if (is_embedded)
    {
        is_valid = false;
        return;
    }

    /// Try current segment first.
    if (current_segment && target <= static_cast<uint32_t>(info->ranges[current_segment_idx].end))
    {
        if (advanceImpl(target))
            return;
    }

    /// Binary search across segments.
    size_t start = current_segment ? current_segment_idx + 1 : 0;
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
    /// The target is past the read position, so the search resumes from the current block.
    const auto & block_last_row_ids = current_segment->block_last_row_ids;
    const auto * blocks_end = block_last_row_ids.data() + current_segment->block_count;
    const auto * it = gallopingLowerBound(block_last_row_ids.data() + current_block, blocks_end, target);

    if (it == blocks_end)
        return false;

    size_t j = static_cast<size_t>(it - block_last_row_ids.data());

    if (j != current_block || decoded_count == 0)
        decodeBlock(j);

    /// Search within the decoded packed block.
    const auto * found_it = gallopingLowerBound(decoded_values_ptr, decoded_values_ptr + decoded_count, target);
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

    if (++index < decoded_count)
        return;

    if (is_embedded)
    {
        is_valid = false;
        return;
    }

    ++current_block;
    chassert(current_segment);

    if (current_block < current_segment->block_count)
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

/// Scatter-write into `out` for doc_ids in values[begin..length).
/// PadOp::Or assigns 1, PadOp::And increments the counter.
namespace
{

/// Iterator to the first row_id >= row_offset + num_rows. Returns `end` directly when the
/// exclusive bound exceeds UInt32::max — saturating would drop a match at the boundary.
inline const uint32_t * findRowRangeEnd(const uint32_t * begin, const uint32_t * end, size_t row_offset, size_t num_rows)
{
    size_t exclusive_end = row_offset + num_rows;
    if (exclusive_end > std::numeric_limits<uint32_t>::max() || begin == end || *(end - 1) < exclusive_end)
        return end;

    return gallopingLowerBound(begin, end, static_cast<uint32_t>(exclusive_end));
}

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

/// Apply the padding op to a contiguous, fully-dense output region (every row in it is a match):
///   PadOp::Or  — set every byte to 1;
///   PadOp::And — increment every counter.
template <PadOp op>
inline void padDenseRange(UInt8 * __restrict out, size_t count)
{
    if constexpr (op == PadOp::Or)
        memset(out, 1, count);
    else
        for (size_t i = 0; i < count; ++i)
            ++out[i];
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

/// Whether the output region [data, data + count) needs no contribution from this posting list
/// and the whole segment/block can be skipped:
///   PadOp::Or  — already all-ones (every row set, nothing left to set);
///   PadOp::And — already all-zeros (no row can ever reach count == n, so incrementing is pointless).
template <PadOp op>
inline bool canSkipRegion(const UInt8 * data, size_t count)
{
    if constexpr (op == PadOp::Or)
        return hasNoZeros(data, count);
    else
        return memoryIsZero(data, 0, count);
}

} // anonymous namespace

PostingsApplyWindow PostingListCursor::linearOr(UInt8 * data, size_t row_offset, size_t num_rows)
{
    requireRowOffsetRepresentable(row_offset);

    if (is_embedded)
        return linearEmbedded<PadOp::Or>(data, row_offset, num_rows);

    return linearSegments<PadOp::Or>(data, row_offset, num_rows);
}

PostingsApplyWindow PostingListCursor::linearAnd(UInt8 * data, size_t row_offset, size_t num_rows)
{
    requireRowOffsetRepresentable(row_offset);

    if (is_embedded)
        return linearEmbedded<PadOp::And>(data, row_offset, num_rows);

    return linearSegments<PadOp::And>(data, row_offset, num_rows);
}

template <PadOp op>
PostingsApplyWindow PostingListCursor::linearSegments(UInt8 * data, size_t row_offset, size_t num_rows)
{
    PostingsApplyWindow window;

    if (info->ranges.empty() || total_segments == 0)
        return window;

    for (size_t i = current_segment_idx; i < total_segments; ++i)
    {
        size_t seg_begin = info->ranges[i].begin;
        size_t seg_end = info->ranges[i].end;

        if (row_offset > seg_end)
            continue;

        if (row_offset + num_rows <= seg_begin)
            break;

        /// Level 2a: segment-level skip. If the output region for this segment is already resolved
        /// (all-ones for OR, all-zeros for AND), skip entirely — saving the I/O cost of prepareSegment.
        {
            size_t clip_begin = std::max(seg_begin, row_offset);
            size_t clip_end = std::min(seg_end + 1, row_offset + num_rows);

            if (clip_begin < clip_end)
            {
                size_t clip_off = clip_begin - row_offset;
                size_t clip_count = clip_end - clip_begin;

                if (canSkipRegion<op>(data + clip_off, clip_count))
                {
                    ++counters.segments_skipped_resolved;
                    continue;
                }
            }
        }

        /// Skip re-preparing the segment if it is already loaded.
        if (i != current_segment_idx || !current_segment)
            prepareSegment(i);

        /// Level 1: dense segment shortcut.
        /// If every row in the segment range has a posting, pad the whole clipped range at once
        /// instead of decoding blocks.
        {
            size_t range_span = seg_end - seg_begin + 1;
            if (current_segment->doc_count == range_span)
            {
                size_t clip_begin = std::max(seg_begin, row_offset);
                size_t clip_end = std::min(seg_end + 1, row_offset + num_rows);

                if (clip_begin < clip_end)
                {
                    ++counters.segments_skipped_dense;
                    padDenseRange<op>(data + (clip_begin - row_offset), clip_end - clip_begin);
                    window.extend(clip_begin, clip_end);
                    continue;
                }
            }
        }

        /// Decode all blocks in this segment that overlap with [row_offset, row_offset + num_rows).
        /// The windows come in ascending order, so the search for the first block resumes from the last
        /// decoded block when that block still lies before the window, instead of bisecting the whole segment.
        const auto & block_last_row_ids = current_segment->block_last_row_ids;
        const size_t block_count = current_segment->block_count;
        const size_t block_begin = (current_block > 0 && block_last_row_ids[current_block - 1] < row_offset) ? current_block : 0;

        const auto * first_block_it = gallopingLowerBound(
            block_last_row_ids.data() + block_begin,
            block_last_row_ids.data() + block_count,
            static_cast<uint32_t>(row_offset));

        const size_t first_block_idx = static_cast<size_t>(first_block_it - block_last_row_ids.data());

        for (size_t block_idx = first_block_idx; block_idx < block_count; ++block_idx)
        {
            if (block_idx > 0 && block_last_row_ids[block_idx - 1] == std::numeric_limits<uint32_t>::max())
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted data in lazy posting list cursor: previous block_last_row_id is UInt32::max "
                    "at block {}, computing block_first would overflow", block_idx);
            }

            uint32_t block_first = (block_idx == 0) ? static_cast<uint32_t>(seg_begin) : (block_last_row_ids[block_idx - 1] + 1);
            uint32_t block_last = block_last_row_ids[block_idx];
            chassert(block_last >= row_offset);

            if (block_first >= row_offset + num_rows)
                break;

            /// Level 2b: block-level skip (same resolved-region test as Level 2a, per block).
            {
                size_t blk_clip_begin = std::max(static_cast<size_t>(block_first), row_offset);
                size_t blk_clip_end = std::min(static_cast<size_t>(block_last) + 1, row_offset + num_rows);

                if (blk_clip_begin < blk_clip_end)
                {
                    size_t blk_off = blk_clip_begin - row_offset;
                    size_t blk_cnt = blk_clip_end - blk_clip_begin;

                    if (canSkipRegion<op>(data + blk_off, blk_cnt))
                    {
                        ++counters.blocks_skipped_resolved;
                        continue;
                    }
                }
            }

            /// A block that straddles two consecutive windows is still decoded from the previous call
            if (block_idx != current_block || decoded_count == 0)
                decodeBlock(block_idx);

            chassert(index <= decoded_count);
            const size_t value_begin = (index > 0 && decoded_values_ptr[index - 1] < row_offset) ? index : 0;

            const auto * begin_it = gallopingLowerBound(
                decoded_values_ptr + value_begin,
                decoded_values_ptr + decoded_count,
                static_cast<uint32_t>(row_offset));

            const auto * end_it = findRowRangeEnd(begin_it, decoded_values_ptr + decoded_count, row_offset, num_rows);
            size_t begin_idx = static_cast<size_t>(begin_it - decoded_values_ptr);
            size_t end_idx = static_cast<size_t>(end_it - decoded_values_ptr);
            index = end_idx;

            /// No doc_ids of this block fall into the window. The block has a doc_id >= row_offset (`block_last`),
            /// so that doc_id is past the window, and so is everything in the following blocks and segments.
            if (begin_idx == end_idx)
                return window;

            padColumn<op>(data, decoded_values_ptr, row_offset, begin_idx, end_idx);
            window.extend(decoded_values_ptr[begin_idx], static_cast<size_t>(decoded_values_ptr[end_idx - 1]) + 1);
        }
    }

    return window;
}

template <PadOp op>
PostingsApplyWindow PostingListCursor::linearEmbedded(UInt8 * data, size_t row_offset, size_t num_rows)
{
    if (decoded_count == 0)
        return {};

    /// Dense shortcut: if every row in the range is in the posting list,
    /// pad the entire clipped region at once without binary search.
    chassert(shared_values != nullptr);
    size_t embedded_begin = static_cast<UInt64>(shared_values->front());
    size_t embedded_end = static_cast<UInt64>(shared_values->back());
    size_t range_span = embedded_end - embedded_begin + 1;

    if (decoded_count == range_span)
    {
        size_t clip_begin = std::max(embedded_begin, row_offset);
        size_t clip_end = std::min(embedded_end + 1, row_offset + num_rows);

        if (clip_begin < clip_end)
        {
            ++counters.segments_skipped_dense;
            padDenseRange<op>(data + (clip_begin - row_offset), clip_end - clip_begin);
            return {clip_begin, clip_end};
        }
    }

    /// The windows come in ascending order: resume the search from the read position when it still lies
    /// before the window, and leave it at the first doc_id past the window for the next scan.
    chassert(index <= decoded_count);
    const size_t search_from = (index > 0 && decoded_values_ptr[index - 1] < row_offset) ? index : 0;

    const auto * begin_it = gallopingLowerBound(decoded_values_ptr + search_from, decoded_values_ptr + decoded_count, static_cast<uint32_t>(row_offset));
    const auto * end_it = findRowRangeEnd(begin_it, decoded_values_ptr + decoded_count, row_offset, num_rows);
    size_t begin_idx = static_cast<size_t>(begin_it - decoded_values_ptr);
    size_t end_idx = static_cast<size_t>(end_it - decoded_values_ptr);
    index = end_idx;

    if (begin_idx == end_idx)
        return {};

    padColumn<op>(data, decoded_values_ptr, row_offset, begin_idx, end_idx);
    return {decoded_values_ptr[begin_idx], static_cast<size_t>(decoded_values_ptr[end_idx - 1]) + 1};
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


/// Brute-force intersection via bitmap counting. The cursors are sorted by ascending cardinality.
/// First cursor sets bits (linearOr), remaining cursors increment counters (linearAnd),
/// then a final pass converts count == n into 1, everything else into 0.
/// `out` must be passed with all-zero bytes.
void intersectBruteForce(UInt8 * out, const std::vector<PostingListCursorPtr> & cursors, size_t row_offset, size_t num_rows)
{
    const PostingsApplyWindow first = cursors[0]->linearOr(out, row_offset, num_rows);

    if (first.empty())
    {
        ProfileEvents::increment(ProfileEvents::TextIndexLazyBruteForceEarlyExits);
        return;
    }

    PostingsApplyWindow window = first;

    for (size_t i = 1; i < cursors.size(); ++i)
    {
        PostingsApplyWindow written = cursors[i]->linearAnd(out + (window.begin - row_offset), window.begin, window.end - window.begin);

        if (written.empty())
        {
            ProfileEvents::increment(ProfileEvents::TextIndexLazyBruteForceEarlyExits);
            memset(out + (first.begin - row_offset), 0, first.end - first.begin);
            return;
        }

        chassert(written.begin >= window.begin && written.end <= window.end);
        window = written;
    }

    size_t n = cursors.size();
    if (n > 1)
    {
        chassert(n < 256);
        finalizeCounters(out + (first.begin - row_offset), first.end - first.begin, static_cast<UInt8>(n));
    }
}

} // anonymous namespace

void lazyUnionPostingLists(
    IColumn & column,
    const std::vector<PostingListCursorPtr> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows)
{
    requireRowOffsetRepresentable(row_offset);

    auto & data = assert_cast<DB::ColumnUInt8 &>(column).getData();
    UInt8 * out = data.data() + column_offset;

    /// Sort by descending density so the densest cursor fills the output buffer first.
    auto sorted_cursors = cursors;
    std::ranges::stable_sort(sorted_cursors,
        [](const PostingListCursorPtr & a, const PostingListCursorPtr & b)
        { return a->density() > b->density(); });

    for (auto & cursor : sorted_cursors)
        cursor->linearOr(out, row_offset, num_rows);
}

void lazyIntersectPostingLists(
    IColumn & column,
    const std::vector<PostingListCursorPtr> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows,
    TextIndexPostingsIntersectionAlgorithm algorithm)
{
    requireRowOffsetRepresentable(row_offset);

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

    bool use_brute_force = algorithm == TextIndexPostingsIntersectionAlgorithm::BruteForce;

    /// `Auto` picks leapfrog only where it can skip whole packed blocks of the densest list.
    /// A block of that list spans about `BLOCK_SIZE / max_density` rows.
    /// Over that span, the sparsest list has about `min_density * BLOCK_SIZE / max_density` postings.
    /// Once that reaches one, leapfrog decodes every block anyway
    /// and only adds a search per posting on top of the brute-force counting pass.
    if (algorithm == TextIndexPostingsIntersectionAlgorithm::Auto)
    {
        double min_density = std::numeric_limits<double>::max();
        double max_density = 0.0;

        for (size_t i = 0; i < n; ++i)
        {
            min_density = std::min(min_density, cursors[i]->density());
            max_density = std::max(max_density, cursors[i]->density());
        }

        use_brute_force = min_density * static_cast<double>(IPostingListBlockCodec::BLOCK_SIZE) >= max_density;
    }

    /// Sort cursors by ascending cardinality. The sparsest cursor leads the intersection.
    auto sorted_cursors = cursors;
    std::ranges::sort(sorted_cursors,
        [](const PostingListCursorPtr & a, const PostingListCursorPtr & b)
        { return a->cardinality() < b->cardinality(); });

    /// n < 256: brute-force uses UInt8 counters per row — would overflow with 256+ cursors.
    if (n < 256 && use_brute_force)
    {
        ProfileEvents::increment(ProfileEvents::TextIndexLazyBruteForceIntersections);
        intersectBruteForce(out, sorted_cursors, row_offset, num_rows);
        return;
    }

    for (size_t i = 0; i < n; ++i)
    {
        sorted_cursors[i]->advance(static_cast<uint32_t>(row_offset));
        if (!sorted_cursors[i]->valid() || sorted_cursors[i]->value() >= end)
            return;
    }

    ProfileEvents::increment(ProfileEvents::TextIndexLazyLeapfrogIntersections);
    intersectLeapfrog(out, sorted_cursors, row_offset, end);
}

}
