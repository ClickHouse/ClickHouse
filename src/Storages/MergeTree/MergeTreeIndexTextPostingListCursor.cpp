#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/ProjectionIndex/LengthPrefixedInt.h>
#include <Common/ProfileEvents.h>
#include <algorithm>
#include <cstring>

#include <turbopfor.h>

namespace ProfileEvents
{
    extern const Event TextIndexLazyPackedBlocksDecoded;
    extern const Event TextIndexLazyPackedBlocksSkipped;
    extern const Event TextIndexLazySeekCount;
    extern const Event TextIndexLazyLargeBlocksPrepared;
    extern const Event TextIndexLazyBruteForceIntersections;
    extern const Event TextIndexLazyLeapfrogIntersections;
}

namespace DB
{

namespace
{

/// Convenience alias for the shared prefix-varint codec.
inline void readPrefixVarUInt32(UInt32 & x, ReadBuffer & istr)
{
    LengthPrefixedInt::readUInt32(x, istr);
}

} // anonymous namespace

PostingListCursor::PostingListCursor(LargePostingListReaderStreamPtr owned_stream_, const TokenPostingsInfo & info_)
    : stream(owned_stream_.get())
    , owned_stream(std::move(owned_stream_))
    , info(info_)
    , total_large_blocks(info.offsets.size())
{
    /// Compute global density once: cardinality / total_range_span.
    if (!info.ranges.empty())
    {
        UInt32 global_begin = static_cast<UInt32>(info.ranges.front().begin);
        UInt32 global_end = static_cast<UInt32>(info.ranges.back().end);
        UInt32 range_span = global_end - global_begin + 1;
        density_val = (range_span > 0) ? static_cast<double>(info.cardinality) / static_cast<double>(range_span) : 1.0;
    }
    else
        density_val = 1.0;

    if (total_large_blocks > 0)
        prepare(0);
    else if (info.embedded_postings)
    {
        /// Embedded postings with no ranges/offsets — call prepare to decode them.
        prepare(0);
    }
    else
        is_valid = false;
}

PostingListCursor::PostingListCursor(const TokenPostingsInfo & info_)
    : PostingListCursor(nullptr, info_)
{
}

UInt32 PostingListCursor::cardinality() const
{
    return info.cardinality;
}

void PostingListCursor::prepare(size_t large_block_idx)
{
    /// Skip if this large block is already loaded.
    if (has_prepared_first_large_block && large_block_idx == current_large_block_idx)
        return;

    ProfileEvents::increment(ProfileEvents::TextIndexLazyLargeBlocksPrepared);
    has_prepared_first_large_block = true;

    decoded_count = 0;

    if (info.embedded_postings)
    {
        /// Embedded posting list: already materialized as a Roaring Bitmap.
        /// Decode all doc_ids into decoded_values in one shot (at most 6 entries).
        chassert(!stream);
        decoded_count = info.embedded_postings->cardinality();
        chassert(decoded_count <= TURBOPFOR_BLOCK_SIZE + 1);
        info.embedded_postings->toUint32Array(decoded_values);
        current_block = 0;
        block_count = 1;
        current_large_block_idx = large_block_idx;
        is_valid = decoded_count > 0;
        is_embedded = true;
        return;
    }

    /// Large posting list: read from .lpst stream, TurboPFor delta-encoded.
    /// Each large block has a corresponding `LargePostingBlockMeta`.
    chassert(stream);
    chassert(large_block_idx < info.offsets.size());
    chassert(large_block_idx < info.ranges.size());

    const auto & block_meta = info.offsets[large_block_idx];
    large_block_doc_count = block_meta.block_doc_count;

    /// Compute packed block structure from the total doc count.
    size_t full_blocks = large_block_doc_count / TURBOPFOR_BLOCK_SIZE;
    tail_size = large_block_doc_count % TURBOPFOR_BLOCK_SIZE;
    block_count = full_blocks + (tail_size > 0 ? 1 : 0);

    current_block = 0;
    current_large_block_idx = large_block_idx;

    /// Seek to the Index Section and read the packed block index.
    chassert(block_meta.index_offset != 0);
    stream->seek(block_meta.index_offset);
    auto & data_buf = *stream->getDataBuffer();

    /// Index Section layout: [num_packed_blocks] [last_doc_ids...] [offsets...]
    UInt32 num_packed_blocks;
    readPrefixVarUInt32(num_packed_blocks, data_buf);
    chassert(num_packed_blocks == block_count);

    packed_block_last_doc_ids.resize(num_packed_blocks);
    packed_block_offsets.resize(num_packed_blocks);

    for (UInt32 j = 0; j < num_packed_blocks; ++j)
        readPrefixVarUInt32(packed_block_last_doc_ids[j], data_buf);
    for (UInt32 j = 0; j < num_packed_blocks; ++j)
        readVarUInt(packed_block_offsets[j], data_buf);

    if (block_count > 0 || large_block_idx == 0)
        is_valid = true;
    else
        is_valid = false;
}

bool PostingListCursor::probeAndDecodePackedBlock(size_t block_idx)
{
    chassert(stream);
    chassert(block_idx < packed_block_offsets.size());

    current_block = block_idx;
    arithmetic_mode = false;

    /// Compute the delta base for TurboPFor decoding.
    uint32_t delta_base;
    if (block_idx == 0)
    {
        delta_base = static_cast<uint32_t>(info.ranges[current_large_block_idx].begin);
        if (current_large_block_idx > 0)
            --delta_base;
    }
    else
        delta_base = packed_block_last_doc_ids[block_idx - 1];

    /// For large block 0, packed block 0: the first_doc_id is prepended from
    /// the dictionary, breaking any arithmetic sequence.  Skip straight to decode.
    bool prepend_first_doc_id = (current_large_block_idx == 0 && block_idx == 0);

    /// Seek to the packed block and read the length-prefix (one seek, shared by
    /// both the probe path and the decode path).
    stream->seek(packed_block_offsets[block_idx]);
    auto & data_buf = *stream->getDataBuffer();

    UInt32 bytes;
    readPrefixVarUInt32(bytes, data_buf);

    UInt32 count = (block_idx + 1 == block_count && tail_size > 0)
                       ? static_cast<UInt32>(tail_size)
                       : static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE);

    /// --- Arithmetic probe (only when first_doc_id prepend is not needed) ---
    if (!prepend_first_doc_id && bytes > 0)
    {
        uint8_t header_byte;
        bool data_in_buffer = (data_buf.available() >= bytes);
        const uint8_t * payload_start;

        if (data_in_buffer)
        {
            header_byte = static_cast<uint8_t>(*data_buf.position());
            payload_start = reinterpret_cast<const uint8_t *>(data_buf.position());
        }
        else
        {
            /// Copy into packed_buffer so we can inspect header + decode from it.
            chassert(bytes <= 512);
            data_buf.readStrict(reinterpret_cast<char *>(stream->packed_buffer), bytes);
            header_byte = stream->packed_buffer[0];
            payload_start = stream->packed_buffer;
        }

        uint32_t constant_value = 0;
        bool is_arithmetic = false;

        if (header_byte == 0x00)
        {
            /// All-zero block: all deltas are 0, step = 1.
            constant_value = 0;
            is_arithmetic = true;
        }
        else if ((header_byte & 0xC0u) == 0xC0u)
        {
            /// Constant block: header = 0xC0 | b.
            unsigned b = header_byte & 0x3Fu;
            unsigned bytes_stored = (b + 7u) / 8u;

            if (bytes_stored == 0)
            {
                constant_value = 0;
                is_arithmetic = true;
            }
            else
            {
                const uint8_t * cv_payload = payload_start + 1;
                constant_value = 0;
                for (unsigned i = 0; i < bytes_stored && i < 4; ++i)
                    constant_value |= static_cast<uint32_t>(cv_payload[i]) << (8u * i);
                if (b < 32u)
                    constant_value &= (1u << b) - 1u;
                is_arithmetic = true;
            }
        }

        if (is_arithmetic)
        {
            ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksSkipped);
            arithmetic_mode = true;
            arithmetic_step = constant_value + 1;
            arithmetic_first = delta_base + arithmetic_step;
            arithmetic_count = count;
            decoded_count = count;
            index = 0;

            return true;
        }

        /// Non-arithmetic block — continue to decode from current data.
        /// The stream has already read the length-prefix.  If data was copied
        /// into packed_buffer (non-inline path), we can decode directly from it.
        /// If data is still in the read buffer, consume it now.
        uint8_t * src_ptr;
        if (data_in_buffer)
        {
            src_ptr = reinterpret_cast<uint8_t *>(data_buf.position());
            data_buf.position() += bytes;
        }
        else
        {
            /// Already copied into packed_buffer above.
            src_ptr = stream->packed_buffer;
        }

        last_decoded_doc_id = delta_base;
        UInt32 actual_count = count;
        decoded_count = actual_count;
        uint32_t * decode_dst = decoded_values;

        if (count == TURBOPFOR_BLOCK_SIZE)
            turbopfor::p4D1Dec128v32(src_ptr, TURBOPFOR_BLOCK_SIZE, decode_dst, last_decoded_doc_id);
        else
            turbopfor::p4D1Dec32(src_ptr, count, decode_dst, last_decoded_doc_id);

        last_decoded_doc_id = decoded_values[actual_count - 1];
        index = 0;

        ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded);
        return false;
    }

    /// --- Direct decode path (prepend_first_doc_id or bytes == 0) ---
    /// Need to read the payload from the current stream position (length-prefix
    /// already consumed above, so no second seek is needed).
    {
        uint8_t * src_ptr;
        if (data_buf.available() >= bytes)
        {
            src_ptr = reinterpret_cast<uint8_t *>(data_buf.position());
            data_buf.position() += bytes;
        }
        else
        {
            chassert(bytes <= 512);
            data_buf.readStrict(reinterpret_cast<char *>(stream->packed_buffer), bytes);
            src_ptr = stream->packed_buffer;
        }

        UInt32 actual_count = prepend_first_doc_id ? count + 1 : count;
        decoded_count = actual_count;
        uint32_t * decode_dst = decoded_values + (prepend_first_doc_id ? 1 : 0);

        last_decoded_doc_id = delta_base;

        if (count == TURBOPFOR_BLOCK_SIZE)
            turbopfor::p4D1Dec128v32(src_ptr, TURBOPFOR_BLOCK_SIZE, decode_dst, last_decoded_doc_id);
        else
            turbopfor::p4D1Dec32(src_ptr, count, decode_dst, last_decoded_doc_id);

        if (prepend_first_doc_id)
            decoded_values[0] = static_cast<uint32_t>(info.ranges[0].begin);

        last_decoded_doc_id = decoded_values[actual_count - 1];
        index = 0;

        ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded);
        return false;
    }
}

void PostingListCursor::seek(uint32_t target)
{
    ProfileEvents::increment(ProfileEvents::TextIndexLazySeekCount);

    /// Fast path: target may fall within the currently loaded large block.
    if (!is_embedded && seekImpl(target))
        return;

    /// Slow path: scan subsequent large blocks whose range covers the target.
    /// For embedded postings, `seekImpl` was skipped above, so start from current index.
    bool found = false;
    size_t start = is_embedded ? current_large_block_idx : current_large_block_idx + 1;
    for (size_t i = start; i < total_large_blocks; ++i)
    {
        const auto & range = info.ranges[i];
        if (range.end >= target)
        {
            prepare(i);
            if (seekImpl(target))
            {
                found = true;
                break;
            }
        }
    }

    is_valid = found;
}

bool PostingListCursor::seekImpl(uint32_t target)
{
    if (is_embedded)
    {
        /// Embedded: all doc_ids are in decoded_values, use binary search.
        auto it = std::lower_bound(decoded_values, decoded_values + decoded_count, target);
        if (it != decoded_values + decoded_count)
        {
            index = static_cast<size_t>(it - decoded_values);
            return true;
        }
        return false;
    }

    /// Check if target falls within the already-decoded/active packed block.
    if (index < decoded_count)
    {
        if (arithmetic_mode)
        {
            /// Arithmetic block: compute position directly.
            uint32_t last_in_block = arithmetic_first + (static_cast<uint32_t>(decoded_count) - 1) * arithmetic_step;
            if (target <= last_in_block)
            {
                if (target <= arithmetic_first)
                {
                    index = 0;
                    return true;
                }
                uint32_t offset_from_first = target - arithmetic_first;
                uint32_t idx = offset_from_first / arithmetic_step;
                uint32_t actual = arithmetic_first + idx * arithmetic_step;
                if (actual < target && idx + 1 < static_cast<uint32_t>(decoded_count))
                {
                    ++idx;
                }
                index = idx;
                return true;
            }
        }
        else
        {
            auto it = std::lower_bound(decoded_values + index, decoded_values + decoded_count, target);
            if (it != decoded_values + decoded_count)
            {
                index = static_cast<size_t>(it - decoded_values);
                return true;
            }
        }
    }

    /// Binary search on packed_block_last_doc_ids: find the first packed block
    /// whose last doc_id >= target.
    auto it = std::lower_bound(packed_block_last_doc_ids.begin(), packed_block_last_doc_ids.end(), target);
    if (it == packed_block_last_doc_ids.end())
        return false;

    size_t j = static_cast<size_t>(it - packed_block_last_doc_ids.begin());

    /// If the target block is already decoded, search it directly without re-decoding.
    /// This avoids redundant TurboPFor decode + stream seek when consecutive seeks
    /// land in the same packed block (common in leapfrog intersection).
    if (j != current_block || decoded_count == 0)
    {
        /// Probe the header and, if non-arithmetic, decode in one pass
        /// (avoids a redundant seek + length-prefix re-read).
        if (probeAndDecodePackedBlock(j))
        {
            /// Direct arithmetic computation — no decompression needed.
            /// arithmetic_mode / arithmetic_step / arithmetic_first / arithmetic_count
            /// are already populated by probeAndDecodePackedBlock.

            if (target <= arithmetic_first)
            {
                index = 0;
            }
            else
            {
                uint32_t offset_from_first = target - arithmetic_first;
                uint32_t idx = offset_from_first / arithmetic_step;
                uint32_t actual = arithmetic_first + idx * arithmetic_step;
                if (actual < target && idx + 1 < arithmetic_count)
                    ++idx;
                index = idx;
            }

            return true;
        }

        /// Non-arithmetic block already decoded by probeAndDecodePackedBlock.
    }

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

    if (index >= decoded_count)
    {
        ++current_block;
        if (current_block < block_count)
        {
            /// More packed blocks in this large block — probe + decode.
            probeAndDecodePackedBlock(current_block);
            return;
        }

        /// Current large block exhausted — advance to next one.
        size_t next_large_block = current_large_block_idx + 1;
        if (next_large_block >= total_large_blocks)
        {
            is_valid = false;
            return;
        }

        prepare(next_large_block);
        probeAndDecodePackedBlock(0);
    }
}

enum class PadOp { Or, And };

/// Scatter-write into `out` for doc_ids in values[begin..length).
/// PadOp::Or assigns 1, PadOp::And increments the counter.
/// 4-wide loop with prefetch for cache-line utilization (~5-10% improvement
/// on dense posting list iteration in real-world benchmarks).
template <PadOp op>
inline void padColumn(UInt8 * __restrict out, const uint32_t * values, size_t row_begin, size_t begin, size_t length)
{
    const uint32_t * p = values + begin;
    const uint32_t * end = values + length;

    if (p >= end)
        return;

    const size_t count = static_cast<size_t>(end - p);
    const uint32_t * loop_end = p + (count / 4) * 4;

    for (; p < loop_end; p += 4)
    {
        __builtin_prefetch(p + 16, 0, 3);
        if (p + 8 < end)
            __builtin_prefetch(&out[p[8] - row_begin], 1, 0);

        if constexpr (op == PadOp::Or)
        {
            out[p[0] - row_begin] = 1;
            out[p[1] - row_begin] = 1;
            out[p[2] - row_begin] = 1;
            out[p[3] - row_begin] = 1;
        }
        else
        {
            ++out[p[0] - row_begin];
            ++out[p[1] - row_begin];
            ++out[p[2] - row_begin];
            ++out[p[3] - row_begin];
        }
    }

    switch (end - p)
    {
        case 3:
            if constexpr (op == PadOp::Or)
                out[p[2] - row_begin] = 1;
            else
                ++out[p[2] - row_begin];
            [[fallthrough]];
        case 2:
            if constexpr (op == PadOp::Or)
                out[p[1] - row_begin] = 1;
            else
                ++out[p[1] - row_begin];
            [[fallthrough]];
        case 1:
            if constexpr (op == PadOp::Or)
                out[p[0] - row_begin] = 1;
            else
                ++out[p[0] - row_begin];
            [[fallthrough]];
        default: break;
    }
}

/// Scatter-write arithmetic-sequence doc_ids into `out` for the range [row_begin, row_end].
/// Avoids full TurboPFor decompression for constant-delta and all-zero blocks.
template <PadOp op>
inline void padArithmeticBlock(UInt8 * __restrict out, uint32_t first_doc_id, uint32_t last_doc_id, uint32_t step, size_t row_begin, size_t row_end)
{
    uint32_t start = std::max(first_doc_id, static_cast<uint32_t>(row_begin));
    uint32_t end = std::min(last_doc_id, static_cast<uint32_t>(row_end));

    if (start > end)
        return;

    /// Fast path for step=1 (zero-delta / consecutive doc_ids) — the most common
    /// arithmetic block pattern.  Replaces N scalar stores with a single memset
    /// (Or) or a tight loop that the compiler auto-vectorizes to SIMD paddb (And).
    if (step == 1)
    {
        size_t off = start - row_begin;
        size_t count = end - start + 1;
        if constexpr (op == PadOp::Or)
        {
            memset(out + off, 1, count);
        }
        else
        {
            UInt8 * __restrict dst = out + off;
            for (size_t i = 0; i < count; ++i)
                ++dst[i];
        }
        return;
    }

    /// General case: non-unit step.
    uint32_t offset = start - first_doc_id;
    uint32_t idx = (offset + step - 1) / step;  /// ceil division
    uint32_t doc_id = first_doc_id + idx * step;

    while (doc_id <= end)
    {
        if constexpr (op == PadOp::Or)
            out[doc_id - row_begin] = 1;
        else
            ++out[doc_id - row_begin];
        doc_id += step;
    }
}

void PostingListCursor::linearOrImpl(size_t large_block, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    chassert(large_block < info.ranges.size());

    if (unlikely(is_embedded))
    {
        auto it = std::lower_bound(decoded_values, decoded_values + decoded_count, row_begin);
        if (it == decoded_values + decoded_count)
            return;
        size_t begin_idx = static_cast<size_t>(it - decoded_values);
        auto it_end = std::upper_bound(decoded_values, decoded_values + decoded_count, row_end);
        size_t end_idx = it_end - decoded_values;
        padColumn<PadOp::Or>(out, decoded_values, row_begin, begin_idx, end_idx);
        return;
    }

    /// Use packed_block_last_doc_ids[] to determine the range of packed blocks
    /// that overlap [row_begin, row_end], skipping all blocks outside this range
    /// without any I/O or decoding.
    ///
    /// packed_block_last_doc_ids[j] is the last doc_id in packed block j.
    /// The first doc_id of block j is approximately packed_block_last_doc_ids[j-1]+1
    /// (or info.ranges[large_block].begin for block 0).
    ///
    /// We want the first block whose last_doc_id >= row_begin (it may contain row_begin)
    /// and the last block whose first doc_id could be <= row_end.
    auto blk_start_it = std::lower_bound(packed_block_last_doc_ids.begin(), packed_block_last_doc_ids.end(), static_cast<UInt32>(row_begin));
    if (blk_start_it == packed_block_last_doc_ids.end())
        return;  /// All blocks end before row_begin.
    size_t blk_start = static_cast<size_t>(blk_start_it - packed_block_last_doc_ids.begin());

    /// Find the last block that could contain doc_ids in [row_begin, row_end].
    /// lower_bound(row_end) returns the first block with last_doc_id >= row_end;
    /// that block is the last one we need (it contains or ends at row_end).
    auto blk_end_it = std::lower_bound(packed_block_last_doc_ids.begin(), packed_block_last_doc_ids.end(), static_cast<UInt32>(row_end));
    size_t blk_end;
    if (blk_end_it == packed_block_last_doc_ids.end())
        blk_end = block_count;  /// row_end exceeds all blocks — process them all.
    else
        blk_end = static_cast<size_t>(blk_end_it - packed_block_last_doc_ids.begin()) + 1;  /// +1 for exclusive upper bound.

    for (size_t blk = blk_start; blk < blk_end; ++blk)
    {
        /// Probe the header; if non-arithmetic, decode in one pass.
        if (probeAndDecodePackedBlock(blk))
        {
            /// Scatter arithmetic doc_ids directly — no decompression.
            uint32_t last_doc_id = arithmetic_first + (arithmetic_count - 1) * arithmetic_step;
            padArithmeticBlock<PadOp::Or>(out, arithmetic_first, last_doc_id, arithmetic_step, row_begin, row_end);
            continue;
        }

        /// Non-arithmetic block already decoded by probeAndDecodePackedBlock.

        if (decoded_count == 0)
            continue;

        /// For the first and last blocks in the range, the block may only partially
        /// overlap [row_begin, row_end] — use binary search to clip.
        /// For all middle blocks, every doc_id is guaranteed to be within
        /// [row_begin, row_end], so skip the binary searches entirely.
        bool is_first_block = (blk == blk_start);
        bool is_last_block = (blk + 1 == blk_end);
        bool need_clip = (is_first_block && decoded_values[0] < row_begin)
                      || (is_last_block && decoded_values[decoded_count - 1] > row_end);

        if (need_clip)
        {
            size_t begin_idx = 0;
            size_t end_idx = decoded_count;

            if (is_first_block && decoded_values[0] < row_begin)
            {
                auto it = std::lower_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_begin));
                if (it == decoded_values + decoded_count)
                    continue;
                begin_idx = static_cast<size_t>(it - decoded_values);
            }
            if (is_last_block && decoded_values[decoded_count - 1] > row_end)
            {
                auto it_end = std::upper_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_end));
                end_idx = static_cast<size_t>(it_end - decoded_values);
            }

            padColumn<PadOp::Or>(out, decoded_values, row_begin, begin_idx, end_idx);
        }
        else
        {
            /// Entire block is within [row_begin, row_end] — no clipping needed.
            padColumn<PadOp::Or>(out, decoded_values, row_begin, 0, decoded_count);
        }
    }
}

void PostingListCursor::linearOr(UInt8 * data, size_t row_offset, size_t num_rows)
{
    for (size_t i = current_large_block_idx; i < total_large_blocks; ++i)
    {
        auto large_block = i;
        size_t begin = info.ranges[large_block].begin;
        size_t end = info.ranges[large_block].end;

        if (row_offset > end)
            continue;

        if ((row_offset + num_rows) < begin)
            break;

        end = std::min(end, row_offset + num_rows - 1);
        prepare(large_block);
        linearOrImpl(large_block, data, row_offset, end);
    }
}

void PostingListCursor::linearAndImpl(size_t large_block, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    chassert(large_block < info.ranges.size());

    if (unlikely(is_embedded))
    {
        auto it = std::lower_bound(decoded_values, decoded_values + decoded_count, row_begin);
        if (it == decoded_values + decoded_count)
            return;
        size_t idx = static_cast<size_t>(it - decoded_values);
        auto it_end = std::upper_bound(decoded_values, decoded_values + decoded_count, row_end);
        size_t length = it_end - decoded_values;
        padColumn<PadOp::And>(out, decoded_values, row_begin, idx, length);
        return;
    }

    /// Use packed_block_last_doc_ids[] to skip blocks outside [row_begin, row_end]
    /// without decoding.  Same strategy as linearOrImpl.
    auto blk_start_it = std::lower_bound(packed_block_last_doc_ids.begin(), packed_block_last_doc_ids.end(), static_cast<UInt32>(row_begin));
    if (blk_start_it == packed_block_last_doc_ids.end())
        return;
    size_t blk_start = static_cast<size_t>(blk_start_it - packed_block_last_doc_ids.begin());

    auto blk_end_it = std::lower_bound(packed_block_last_doc_ids.begin(), packed_block_last_doc_ids.end(), static_cast<UInt32>(row_end));
    size_t blk_end;
    if (blk_end_it == packed_block_last_doc_ids.end())
        blk_end = block_count;
    else
        blk_end = static_cast<size_t>(blk_end_it - packed_block_last_doc_ids.begin()) + 1;

    for (size_t blk = blk_start; blk < blk_end; ++blk)
    {
        /// Probe the header; if non-arithmetic, decode in one pass.
        if (probeAndDecodePackedBlock(blk))
        {
            uint32_t last_doc_id = arithmetic_first + (arithmetic_count - 1) * arithmetic_step;
            padArithmeticBlock<PadOp::And>(out, arithmetic_first, last_doc_id, arithmetic_step, row_begin, row_end);
            continue;
        }

        /// Non-arithmetic block already decoded by probeAndDecodePackedBlock.

        if (decoded_count == 0)
            continue;

        bool is_first_block = (blk == blk_start);
        bool is_last_block = (blk + 1 == blk_end);
        bool need_clip = (is_first_block && decoded_values[0] < row_begin)
                      || (is_last_block && decoded_values[decoded_count - 1] > row_end);

        if (need_clip)
        {
            size_t begin_idx = 0;
            size_t end_idx = decoded_count;

            if (is_first_block && decoded_values[0] < row_begin)
            {
                auto it = std::lower_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_begin));
                if (it == decoded_values + decoded_count)
                    continue;
                begin_idx = static_cast<size_t>(it - decoded_values);
            }
            if (is_last_block && decoded_values[decoded_count - 1] > row_end)
            {
                auto it_end = std::upper_bound(decoded_values, decoded_values + decoded_count, static_cast<uint32_t>(row_end));
                end_idx = static_cast<size_t>(it_end - decoded_values);
            }

            padColumn<PadOp::And>(out, decoded_values, row_begin, begin_idx, end_idx);
        }
        else
        {
            padColumn<PadOp::And>(out, decoded_values, row_begin, 0, decoded_count);
        }
    }
}

void PostingListCursor::linearAnd(UInt8 * data, size_t row_offset, size_t num_rows)
{
    for (size_t i = current_large_block_idx; i < total_large_blocks; ++i)
    {
        auto large_block = i;
        size_t begin = info.ranges[large_block].begin;
        size_t end = info.ranges[large_block].end;

        if (row_offset > end)
            continue;

        if ((row_offset + num_rows) < begin)
            break;

        end = std::min(end, row_offset + num_rows - 1);
        prepare(large_block);
        linearAndImpl(large_block, data, row_offset, end);
    }
}

namespace
{

/// Element for the min-heap used in N-way intersection.
struct HeapItem
{
    uint32_t val = 0;
    uint32_t idx = 0;

    HeapItem() = default;
    HeapItem(uint32_t val_, uint32_t idx_) : val(val_), idx(idx_) {}
    bool operator>(const HeapItem & other) const { return val > other.val; }
};

/// Two-cursor intersection.  The lagging cursor seeks to the leading cursor's doc_id.
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

/// Three-cursor intersection.  All cursors behind the maximum seek forward.
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

/// Four-cursor intersection.
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

/// N-way leapfrog intersection (N <= 8).
/// Uses a linear scan over cursor values to find min/max each round.
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
                    cursors[i]->seek(max_val);
                    if (!cursors[i]->valid())
                        return;
                    vals[i] = cursors[i]->value();
                }
            }
        }
    }
}

/// N-way leapfrog intersection (N > 8).
/// Uses a min-heap to efficiently extract the minimum cursor each round.
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

/// Dispatch to the best leapfrog variant based on cursor count:
///   2 → unrolled two-cursor,  3 → three-cursor,  4 → four-cursor,
///   5..8 → linear scan,  >8 → min-heap.
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

} // anonymous namespace

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

    if (n == 1)
    {
        cursors.front()->linearOr(out, row_offset, num_rows);
        return;
    }

    /// Algorithm selection uses the MINIMUM density across all cursors:
    /// brute-force only wins when ALL lists are dense.  A single sparse cursor
    /// makes leapfrog more efficient because it can skip large unused ranges.
    double min_density = std::numeric_limits<double>::max();
    for (size_t i = 0; i < n; ++i)
        min_density = std::min(min_density, cursors[i]->density());

    if (n < 256 && (min_density >= density_threshold || brute_force_apply))
    {
        ProfileEvents::increment(ProfileEvents::TextIndexLazyBruteForceIntersections);
        intersectBruteForce(out, cursors, row_offset, num_rows);
        return;
    }

    /// Sort cursors by ascending cardinality so the sparsest cursor leads
    /// the leapfrog.  The sparse leader advances in large jumps while dense
    /// followers catch up cheaply via seek.
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
