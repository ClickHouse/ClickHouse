#include <Storages/MergeTree/ProjectionIndex/PostingListCursor.h>

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <turbopfor.h>

namespace ProfileEvents
{
    extern const Event TextIndexLazyPackedBlocksDecoded;
    extern const Event TextIndexLazyPackedBlocksSkipped;
    extern const Event TextIndexLazySeekCount;
    extern const Event TextIndexLazyLargeBlocksPrepared;
    extern const Event TextIndexLazyLargeBlocksSkippedDense;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Probe whether a TurboPFor-encoded block is arithmetic (constant-delta).
/// If so, sets `constant_value` and returns true.
inline bool probeArithmeticBlock(const uint8_t * payload_start, UInt32 bytes, uint32_t & constant_value)
{
    if (bytes == 0)
        return false;

    const uint8_t header_byte = *payload_start;

    if (header_byte == 0x00)
    {
        constant_value = 0;
        return true;
    }

    if ((header_byte & 0xC0u) == 0xC0u)
    {
        unsigned b = header_byte & 0x3Fu;
        unsigned bytes_stored = (b + 7u) / 8u;

        if (bytes_stored == 0)
        {
            constant_value = 0;
            return true;
        }

        /// Guard against truncated/corrupted .pidx where `bytes` < 1 + bytes_stored:
        /// our encoder always writes the full payload, but the deserialization path
        /// may see malformed input.
        if (1 + bytes_stored > bytes)
            return false;

        const uint8_t * cv_payload = payload_start + 1;
        constant_value = 0;
        for (unsigned i = 0; i < bytes_stored && i < 4; ++i)
            constant_value |= static_cast<uint32_t>(cv_payload[i]) << (8u * i);
        if (b < 32u)
            constant_value &= (1u << b) - 1u;
        return true;
    }

    return false;
}

/// Compute the delta base for TurboPFor decoding of packed block `blk`
/// within large block `large_block_idx`.
/// `prev_large_block_last_doc` is the last doc_id of the previous large block
/// (only used when large_block_idx > 0 and blk == 0).
inline uint32_t packedBlockDeltaBase(size_t blk, size_t large_block_idx, UInt32 range_begin, const LargeBlockData & lb, UInt32 prev_large_block_last_doc = 0)
{
    if (blk == 0)
    {
        if (large_block_idx > 0)
            return prev_large_block_last_doc;
        return range_begin;
    }
    return lb.lastDocIdOf(blk - 1);
}

/// Get the byte offset where packed block `blk` starts in the Data Section.
inline UInt32 packedBlockStartOffset(size_t blk, const LargeBlockData & lb)
{
    return (blk == 0) ? 0 : lb.packed_block_cum_bytes[blk - 1];
}

/// Get the byte size of packed block `blk`.
inline UInt32 packedBlockBytes(size_t blk, const LargeBlockData & lb)
{
    return lb.packed_block_cum_bytes[blk] - packedBlockStartOffset(blk, lb);
}

/// Compute the element count for packed block `blk`.
inline UInt32 packedBlockCount(size_t blk, const LargeBlockData & lb)
{
    if (blk + 1 == lb.block_count && lb.tail_size > 0)
        return static_cast<UInt32>(lb.tail_size);
    return static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE);
}

/// Seek within an arithmetic block: find index of first element >= target.
inline size_t seekInArithmeticBlock(uint32_t target, uint32_t first, uint32_t step, uint32_t count)
{
    if (target <= first)
        return 0;
    uint32_t offset_from_first = target - first;
    uint32_t idx = offset_from_first / step;
    uint32_t actual = first + idx * step;
    if (actual < target && idx + 1 < count)
        ++idx;
    return idx;
}

/// Scatter-write: set out[values[i] - row_begin] = 1 for i in [begin, end).
inline void markRows(UInt8 * __restrict out, const uint32_t * values, size_t row_begin, size_t begin, size_t end)
{
    if (begin >= end)
        return;

    for (size_t i = begin; i < end; ++i)
        out[values[i] - row_begin] = 1;
}

/// Scatter-write arithmetic-sequence doc_ids clipped to [row_begin, row_end].
inline void
markArithmeticRows(UInt8 * __restrict out, uint32_t first_doc_id, uint32_t last_doc_id, uint32_t step, size_t row_begin, size_t row_end)
{
    uint32_t start = std::max(first_doc_id, static_cast<uint32_t>(row_begin));
    uint32_t end = std::min(last_doc_id, static_cast<uint32_t>(row_end));

    if (start > end)
        return;

    if (step == 1)
    {
        size_t off = start - row_begin;
        size_t count = end - start + 1;
        memset(out + off, 1, count);
        return;
    }

    uint32_t offset = start - first_doc_id;
    uint32_t idx = (offset + step - 1) / step;
    /// Iterate count-based to avoid `doc_id += step` wrapping past UINT32_MAX
    /// on the last increment (e.g., step close to 2^32 with non-trivial count).
    /// (end - first_doc_id) / step is an upper bound on the largest valid index.
    uint32_t last_idx = (end - first_doc_id) / step;
    for (uint32_t i = idx; i <= last_idx; ++i)
    {
        uint32_t doc_id = first_doc_id + i * step;
        out[doc_id - row_begin] = 1;
    }
}

} // anonymous namespace
// ─── ProjectionPostingListCursor ──────────────────────────────────────────────────

ProjectionPostingListCursor::ProjectionPostingListCursor( // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    LargePostingListReaderStreamPtr owned_stream_,
    const ProjectionTokenInfo & info_,
    LargePostingListReaderStreamPtr idx_stream_,
    LargePostingListReaderStreamPtr pos_stream_,
    DecodedBlockCache * decoded_block_cache_)
    : stream(owned_stream_.get())
    , owned_stream(std::move(owned_stream_))
    , idx_stream(std::move(idx_stream_))
    , pos_stream(std::move(pos_stream_))
    , info(info_)
    , total_large_blocks(info.large_block_metas.size())
    , decoded_cache(decoded_block_cache_)
{
    if (!info.ranges.empty())
    {
        UInt32 global_begin = static_cast<UInt32>(info.ranges.front().begin);
        UInt32 global_end = static_cast<UInt32>(info.ranges.back().end);
        /// Compute span in UInt64 to avoid wrap when the range covers the full UInt32 space
        /// (begin=0, end=UINT32_MAX → span would wrap to 0 in UInt32).
        UInt64 range_span = static_cast<UInt64>(global_end) - global_begin + 1;
        density_value = static_cast<double>(info.cardinality) / static_cast<double>(range_span);
    }
    else
        density_value = 1.0;

    if (total_large_blocks > 0)
    {
        ensureLargeBlock(0);
        decode_buf[0] = static_cast<uint32_t>(info.ranges.front().begin);
        decoded_values = decode_buf;
        decoded_count = 1;
        current_packed_block = SIZE_MAX;
    }
    else if (!info.ranges.empty())
    {
        /// Single-doc token: no large blocks, just first_doc_id in ranges
        decode_buf[0] = static_cast<uint32_t>(info.ranges.front().begin);
        decoded_values = decode_buf;
        decoded_count = 1;
        current_packed_block = 0;
    }
    else
        is_valid = false;
}

ProjectionPostingListCursor::ProjectionPostingListCursor(const ProjectionTokenInfo & info_)
    : ProjectionPostingListCursor(nullptr, info_)
{
}

UInt32 ProjectionPostingListCursor::cardinality() const
{
    return info.cardinality;
}

uint32_t ProjectionPostingListCursor::firstDocId() const
{
    return info.ranges.empty() ? 0 : static_cast<uint32_t>(info.ranges.front().begin);
}

uint32_t ProjectionPostingListCursor::lastDocId() const
{
    return info.ranges.empty() ? 0 : static_cast<uint32_t>(info.ranges.back().end);
}
const LargeBlockData & ProjectionPostingListCursor::ensureLargeBlock(size_t idx)
{
    if (idx == cached_large_block_idx)
        return *cached_large_block_ptr;

    auto [it, inserted] = large_blocks.try_emplace(idx);
    if (!inserted)
    {
        cached_large_block_idx = idx;
        cached_large_block_ptr = it->second.get();
        return *it->second;
    }

    ProfileEvents::increment(ProfileEvents::TextIndexLazyLargeBlocksPrepared);

    chassert(idx < info.large_block_metas.size());
    chassert(idx < info.ranges.size());

    /// Use block_index_data if populated (by mark filtering). Otherwise load from .pidx stream.
    if (idx < info.block_index_data.size() && info.block_index_data[idx])
    {
        it->second = info.block_index_data[idx];
    }
    else if (idx_stream)
    {
        it->second = LargeBlockData::decodeFromIndex(*idx_stream, info.large_block_metas[idx], pos_stream != nullptr);
        if (!it->second)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ProjectionPostingListCursor: decodeFromIndex returned null for large_block_idx={} "
                "(num_packed_blocks=0 but block_doc_count={})",
                idx, info.large_block_metas[idx].block_doc_count);
        }
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ProjectionPostingListCursor requires an index stream (large_block_idx={}). "
            "Tokens with has_block_index=0 should be materialized into bitmaps, not read via ProjectionPostingListCursor.",
            idx);
    }

    cached_large_block_idx = idx;
    cached_large_block_ptr = it->second.get();
    return *it->second;
}
// ─── Element-level iterator ─────────────────────────────────────────────

bool ProjectionPostingListCursor::loadPackedBlock(size_t block_idx)
{
    const auto & lb = ensureLargeBlock(current_large_block);
    chassert(block_idx < lb.packed_block_cum_bytes.size());

    current_packed_block = block_idx;
    arithmetic_mode = false;

    UInt32 count = packedBlockCount(block_idx, lb);

    /// Fast contiguous check: if last_doc_id - delta_base == count, the block
    /// contains contiguous doc_ids with step=1. Skip reading .pst entirely.
    /// This avoids misinterpreting freq data in phrase-mode .pst as doc deltas.
    {
        UInt32 range_begin = static_cast<UInt32>(info.ranges[current_large_block].begin);
        UInt32 prev_lb_last = (current_large_block > 0) ? static_cast<UInt32>(info.ranges[current_large_block - 1].end) : 0;
        uint32_t delta_base = packedBlockDeltaBase(block_idx, current_large_block, range_begin, lb, prev_lb_last);
        if (lb.lastDocIdOf(block_idx) - delta_base == count)
        {
            arithmetic_mode = true;
            arithmetic_step = 1;
            arithmetic_first = delta_base + 1;
            arithmetic_count = count;
            decoded_count = count;
            index = 0;
            return true;
        }
    }

    /// Check decoded block cache first (populated by mark filtering).
    if (decoded_cache)
    {
        const auto * entry = decoded_cache->get(&lb, block_idx);
        if (entry)
        {
            decoded_values = entry->doc_ids;
            decoded_count = entry->count;
            index = 0;
            ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded);
            return false;
        }
    }

    UInt32 range_begin = static_cast<UInt32>(info.ranges[current_large_block].begin);
    UInt32 prev_lb_last = (current_large_block > 0) ? static_cast<UInt32>(info.ranges[current_large_block - 1].end) : 0;
    uint32_t delta_base = packedBlockDeltaBase(block_idx, current_large_block, range_begin, lb, prev_lb_last);

    UInt32 blk_offset = packedBlockStartOffset(block_idx, lb);
    UInt32 bytes = packedBlockBytes(block_idx, lb);

    stream->seek(lb.data_section_start + blk_offset);
    /// The stream position no longer matches the sequential-fill bookkeeping
    /// maintained by `iterateLargeBlock`. Clearing it forces the next
    /// `iterateLargeBlock` call to issue an explicit `stream->seek(...)` before
    /// decoding, instead of trusting a stale `stream_blk_start == last_sequential_block + 1`
    /// continuation and reading the wrong bytes via `pst_decode_buf`.
    last_sequential_block = SIZE_MAX;
    last_sequential_large_block = SIZE_MAX;
    if (!pst_decode_buf)
        pst_decode_buf.emplace(*stream->getDataBuffer());
    else
        pst_decode_buf->reset();
    const uint8_t * src_ptr = pst_decode_buf->ptr();

    if (bytes > 0) [[likely]]
    {
        uint32_t constant_value = 0;
        if (probeArithmeticBlock(src_ptr, bytes, constant_value) && constant_value < UINT32_MAX)
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
    }

    decoded_count = count;
    decoded_values = decode_buf;
    if (count == TURBOPFOR_BLOCK_SIZE) [[likely]]
        turbopfor::p4D1Dec256v32(src_ptr, TURBOPFOR_BLOCK_SIZE, decode_buf, delta_base);
    else
        turbopfor::p4D1Dec32(src_ptr, count, decode_buf, delta_base);
    index = 0;

    /// Validate decoded values: must be strictly monotonically increasing
    /// and within [delta_base+1, lastDocIdOf(block_idx)]. Treat any violation as
    /// `INCORRECT_DATA` instead of merely logging — the decoded `decode_buf` feeds the
    /// hot lookup path (and downstream `lower_bound` calls), so silently using bad data
    /// would produce wrong query results rather than a clear corrupted-part exception.
    if (count > 1) [[likely]]
    {
        bool ok = true;
        for (UInt32 vi = 1; vi < count && ok; ++vi)
            ok = (decode_buf[vi] > decode_buf[vi - 1]);
        if (!ok || decode_buf[0] <= delta_base || decode_buf[count - 1] > lb.lastDocIdOf(block_idx))
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted projection text index: decoded posting block has invalid doc ids "
                "(block_idx={} large_block={} count={} delta_base={} expected_max={} first={} last={} monotonic={})",
                block_idx, current_large_block, count, delta_base,
                lb.lastDocIdOf(block_idx), decode_buf[0], decode_buf[count - 1], ok);
        }
    }

    ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded);
    return false;
}

void ProjectionPostingListCursor::resetLargeBlockPosition(size_t target_row)
{
    if (current_large_block > 0 && current_large_block < total_large_blocks && target_row < info.ranges[current_large_block].begin)
    {
        const auto * it = std::lower_bound(
            info.ranges.begin(),
            info.ranges.begin() + static_cast<ptrdiff_t>(current_large_block),
            target_row,
            [](const RowsRange & r, size_t t) { return r.end < t; });
        current_large_block = static_cast<size_t>(it - info.ranges.begin());
        last_sequential_block = SIZE_MAX;
    }
}

void ProjectionPostingListCursor::seek(uint32_t target)
{
    ProfileEvents::increment(ProfileEvents::TextIndexLazySeekCount);

    if (index < decoded_count) [[likely]]
    {
        if (arithmetic_mode)
        {
            /// Compute in UInt64 to avoid wrap on adversarial step values; the result
            /// is only used as an inclusive upper bound for `target` comparison.
            UInt64 last_in_block = static_cast<UInt64>(arithmetic_first) + static_cast<UInt64>(decoded_count - 1) * arithmetic_step;
            /// Fast path only when `target` is within the current arithmetic block.
            /// If `target < arithmetic_first` we need to fall through to seekImpl to
            /// reposition to an earlier packed block — without this guard,
            /// seekInArithmeticBlock would clamp the index to 0, returning
            /// `value() == arithmetic_first > target`, silently breaking the
            /// AndCursor seek-path verification for any backward seek.
            if (target >= arithmetic_first && target <= last_in_block)
            {
                index = seekInArithmeticBlock(target, arithmetic_first, arithmetic_step, static_cast<uint32_t>(decoded_count));
                is_valid = true;
                return;
            }
        }
        else
        {
            if (decoded_values[decoded_count - 1] >= target) [[likely]]
            {
                const auto * it = std::lower_bound(decoded_values + index, decoded_values + decoded_count, target);
                index = static_cast<size_t>(it - decoded_values);
                is_valid = true;
                return;
            }
        }
    }

    resetLargeBlockPosition(target);

    if (total_large_blocks == 0)
    {
        is_valid = false;
        return;
    }

    if (seekImpl(target))
    {
        is_valid = true;
        return;
    }

    bool found = false;
    size_t start = current_large_block + 1;
    if (start < total_large_blocks)
    {
        const auto * it = std::lower_bound(
            info.ranges.begin() + static_cast<ptrdiff_t>(start),
            info.ranges.end(),
            target,
            [](const RowsRange & r, uint32_t t) { return r.end < t; });

        if (it != info.ranges.end())
        {
            current_large_block = static_cast<size_t>(it - info.ranges.begin());
            decoded_count = 0;
            current_packed_block = 0;
            found = seekImpl(target);
        }
    }

    is_valid = found;
}

bool ProjectionPostingListCursor::seekImpl(uint32_t target)
{
    if (current_large_block == 0 && target <= info.ranges[0].begin)
    {
        decode_buf[0] = static_cast<uint32_t>(info.ranges[0].begin);
        decoded_values = decode_buf;
        decoded_count = 1;
        index = 0;
        current_packed_block = SIZE_MAX;
        /// Clear arithmetic_mode left over from a previous loadPackedBlock —
        /// otherwise value() would read `arithmetic_first + index*arithmetic_step`
        /// (stale) instead of `decoded_values[index]` (the freshly-seeded first doc).
        arithmetic_mode = false;
        return true;
    }

    const auto & lb = ensureLargeBlock(current_large_block);

    /// Find the first packed block whose last_doc_id >= target.
    size_t num_blocks = lb.numPackedBlocks();
    size_t j = 0;
    {
        size_t lo = 0;
        size_t hi = num_blocks;
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (lb.lastDocIdOf(mid) < target)
                lo = mid + 1;
            else
                hi = mid;
        }
        j = lo;
    }
    if (j == num_blocks)
        return false;

    if (j != current_packed_block || decoded_count == 0)
    {
        if (loadPackedBlock(j))
        {
            index = seekInArithmeticBlock(target, arithmetic_first, arithmetic_step, arithmetic_count);
            return true;
        }
    }

    const auto * found_it = std::lower_bound(decoded_values, decoded_values + decoded_count, target);
    if (found_it != decoded_values + decoded_count)
    {
        index = static_cast<size_t>(found_it - decoded_values);
        return true;
    }

    return false;
}

void ProjectionPostingListCursor::next()
{
    if (!is_valid)
        return;

    ++index;

    if (index >= decoded_count)
    {
        if (total_large_blocks == 0)
        {
            is_valid = false;
            return;
        }

        size_t next_block = (current_packed_block == SIZE_MAX) ? 0 : current_packed_block + 1;

        const auto & lb = ensureLargeBlock(current_large_block);
        if (next_block < lb.block_count)
        {
            loadPackedBlock(next_block);
            return;
        }

        size_t next_large_block = current_large_block + 1;
        if (next_large_block >= total_large_blocks)
        {
            is_valid = false;
            return;
        }

        current_large_block = next_large_block;
        loadPackedBlock(0);
    }
}
// ─── IPostingCursor::fill ───────────────────────────────────────────────

/// Iterate packed blocks within a large block, calling visitor for each block's data.
///
/// ArithmeticVisitor: (uint32_t first, uint32_t last, uint32_t step) → void
/// DecodedVisitor:    (const uint32_t * values, UInt32 count) → void
template <typename ArithmeticVisitor, typename DecodedVisitor>
void ProjectionPostingListCursor::iterateLargeBlock(
    size_t large_block, size_t row_begin, size_t row_end, ArithmeticVisitor && on_arithmetic, DecodedVisitor && on_decoded)
{
    chassert(large_block < info.ranges.size());
    const auto & lb = ensureLargeBlock(large_block);

    /// Find the first packed block whose last_doc_id >= row_begin via binary search
    /// on the interleaved ranges array.
    size_t blk_start = 0;
    {
        size_t lo = 0;
        size_t hi = lb.numPackedBlocks();
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (lb.lastDocIdOf(mid) < static_cast<UInt32>(row_begin))
                lo = mid + 1;
            else
                hi = mid;
        }
        blk_start = lo;
    }
    if (blk_start >= lb.block_count)
        return;

    /// Find the end block: first block whose last_doc_id >= row_end.
    size_t blk_end = 0;
    {
        size_t lo = blk_start;
        size_t hi = lb.numPackedBlocks();
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (lb.lastDocIdOf(mid) < static_cast<UInt32>(row_end))
                lo = mid + 1;
            else
                hi = mid;
        }
        blk_end = (lo == lb.numPackedBlocks()) ? lb.block_count : lo + 1;
    }

    const UInt32 range_begin = static_cast<UInt32>(info.ranges[large_block].begin);
    const UInt32 prev_lb_last = (large_block > 0) ? static_cast<UInt32>(info.ranges[large_block - 1].end) : 0;
    uint32_t * iter_decode_buf = stream ? stream->doc_buffer : this->decode_buf;
    size_t last_stream_consumed = SIZE_MAX;

    {
        size_t stream_blk_start = blk_start;

        for (size_t blk = blk_start; blk < blk_end; ++blk)
        {
            UInt32 count = packedBlockCount(blk, lb);
            uint32_t delta_base = packedBlockDeltaBase(blk, large_block, range_begin, lb, prev_lb_last);

            if (lb.lastDocIdOf(blk) - delta_base == count)
            {
                on_arithmetic(delta_base + 1, lb.lastDocIdOf(blk), 1);
                ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksSkipped);
                stream_blk_start = blk + 1;
                continue;
            }

            if (blk == stream_blk_start)
            {
                chassert(stream); /// Non-contiguous block requires stream for decode
                if (!(last_sequential_large_block == large_block && last_sequential_block != SIZE_MAX
                      && stream_blk_start == last_sequential_block + 1))
                {
                    stream->seek(lb.data_section_start + packedBlockStartOffset(stream_blk_start, lb));
                    if (pst_decode_buf)
                        pst_decode_buf->reset();
                }
            }
            if (!pst_decode_buf)
                pst_decode_buf.emplace(*stream->getDataBuffer());

            UInt32 bytes = packedBlockBytes(blk, lb);
            const uint8_t * src_ptr = pst_decode_buf->ptr();

            if (bytes > 0) [[likely]]
            {
                uint32_t constant_value = 0;
                if (probeArithmeticBlock(src_ptr, bytes, constant_value) && constant_value < UINT32_MAX)
                {
                    uint32_t step = constant_value + 1;
                    uint32_t first = delta_base + step;
                    /// Compute in UInt64 and saturate: on adversarial input the product
                    /// could exceed UINT32_MAX; clamping keeps downstream UInt32 math safe.
                    UInt64 last_doc_id_64 = static_cast<UInt64>(first) + static_cast<UInt64>(count - 1) * step;
                    uint32_t last_doc_id = static_cast<uint32_t>(std::min<UInt64>(last_doc_id_64, UINT32_MAX));
                    on_arithmetic(first, last_doc_id, step);
                    pst_decode_buf->advance(bytes);
                    last_stream_consumed = blk;
                    ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksSkipped);
                    continue;
                }
            }

            if (count == TURBOPFOR_BLOCK_SIZE) [[likely]]
                turbopfor::p4D1Dec256v32(src_ptr, TURBOPFOR_BLOCK_SIZE, iter_decode_buf, delta_base);
            else
                turbopfor::p4D1Dec32(src_ptr, count, iter_decode_buf, delta_base);

            /// Validate: strictly monotonically increasing.
            if (count > 1) [[likely]]
            {
                bool ok = true;
                for (UInt32 vi = 1; vi < count && ok; ++vi)
                    ok = (iter_decode_buf[vi] > iter_decode_buf[vi - 1]);
                if (!ok || iter_decode_buf[0] <= delta_base || iter_decode_buf[count - 1] > lb.lastDocIdOf(blk))
                {
                    LOG_TRACE(getLogger("ProjectionPostingListCursor"),
                        "iterateLargeBlock DECODE ERROR: blk={} large_block={} count={} delta_base={} "
                        "expected_max={} first_val={} last_val={} monotonic={} "
                        "src_ptr_align16={} bytes={} sequential_skip={}",
                        blk, large_block, count, delta_base,
                        lb.lastDocIdOf(blk), iter_decode_buf[0], iter_decode_buf[count - 1], ok,
                        (reinterpret_cast<uintptr_t>(src_ptr) % 16 == 0), bytes,
                        (blk == stream_blk_start
                         && last_sequential_large_block == large_block
                         && last_sequential_block != SIZE_MAX
                         && stream_blk_start == last_sequential_block + 1));
                }
            }

            pst_decode_buf->advance(bytes);
            last_stream_consumed = blk;
            on_decoded(iter_decode_buf, count);

            ProfileEvents::increment(ProfileEvents::TextIndexLazyPackedBlocksDecoded);
        }
    }

    /// Only record sequential position for blocks that actually consumed stream data.
    /// Contiguous blocks (detected from metadata alone) skip the stream, so including
    /// them would cause the next call to skip a necessary seek.
    if (last_stream_consumed != SIZE_MAX)
    {
        last_sequential_block = last_stream_consumed;
        last_sequential_large_block = large_block;
    }
}

void ProjectionPostingListCursor::markLargeBlock(size_t large_block, UInt8 * __restrict out, size_t row_begin, size_t row_end)
{
    const UInt32 range_begin = static_cast<UInt32>(info.ranges[large_block].begin);

    if (large_block == 0)
    {
        uint32_t first_doc_id = range_begin;
        if (first_doc_id >= row_begin && first_doc_id <= row_end)
            out[first_doc_id - row_begin] = 1;
    }

    iterateLargeBlock(
        large_block,
        row_begin,
        row_end,
        [&](uint32_t first, uint32_t last, uint32_t step) { markArithmeticRows(out, first, last, step, row_begin, row_end); },
        [&](const uint32_t * values, UInt32 count)
        {
            if (values[0] >= row_begin && values[count - 1] <= row_end) [[likely]]
            {
                markRows(out, values, row_begin, 0, count);
                return;
            }

            size_t begin_idx = 0;
            size_t end_idx = count;

            if (values[0] < row_begin)
            {
                const auto * it = std::lower_bound(values, values + count, static_cast<uint32_t>(row_begin));
                if (it == values + count)
                    return;
                begin_idx = static_cast<size_t>(it - values);
            }
            if (values[count - 1] > row_end)
            {
                const auto * it_e = std::upper_bound(values, values + count, static_cast<uint32_t>(row_end));
                end_idx = static_cast<size_t>(it_e - values);
            }

            markRows(out, values, row_begin, begin_idx, end_idx);
        });
}

void ProjectionPostingListCursor::fill(UInt8 * out, size_t row_offset, size_t num_rows)
{
    if (unlikely(num_rows == 0))
        return;

    /// Single-doc token (no large blocks, just ranges[0])
    if (total_large_blocks == 0 && !info.ranges.empty())
    {
        uint32_t doc_id = static_cast<uint32_t>(info.ranges[0].begin);
        if (doc_id >= row_offset && doc_id < row_offset + num_rows)
            out[doc_id - row_offset] = 1;
        return;
    }

    resetLargeBlockPosition(row_offset);

    for (size_t i = current_large_block; i < total_large_blocks; ++i)
    {
        size_t lb_begin = info.ranges[i].begin;
        size_t lb_end = info.ranges[i].end;

        if (row_offset > lb_end)
            continue;

        if ((row_offset + num_rows) < lb_begin)
            break;

        size_t end = std::min(lb_end, row_offset + num_rows - 1);

        size_t clip_begin = std::max(lb_begin, row_offset);
        size_t clip_off = clip_begin - row_offset;
        size_t clip_count = end - clip_begin + 1;

        /// Dense large block: memset directly.
        {
            size_t actual_doc_count = info.large_block_metas[i].block_doc_count;
            if (i == 0)
                actual_doc_count += 1;

            if (actual_doc_count == lb_end - lb_begin + 1)
            {
                memset(out + clip_off, 1, clip_count);
                ProfileEvents::increment(ProfileEvents::TextIndexLazyLargeBlocksSkippedDense);
                continue;
            }
        }

        markLargeBlock(i, out, row_offset, end);
    }
}
// ─── collectDocIds ──────────────────────────────────────────────────────

void ProjectionPostingListCursor::collectLargeBlock(size_t large_block, PaddedPODArray<uint32_t> & out, size_t row_begin, size_t row_end)
{
    const UInt32 range_begin = static_cast<UInt32>(info.ranges[large_block].begin);

    if (large_block == 0)
    {
        uint32_t first_doc_id = range_begin;
        if (first_doc_id >= row_begin && first_doc_id <= row_end)
            out.push_back(first_doc_id);
    }

    iterateLargeBlock(
        large_block,
        row_begin,
        row_end,
        [&](uint32_t first, uint32_t last, uint32_t step)
        {
            uint32_t start = std::max(first, static_cast<uint32_t>(row_begin));
            uint32_t end = std::min(last, static_cast<uint32_t>(row_end));
            if (start > end)
                return;
            if (step == 1)
            {
                size_t old_size = out.size();
                size_t count = end - start + 1;
                out.resize(old_size + count);
                /// Count-based loop to avoid `d` wrapping when end == UINT32_MAX.
                for (size_t k = 0; k < count; ++k)
                    out[old_size + k] = start + static_cast<uint32_t>(k);
            }
            else
            {
                uint32_t offset = start - first;
                uint32_t idx = (offset + step - 1) / step;
                /// Count-based loop to avoid `doc_id += step` wrapping past UINT32_MAX
                /// on the last increment with large step.
                uint32_t last_idx = (end - first) / step;
                for (uint32_t i = idx; i <= last_idx; ++i)
                    out.push_back(first + i * step);
            }
        },
        [&](const uint32_t * values, UInt32 count)
        {
            const auto * begin_it = std::lower_bound(values, values + count, static_cast<uint32_t>(row_begin));
            const auto * end_it = std::upper_bound(begin_it, values + count, static_cast<uint32_t>(row_end));
            out.insert(out.end(), begin_it, end_it);
        });
}

void ProjectionPostingListCursor::collectDocIds(PaddedPODArray<uint32_t> & out, size_t row_offset, size_t num_rows)
{
    if (unlikely(num_rows == 0))
        return;

    if (total_large_blocks == 0 && !info.ranges.empty())
    {
        uint32_t doc_id = static_cast<uint32_t>(info.ranges[0].begin);
        if (doc_id >= row_offset && doc_id < row_offset + num_rows)
            out.push_back(doc_id);
        return;
    }

    resetLargeBlockPosition(row_offset);

    for (size_t i = current_large_block; i < total_large_blocks; ++i)
    {
        size_t lb_begin = info.ranges[i].begin;
        size_t lb_end = info.ranges[i].end;

        if (row_offset > lb_end)
            continue;
        if ((row_offset + num_rows) < lb_begin)
            break;

        size_t end = std::min(lb_end, row_offset + num_rows - 1);

        size_t actual_doc_count = info.large_block_metas[i].block_doc_count;
        if (i == 0)
            actual_doc_count += 1;

        if (actual_doc_count == lb_end - lb_begin + 1)
        {
            uint32_t start = static_cast<uint32_t>(std::max(lb_begin, row_offset));
            uint32_t clip_end = static_cast<uint32_t>(end);
            size_t old_size = out.size();
            size_t count = clip_end - start + 1;
            out.resize(old_size + count);
            /// Count-based loop to avoid `d` wrapping when clip_end == UINT32_MAX.
            for (size_t k = 0; k < count; ++k)
                out[old_size + k] = start + static_cast<uint32_t>(k);
            ProfileEvents::increment(ProfileEvents::TextIndexLazyLargeBlocksSkippedDense);
            continue;
        }

        collectLargeBlock(i, out, row_offset, end);
    }
}

}
