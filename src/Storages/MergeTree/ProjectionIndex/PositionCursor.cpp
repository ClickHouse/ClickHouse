#include <Storages/MergeTree/ProjectionIndex/PositionCursor.h>

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Common/Exception.h>

#include <turbopfor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

inline UInt32 packedBlockStartOffset(size_t blk, const LargeBlockData & lb)
{
    return (blk == 0) ? 0 : lb.packed_block_cum_bytes[blk - 1];
}

inline UInt32 packedBlockCount(size_t blk, const LargeBlockData & lb)
{
    size_t num_pb = lb.numPackedBlocks();
    if (blk + 1 == num_pb && lb.tail_size > 0)
        return static_cast<UInt32>(lb.tail_size);
    return static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE);
}

inline uint32_t packedBlockDeltaBase(size_t blk, size_t large_block_idx, UInt32 range_begin, const LargeBlockData & lb, UInt32 prev_lb_last)
{
    if (blk == 0)
    {
        if (large_block_idx > 0)
            return prev_lb_last;
        return range_begin;
    }
    return lb.lastDocIdOf(blk - 1);
}

}

PositionCursor::PositionCursor(
    LargePostingListReaderStreamPtr pst_stream_,
    LargePostingListReaderStreamPtr pos_stream_,
    LargePostingListReaderStreamPtr idx_stream_,
    const ProjectionTokenInfo & info_)
    : pst_stream(std::move(pst_stream_))
    , pos_stream(std::move(pos_stream_))
    , idx_stream(std::move(idx_stream_))
    , info(&info_)
{
}

const LargeBlockData & PositionCursor::getLargeBlockData(size_t lb_idx)
{
    chassert(lb_idx < info->block_index_data.size() && info->block_index_data[lb_idx]);
    return *info->block_index_data[lb_idx];
}

bool PositionCursor::locateDoc(UInt32 doc_id, size_t & out_lb_idx, size_t & out_pb_idx)
{
    /// Handle first_doc (doc_count=1, no large blocks) or first_doc before large blocks
    if (info->first_doc_freq > 0 && info->large_block_metas.empty())
    {
        if (doc_id == static_cast<UInt32>(info->ranges[0].begin))
        {
            out_lb_idx = SIZE_MAX;
            out_pb_idx = 0;
            return true;
        }
        return false;
    }

    /// Binary search large blocks via ranges
    const auto * it = std::lower_bound(
        info->ranges.begin(), info->ranges.end(), doc_id, [](const RowsRange & r, UInt32 target) { return r.end < target; });

    if (it == info->ranges.end())
        return false;

    size_t lb_idx = static_cast<size_t>(it - info->ranges.begin());

    /// Check if doc_id is the first_doc (before any large block)
    if (lb_idx == 0 && info->first_doc_freq > 0 && doc_id == static_cast<UInt32>(info->ranges[0].begin))
    {
        out_lb_idx = SIZE_MAX;
        out_pb_idx = 0;
        return true;
    }

    /// Must be within this large block's doc range
    if (lb_idx >= info->large_block_metas.size())
        return false;

    const auto & lb = getLargeBlockData(lb_idx);

    /// Binary search packed_block_ranges to find the right packed block
    size_t num_pb = lb.numPackedBlocks();
    size_t lo = 0;
    size_t hi = num_pb;
    while (lo < hi)
    {
        size_t mid = lo + (hi - lo) / 2;
        if (lb.lastDocIdOf(mid) < doc_id)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo == num_pb)
        return false;

    out_lb_idx = lb_idx;
    out_pb_idx = lo;
    return true;
}

void PositionCursor::ensurePackedBlockDecoded(size_t lb_idx, size_t pb_idx)
{
    if (pb_cache.large_block == lb_idx && pb_cache.packed_block == pb_idx)
        return;

    const auto & lb = getLargeBlockData(lb_idx);
    UInt32 count = packedBlockCount(pb_idx, lb);

    UInt32 range_begin = static_cast<UInt32>(info->ranges[lb_idx].begin);
    UInt32 prev_lb_last = (lb_idx > 0) ? static_cast<UInt32>(info->ranges[lb_idx - 1].end) : 0;
    UInt32 delta_base = packedBlockDeltaBase(pb_idx, lb_idx, range_begin, lb, prev_lb_last);

    UInt32 blk_offset = packedBlockStartOffset(pb_idx, lb);

    pst_stream->seek(lb.data_section_start + blk_offset);
    if (!pst_decode_buf)
        pst_decode_buf.emplace(*pst_stream->getDataBuffer());
    else
        pst_decode_buf->reset();

    /// Decode doc deltas
    {
        const uint8_t * p = pst_decode_buf->ptr();
        const uint8_t * end = nullptr;
        if (count == TURBOPFOR_BLOCK_SIZE)
            end = turbopfor::p4D1Dec256v32(p, TURBOPFOR_BLOCK_SIZE, pb_cache.doc_ids, delta_base);
        else
            end = turbopfor::p4D1Dec32(p, count, pb_cache.doc_ids, delta_base);
        pst_decode_buf->advance(static_cast<size_t>(end - p));
    }

    /// Decode freqs
    {
        const uint8_t * p = pst_decode_buf->ptr();
        const uint8_t * end = nullptr;
        if (count == TURBOPFOR_BLOCK_SIZE)
            end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, pb_cache.freqs);
        else
            end = turbopfor::p4Dec32(p, count, pb_cache.freqs);
        pst_decode_buf->advance(static_cast<size_t>(end - p));
    }

    /// Validate the decoded packed block before any consumer runs `std::lower_bound` over
    /// `pb_cache.doc_ids` or trusts the freqs. A corrupted `.pst` could otherwise produce
    /// non-monotonic / out-of-range ids that silently steer `seekDoc` to the wrong doc, or
    /// zero/oversized freqs that drive downstream position arithmetic into garbage. The
    /// expected range for this packed block's doc ids is `(delta_base, lastDocIdOf(pb_idx)]`.
    if (count > 0) [[likely]]
    {
        const UInt32 expected_max = lb.lastDocIdOf(pb_idx);

        bool monotonic = true;
        for (UInt32 vi = 1; vi < count && monotonic; ++vi)
            monotonic = (pb_cache.doc_ids[vi] > pb_cache.doc_ids[vi - 1]);

        if (!monotonic || pb_cache.doc_ids[0] <= delta_base
            || pb_cache.doc_ids[count - 1] > expected_max) [[unlikely]]
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted projection text index: phrase posting block has invalid doc ids "
                "(pb_idx={} lb_idx={} count={} delta_base={} expected_max={} first={} last={} monotonic={})",
                pb_idx, lb_idx, count, delta_base, expected_max,
                pb_cache.doc_ids[0], pb_cache.doc_ids[count - 1], monotonic);

        for (UInt32 vi = 0; vi < count; ++vi)
            if (pb_cache.freqs[vi] == 0) [[unlikely]]
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted projection text index: phrase posting block has zero frequency "
                    "at offset {} (pb_idx={} lb_idx={})",
                    vi, pb_idx, lb_idx);
    }

    pb_cache.large_block = lb_idx;
    pb_cache.packed_block = pb_idx;
    pb_cache.count = count;
}

void PositionCursor::ensurePosBlockDecoded(size_t lb_idx, UInt32 pos_block_idx)
{
    if (pos_cache.large_block == lb_idx && pos_cache.pos_block_idx == pos_block_idx)
        return;

    const auto & lb = getLargeBlockData(lb_idx);
    UInt64 total_pos_deltas = lb.pos_cum_deltas.back();

    UInt64 block_start_delta = static_cast<UInt64>(pos_block_idx) * TURBOPFOR_BLOCK_SIZE;
    /// Guard against corrupted .pos where pos_block_idx points past the end of the
    /// position stream — unsigned subtraction below would wrap to a huge value.
    if (block_start_delta > total_pos_deltas)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Corrupted projection text index: position block index {} starts at delta {} which exceeds total {}",
            pos_block_idx, block_start_delta, total_pos_deltas);
    UInt32 count = static_cast<UInt32>(std::min(static_cast<UInt64>(TURBOPFOR_BLOCK_SIZE), total_pos_deltas - block_start_delta));

    /// Seek to the right byte offset in .pos
    UInt64 byte_offset = (pos_block_idx == 0) ? 0 : lb.pos_cum_bytes[pos_block_idx - 1];
    pos_stream->seek(lb.pos_start_offset + byte_offset);
    if (!pos_decode_buf)
        pos_decode_buf.emplace(*pos_stream->getDataBuffer());
    else
        pos_decode_buf->reset();

    const uint8_t * p = pos_decode_buf->ptr();
    const uint8_t * end = nullptr;
    if (count == TURBOPFOR_BLOCK_SIZE)
        end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, pos_cache.values);
    else
        end = turbopfor::p4Dec32(p, count, pos_cache.values);
    pos_decode_buf->advance(static_cast<size_t>(end - p));

    pos_cache.large_block = lb_idx;
    pos_cache.pos_block_idx = pos_block_idx;
    pos_cache.count = count;
}

UInt32 PositionCursor::seekDoc(UInt32 doc_id)
{
    size_t lb_idx = 0;
    size_t pb_idx = 0;

    if (!locateDoc(doc_id, lb_idx, pb_idx))
    {
        doc_state.freq = 0;
        return 0;
    }

    doc_state.consumed = 0;
    doc_state.is_first = true;
    doc_state.last_position = 0;

    /// Handle first_doc special case
    if (lb_idx == SIZE_MAX)
    {
        UInt32 freq = info->first_doc_freq;
        doc_state.lb_idx = SIZE_MAX;
        doc_state.freq = freq;

        if (freq > 0)
        {
            if (info->large_block_metas.empty())
            {
                /// Single-doc token: positions stored as standalone tail block
                pos_stream->seek(info->first_doc_pos_offset);
                if (!pos_decode_buf)
                    pos_decode_buf.emplace(*pos_stream->getDataBuffer());
                else
                    pos_decode_buf->reset();

                UInt32 n = std::min(freq, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));
                const uint8_t * p = pos_decode_buf->ptr();
                const uint8_t * end = nullptr;
                if (n == TURBOPFOR_BLOCK_SIZE)
                    end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, pos_cache.values);
                else
                    end = turbopfor::p4Dec32(p, n, pos_cache.values);
                pos_decode_buf->advance(static_cast<size_t>(end - p));

                pos_cache.large_block = SIZE_MAX;
                pos_cache.pos_block_idx = 0;
                pos_cache.count = n;

                doc_state.cur_pos_block = 0;
                doc_state.cur_offset = 0;
            }
            else
            {
                /// Multi-doc token: first_doc's positions at the START of large block 0's .pos data
                doc_state.lb_idx = 0;
                doc_state.cur_pos_block = 0;
                doc_state.cur_offset = 0;
                ensurePosBlockDecoded(0, 0);
            }
        }

        return freq;
    }

    /// Large block path
    ensurePackedBlockDecoded(lb_idx, pb_idx);

    const auto * it = std::lower_bound(pb_cache.doc_ids, pb_cache.doc_ids + pb_cache.count, doc_id);
    if (it == pb_cache.doc_ids + pb_cache.count || *it != doc_id)
    {
        doc_state.freq = 0;
        return 0;
    }

    size_t doc_pos_in_block = static_cast<size_t>(it - pb_cache.doc_ids);
    UInt32 freq = pb_cache.freqs[doc_pos_in_block];

    doc_state.lb_idx = lb_idx;
    doc_state.freq = freq;

    if (freq > 0)
    {
        const auto & lb = getLargeBlockData(lb_idx);

        UInt64 cum_freq_before = lb.pos_cum_deltas[pb_idx];
        for (size_t i = 0; i < doc_pos_in_block; ++i)
            cum_freq_before += pb_cache.freqs[i];

        doc_state.cur_pos_block = static_cast<UInt32>(cum_freq_before / TURBOPFOR_BLOCK_SIZE);
        doc_state.cur_offset = static_cast<UInt32>(cum_freq_before % TURBOPFOR_BLOCK_SIZE);

        ensurePosBlockDecoded(lb_idx, doc_state.cur_pos_block);
    }

    return freq;
}

UInt32 PositionCursor::nextPosition()
{
    if (doc_state.consumed >= doc_state.freq)
        return NO_MORE_POSITIONS;

    /// Advance to the next pos block if current is exhausted
    if (doc_state.cur_offset >= pos_cache.count)
    {
        if (doc_state.lb_idx == SIZE_MAX)
        {
            /// Single-doc first_doc: decode next block from sequential stream
            UInt32 remaining = doc_state.freq - doc_state.consumed;
            UInt32 n = std::min(remaining, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));
            const uint8_t * p = pos_decode_buf->ptr();
            const uint8_t * end = nullptr;
            if (n == TURBOPFOR_BLOCK_SIZE)
                end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, pos_cache.values);
            else
                end = turbopfor::p4Dec32(p, n, pos_cache.values);
            pos_decode_buf->advance(static_cast<size_t>(end - p));

            ++pos_cache.pos_block_idx;
            pos_cache.count = n;
        }
        else
        {
            ++doc_state.cur_pos_block;
            ensurePosBlockDecoded(doc_state.lb_idx, doc_state.cur_pos_block);
        }
        doc_state.cur_offset = 0;
    }

    UInt32 delta = pos_cache.values[doc_state.cur_offset];
    ++doc_state.cur_offset;
    ++doc_state.consumed;

    /// Delta-to-absolute: first position is absolute, subsequent are delta-1
    UInt32 abs_pos = 0;
    if (doc_state.is_first)
    {
        abs_pos = delta;
        doc_state.is_first = false;
    }
    else
    {
        /// Detect UInt32 overflow before computing `abs_pos`. A corrupt delta would otherwise
        /// wrap silently and make phrase matching report adjacency for positions that came
        /// from arbitrary file bytes. The check is on the cold per-position path; it adds two
        /// scalar comparisons, negligible relative to the per-position arithmetic and decode.
        /// Both subtractions are computed in UInt64 to avoid wrap in the bound itself.
        const UInt64 last = doc_state.last_position;
        const UInt64 d = delta;
        if (last + d + 1u > std::numeric_limits<UInt32>::max()) [[unlikely]]
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted projection text index: phrase position arithmetic overflows UInt32 "
                "(last_position={}, delta={})",
                doc_state.last_position, delta);
        abs_pos = static_cast<UInt32>(last + d + 1u);
    }
    doc_state.last_position = abs_pos;

    return abs_pos;
}

}
