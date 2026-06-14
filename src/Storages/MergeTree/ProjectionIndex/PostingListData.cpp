#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/ProjectionIndex/LengthPrefixedInt.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Storages/MergeTree/ProjectionIndex/TurboPForBlockDecodeBuffer.h>
#include <base/scope_guard.h>
#include <Common/Arena.h>
#include <Common/Exception.h>

#include <turbopfor.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

/// Alias: VarInt functions delegate to the shared LengthPrefixedInt header.
namespace VarInt = LengthPrefixedInt;

void PagePool::grow()
{
    /// Allocate a new segment. Existing segments and pages stay put — no realloc/copy.
    UInt64 base_idx_64 = static_cast<UInt64>(segments.size()) * SEGMENT_PAGES;
    if (base_idx_64 + SEGMENT_PAGES > NIL)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "Text index page pool overflow: too many unique tokens in a single block");
    UInt32 base_idx = static_cast<UInt32>(base_idx_64);

    auto * new_seg = reinterpret_cast<UInt32 *>(arena->alignedAlloc(SEGMENT_PAGES * PAGE_ELEMS * sizeof(UInt32), 16));
    auto * new_next = reinterpret_cast<UInt32 *>(arena->alloc(SEGMENT_PAGES * sizeof(UInt32)));

    /// Build the free list within the new segment, chaining the last slot to the old free head.
    for (UInt32 i = 0; i < SEGMENT_PAGES - 1; ++i)
    {
        new_seg[i * PAGE_ELEMS] = base_idx + i + 1;
        new_next[i] = NIL;
    }
    new_seg[(SEGMENT_PAGES - 1) * PAGE_ELEMS] = free_head;
    new_next[SEGMENT_PAGES - 1] = NIL;

    free_head = base_idx;
    segments.push_back(new_seg);
    next_segments.push_back(new_next);
}

void TokenWriteContext::add(UInt32 doc_id, PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer)
{
    auto & w = writer();
    if (w.doc_count == 0)
    {
        w.first_doc_id = doc_id;
        last_doc_id = doc_id;
        ++w.doc_count;
        return;
    }

    if (doc_id < last_doc_id)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Received out of order doc id. doc_id = {}, last_doc_id = {}", doc_id, last_doc_id);

    if (doc_id == last_doc_id)
        return;

    /// Track the first doc_id of the current packed block.
    /// doc_count starts at 1 (first_doc_id is separate), so the first delta-encoded doc
    /// has doc_count == 2. A new block starts when (doc_count - 1) % 256 == 1.
    if ((w.doc_count - 1) % TURBOPFOR_BLOCK_SIZE == 0)
        block_first_doc_id = doc_id;

    doc_deltas.append(doc_id - last_doc_id - 1, pool);
    last_doc_id = doc_id;
    ++w.doc_count;

    UInt32 buf_count = (w.doc_count - 1) % TURBOPFOR_BLOCK_SIZE;
    if (buf_count == 0)
    {
        doc_deltas.gather(pool, scratch);
        doc_deltas.freeAll(pool);

        uint8_t * end = turbopfor::p4Enc256v32(scratch, TURBOPFOR_BLOCK_SIZE, packed_buffer);
        UInt32 doc_len = static_cast<UInt32>(end - packed_buffer);
        chassert(doc_len <= TURBOPFOR_MAX_ENCODED_SIZE);
        auto * place = chunk_arena.alignedAlloc(doc_len + sizeof(PostingListChunk), alignof(PostingListChunk));
        PostingListChunk * cur_block = new (place) PostingListChunk(last_doc_id, doc_len, block_first_doc_id);
        memcpy(cur_block->data(), packed_buffer, doc_len);
        if (!w.blocks_head)
            w.blocks_head = cur_block;
        else
            *blocks_tail = cur_block;
        blocks_tail = &cur_block->next;
    }
}

void TokenWriteContext::finalize(PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer)
{
    auto & w = writer();
    w.first_doc_freq = 0;

    if (w.doc_count == 0)
        return;

    UInt32 tail = (w.doc_count - 1) % TURBOPFOR_BLOCK_SIZE;
    if (tail > 0)
    {
        doc_deltas.gather(pool, scratch);
        doc_deltas.freeAll(pool);

        uint8_t * end = turbopfor::p4Enc32(scratch, tail, packed_buffer);
        UInt32 len = static_cast<UInt32>(end - packed_buffer);
        auto * place = chunk_arena.alignedAlloc(len + sizeof(PostingListChunk), alignof(PostingListChunk));
        PostingListChunk * tail_block = new (place) PostingListChunk(last_doc_id, len, block_first_doc_id);
        memcpy(tail_block->data(), packed_buffer, len);
        if (!w.blocks_head)
            w.blocks_head = tail_block;
        else
            *blocks_tail = tail_block;
    }
}

// ─── TokenWriteContextPhrase ────────────────────────────────────────

/// Flush a full 256-element block: gather from page chains, encode, interleave onto blocks chain.
void TokenWriteContextPhrase::flushBlock(PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer)
{
    auto & w = writer();

    /// Encode doc_delta block
    doc_deltas.gather(pool, scratch);
    doc_deltas.freeAll(pool);
    uint8_t * doc_end = turbopfor::p4Enc256v32(scratch, TURBOPFOR_BLOCK_SIZE, packed_buffer);
    UInt32 doc_len = static_cast<UInt32>(doc_end - packed_buffer);
    chassert(doc_len <= TURBOPFOR_MAX_ENCODED_SIZE);
    auto * doc_place = chunk_arena.alignedAlloc(doc_len + sizeof(PostingListChunk), alignof(PostingListChunk));
    PostingListChunk * doc_chunk = new (doc_place) PostingListChunk(last_doc_id, doc_len, block_first_doc_id);
    memcpy(doc_chunk->data(), packed_buffer, doc_len);
    if (!w.blocks_head)
        w.blocks_head = doc_chunk;
    else
        *blocks_tail = doc_chunk;
    blocks_tail = &doc_chunk->next;

    /// Encode freq block — immediately after doc in the chain.
    /// Store sum(freq) in the chunk's auxiliary field so finish() can skip decoding freq
    /// (it only needs the sum to compute pos_cum_deltas).
    freqs.gather(pool, scratch);
    freqs.freeAll(pool);
    UInt32 freq_sum = 0;
    for (UInt32 i = 0; i < TURBOPFOR_BLOCK_SIZE; ++i)
        freq_sum += scratch[i];
    uint8_t * freq_end = turbopfor::p4Enc256v32(scratch, TURBOPFOR_BLOCK_SIZE, packed_buffer);
    UInt32 freq_len = static_cast<UInt32>(freq_end - packed_buffer);
    auto * freq_place = chunk_arena.alignedAlloc(freq_len + sizeof(PostingListChunk), alignof(PostingListChunk));
    PostingListChunk * freq_chunk = new (freq_place) PostingListChunk(freq_sum, freq_len);
    memcpy(freq_chunk->data(), packed_buffer, freq_len);
    *blocks_tail = freq_chunk;
    blocks_tail = &freq_chunk->next;
}

void TokenWriteContextPhrase::sealPosPage(PagePool & pool, Arena & chunk_arena, UInt32 * scratch)
{
    if (positions.empty())
        return;

    UInt32 count = positions.gather(pool, scratch);
    positions.freeAll(pool);

    UInt32 byte_len = count * sizeof(UInt32);
    auto * place = chunk_arena.alignedAlloc(byte_len + sizeof(PostingListChunk), alignof(PostingListChunk));
    PostingListChunk * chunk = new (place) PostingListChunk(0, byte_len);
    memcpy(chunk->data(), scratch, byte_len);
    if (!pos_pages_head)
        pos_pages_head = chunk;
    else
        *pos_pages_tail = chunk;
    pos_pages_tail = &chunk->next;
}

void TokenWriteContextPhrase::add(
    UInt32 doc_id, UInt32 position, PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer)
{
    auto & w = writer();

    if (w.doc_count == 0)
    {
        w.doc_count = 1;
        current_doc_id = doc_id;
        current_doc_freq = 1;
        last_position = position;
        positions.append(position, pool);
        return;
    }

    /// ── New doc transition ──
    if (current_doc_id != doc_id)
    {
        if (doc_id < current_doc_id)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Received out of order doc id. doc_id = {}, last_committed = {}", doc_id, current_doc_id);

        if (w.doc_count == 1)
        {
            w.first_doc_id = current_doc_id;
            w.first_doc_freq = current_doc_freq;
        }
        else
        {
            doc_deltas.append(current_doc_id - last_doc_id - 1, pool);
            freqs.append(current_doc_freq, pool);
        }

        last_doc_id = current_doc_id;

        /// Flush now so the doc chunk records the 256th doc_id as its last_doc_id.
        if (w.doc_count > 1)
        {
            UInt32 idx = (w.doc_count - 2) % TURBOPFOR_BLOCK_SIZE;
            if (idx == TURBOPFOR_BLOCK_SIZE - 1)
                flushBlock(pool, chunk_arena, scratch, packed_buffer);
        }

        ++w.doc_count;
        current_doc_id = doc_id;
        current_doc_freq = 0;
        last_position = 0;

        /// Track the first doc_id of the current packed block.
        /// doc_count - 1 = number of delta-encoded docs. A new block starts at boundary.
        if ((w.doc_count - 2) % TURBOPFOR_BLOCK_SIZE == 0)
            block_first_doc_id = doc_id;
    }

    /// ── Accumulate position ──
    UInt32 pos_delta = (current_doc_freq == 0) ? position : (position - last_position - 1);
    last_position = position;
    ++current_doc_freq;

    positions.append(pos_delta, pool);

    /// Seal position page at TURBOPFOR_BLOCK_SIZE boundary
    if (positions.size(pool) == TURBOPFOR_BLOCK_SIZE)
        sealPosPage(pool, chunk_arena, scratch);
}

void TokenWriteContextPhrase::finalize(PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer)
{
    auto & w = writer();

    if (w.doc_count == 0)
        return;

    if (w.doc_count == 1)
    {
        w.first_doc_id = current_doc_id;
        w.first_doc_freq = current_doc_freq;
        sealPosPage(pool, chunk_arena, scratch);
        if (pos_pages_head)
            w.blocks_head = pos_pages_head;
        return;
    }

    /// Store last doc's delta and freq
    doc_deltas.append(current_doc_id - last_doc_id - 1, pool);
    freqs.append(current_doc_freq, pool);
    last_doc_id = current_doc_id;

    UInt32 tail = (w.doc_count - 1) % TURBOPFOR_BLOCK_SIZE;

    if (tail == 0)
    {
        flushBlock(pool, chunk_arena, scratch, packed_buffer);
    }
    else
    {
        /// Encode tail doc_deltas
        doc_deltas.gather(pool, scratch);
        doc_deltas.freeAll(pool);
        uint8_t * end = turbopfor::p4Enc32(scratch, tail, packed_buffer);
        UInt32 len = static_cast<UInt32>(end - packed_buffer);
        auto * place = chunk_arena.alignedAlloc(len + sizeof(PostingListChunk), alignof(PostingListChunk));
        PostingListChunk * tail_block = new (place) PostingListChunk(last_doc_id, len, block_first_doc_id);
        memcpy(tail_block->data(), packed_buffer, len);
        if (!w.blocks_head)
            w.blocks_head = tail_block;
        else
            *blocks_tail = tail_block;
        blocks_tail = &tail_block->next;

        /// Encode tail freq — immediately after doc in the chain.
        /// Sum stored in chunk's aux field for finish()'s fast path.
        freqs.gather(pool, scratch);
        freqs.freeAll(pool);
        UInt32 freq_sum = 0;
        for (UInt32 i = 0; i < tail; ++i)
            freq_sum += scratch[i];
        end = turbopfor::p4Enc32(scratch, tail, packed_buffer);
        UInt32 flen = static_cast<UInt32>(end - packed_buffer);
        place = chunk_arena.alignedAlloc(flen + sizeof(PostingListChunk), alignof(PostingListChunk));
        PostingListChunk * freq_chunk = new (place) PostingListChunk(freq_sum, flen);
        memcpy(freq_chunk->data(), packed_buffer, flen);
        *blocks_tail = freq_chunk;
        blocks_tail = &freq_chunk->next;
    }

    /// Seal last pos page and append pos pages to the chain
    sealPosPage(pool, chunk_arena, scratch);

    if (pos_pages_head)
        *blocks_tail = pos_pages_head;
}

/// Writes large posting blocks to the `.pst` data stream.  When `write_block_index` is true,
/// an Index Section (per-packed-block offsets and last_doc_ids) is written to `.pidx`
/// to enable sub-block seeking for lazy cursor apply mode.
///
/// When `pos_out` is non-null (phrase mode), each doc block in `.pst` is followed by a
/// TurboPFor-encoded freq block, and position deltas are written as 256-element TurboPFor
/// blocks to `.pos`. The `.pidx` Index Section is extended with `pos_cum_deltas` (cumulative
/// freq sums) and `pos_cum_bytes` (cumulative `.pos` block byte offsets).
class LargePostingBlockWriter
{
public:
    LargePostingBlockWriter( // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        WriteBuffer & meta_out_,
        WriteBuffer & data_out_,
        UInt32 docs_per_large_block_,
        bool write_block_index_,
        LargePostingListWriterStream & scratch_owner,
        WriteBuffer * index_out_ = nullptr,
        WriteBuffer * pos_out_ = nullptr)
        : pos_encode_buf(scratch_owner.packed_buffer)
        , meta_out(meta_out_)
        , data_out(data_out_)
        , index_out(index_out_)
        , pos_out(pos_out_)
        , docs_per_large_block(docs_per_large_block_)
        , write_block_index(write_block_index_)
    {
        if (write_block_index)
        {
            chassert(index_out);
            UInt32 packed_blocks_per_large_block = docs_per_large_block / TURBOPFOR_BLOCK_SIZE;
            packed_block_ranges.reserve(packed_blocks_per_large_block * 2);
            packed_block_cum_bytes.reserve(packed_blocks_per_large_block);

            if (pos_out)
            {
                packed_block_pos_cum_deltas.reserve(packed_blocks_per_large_block + 1);
                packed_block_pos_cum_deltas.push_back(0);
            }
        }
    }

    /// Record .pos start offset before any pos data is written (merge path only).
    /// In the initial write, pos_block_start_offset is set inside addBlock when
    /// docs_in_current_block==0. In the merge write, first_doc's pos is fed via
    /// feedPositionDeltas BEFORE addBlock, so we must capture the offset earlier.
    void initPosStartOffset()
    {
        if (pos_out && docs_in_current_block == 0 && !pos_offset_initialized)
        {
            pos_block_start_offset = pos_out->count();
            pos_offset_initialized = true;
        }
    }

    /// Add a packed doc-delta block (and optionally freq block) to the current large block.
    void addBlock(
        UInt32 first_doc_id,
        UInt32 last_doc_id,
        const char * doc_data,
        UInt32 doc_bytes,
        const char * freq_data = nullptr,
        UInt32 freq_bytes = 0,
        UInt32 freq_sum = 0,
        UInt32 block_doc_count = TURBOPFOR_BLOCK_SIZE)
    {
        if (docs_in_current_block == 0)
        {
            large_block_start_offset = data_out.count();
            current_block_first_doc_id = first_doc_id;
            if (pos_out && !pos_offset_initialized)
                pos_block_start_offset = pos_out->count();
            pos_offset_initialized = false;
        }

        /// Write doc deltas to .pst
        data_out.write(doc_data, doc_bytes);

        /// Write freq block to .pst (immediately after doc deltas)
        UInt32 total_pst_bytes = doc_bytes;
        if (freq_data && freq_bytes > 0)
        {
            data_out.write(freq_data, freq_bytes);
            total_pst_bytes += freq_bytes;
        }

        if (write_block_index)
        {
            packed_block_ranges.push_back(first_doc_id);
            packed_block_ranges.push_back(last_doc_id);
            UInt32 prev_cum = packed_block_cum_bytes.empty() ? 0 : packed_block_cum_bytes.back();
            packed_block_cum_bytes.push_back(prev_cum + total_pst_bytes);
        }

        /// Track position skip data from freq_sum (pos data is written in flushLargeBlock).
        if (write_block_index && pos_out)
            packed_block_pos_cum_deltas.push_back(packed_block_pos_cum_deltas.back() + freq_sum);

        docs_in_current_block += block_doc_count;
        current_block_last_doc_id = last_doc_id;

        if (docs_in_current_block >= docs_per_large_block)
            flushLargeBlock();
    }

    void finish(UInt32 num_large_blocks_expected)
    {
        if (docs_in_current_block > 0)
            flushLargeBlock();
        else if (pos_out)
        {
            /// Even with no doc blocks (e.g. doc_count==1 path), pos data may have been
            /// fed via `feedPositionDeltas` and is sitting in the carry — flush it.
            flushPositionTail();
        }

        chassert(num_large_blocks_written == num_large_blocks_expected);

        /// Write large block metadata in columnar TurboPFor format.
        writeLargeBlockMeta();
    }

    /// Set the pos pages chain for direct encoding in flushLargeBlock.
    /// Pos pages are 256-aligned raw UInt32 deltas from the tokenize phase.
    void setPositionPages(PostingListChunk * head)
    {
        pos_page = head;
        pos_page_offset = 0;
    }

    /// Record how many pos deltas from first_doc precede the first doc block.
    void setPosBaseline(UInt64 first_doc_freq)
    {
        if (write_block_index && !packed_block_pos_cum_deltas.empty())
            packed_block_pos_cum_deltas[0] = first_doc_freq;
    }

    /// Feed position deltas into the .pos accumulator (merge write path only).
    void feedPositionDeltas(const UInt32 * deltas, UInt32 count)
    {
        if (pos_out && count > 0)
            encodePosDeltas(deltas, count);
    }

    /// Scratch buffer borrowed from LargePostingListWriterStream.
    uint8_t * pos_encode_buf;

    /// Carry buffer for stateful pos-delta encoding within a single large block.
    /// Multiple `encodePosDeltas`/`feedPositionDeltas` calls are joined into 256-element
    /// blocks; the residual is flushed by `flushPositionTail` at large block boundary.
    UInt32 pos_carry[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
    UInt32 pos_carry_count = 0;

private:
    /// Delta-1-encode an array in 256-element SIMD blocks and write to `out`.
    /// Supports both UInt32 (p4D1Enc256v32/p4D1Enc32) and UInt64 (p4D1Enc256v64/p4D1Enc64).
    template <typename T>
    void encodeDelta1(T * values, UInt32 count, T start, WriteBuffer & out) const
    {
        UInt32 p = 0;
        UInt32 remaining = count;
        while (remaining >= TURBOPFOR_BLOCK_SIZE)
        {
            uint8_t * end = nullptr;
            if constexpr (std::is_same_v<T, UInt32>)
                end = turbopfor::p4D1Enc256v32(values + p, TURBOPFOR_BLOCK_SIZE, pos_encode_buf, start);
            else
                end = turbopfor::p4D1Enc256v64(values + p, TURBOPFOR_BLOCK_SIZE, pos_encode_buf, start);
            out.write(reinterpret_cast<const char *>(pos_encode_buf), static_cast<size_t>(end - pos_encode_buf));
            start = values[p + TURBOPFOR_BLOCK_SIZE - 1];
            p += TURBOPFOR_BLOCK_SIZE;
            remaining -= TURBOPFOR_BLOCK_SIZE;
        }
        if (remaining > 0)
        {
            uint8_t * end = nullptr;
            if constexpr (std::is_same_v<T, UInt32>)
                end = turbopfor::p4D1Enc32(values + p, remaining, pos_encode_buf, start);
            else
                end = turbopfor::p4D1Enc64(values + p, remaining, pos_encode_buf, start);
            out.write(reinterpret_cast<const char *>(pos_encode_buf), static_cast<size_t>(end - pos_encode_buf));
        }
    }

    /// Encode position deltas to .pos. Stateful across calls within a single large block:
    /// buffers a partial 256-block in `pos_carry` so multiple calls (e.g. one per merged
    /// posting block) compose into a single block layout — only the LAST block (emitted by
    /// `flushPositionTail`) may be smaller than 256. Without this, each call would emit its
    /// own tail prematurely, breaking the reader's pos_cum_bytes accounting.
    void encodePosDeltas(const UInt32 * deltas, UInt32 count)
    {
        chassert(pos_out);
        UInt32 src_pos = 0;

        /// Top up the carry first.
        if (pos_carry_count > 0)
        {
            UInt32 need = TURBOPFOR_BLOCK_SIZE - pos_carry_count;
            UInt32 fill = std::min(need, count);
            std::memcpy(pos_carry + pos_carry_count, deltas, fill * sizeof(UInt32));
            pos_carry_count += fill;
            src_pos += fill;
            if (pos_carry_count == TURBOPFOR_BLOCK_SIZE)
            {
                emitPosBlock(pos_carry, TURBOPFOR_BLOCK_SIZE, false);
                pos_carry_count = 0;
            }
        }

        /// Emit full 256-blocks directly from the input.
        while (src_pos + TURBOPFOR_BLOCK_SIZE <= count)
        {
            emitPosBlock(deltas + src_pos, TURBOPFOR_BLOCK_SIZE, false);
            src_pos += TURBOPFOR_BLOCK_SIZE;
        }

        /// Stash any leftover into the carry.
        UInt32 leftover = count - src_pos;
        if (leftover > 0)
        {
            std::memcpy(pos_carry + pos_carry_count, deltas + src_pos, leftover * sizeof(UInt32));
            pos_carry_count += leftover;
        }
    }

    /// Flush the partial tail block at the end of a large block.
    void flushPositionTail()
    {
        if (pos_carry_count > 0)
        {
            emitPosBlock(pos_carry, pos_carry_count, true);
            pos_carry_count = 0;
        }
    }

    void emitPosBlock(const UInt32 * src, UInt32 n, bool is_tail)
    {
        uint8_t * end = nullptr;
        if (is_tail)
            end = turbopfor::p4Enc32(src, n, pos_encode_buf);
        else
            end = turbopfor::p4Enc256v32(src, TURBOPFOR_BLOCK_SIZE, pos_encode_buf);
        UInt32 encoded_bytes = static_cast<UInt32>(end - pos_encode_buf);
        pos_out->write(reinterpret_cast<const char *>(pos_encode_buf), encoded_bytes);
        pos_block_cum_bytes.push_back(pos_block_cum_bytes.empty() ? encoded_bytes : pos_block_cum_bytes.back() + encoded_bytes);
    }

    /// Consume `count` raw UInt32 deltas from pos page walker and encode to .pos.
    /// Pos pages may not be aligned to TURBOPFOR_BLOCK_SIZE — `encodePosDeltas` carries
    /// state across calls so a multi-page large block produces one tail at the end.
    void consumeAndEncodePos(UInt64 count)
    {
        if (!pos_out || count == 0)
            return;

        while (count > 0 && pos_page)
        {
            const UInt32 * raw = reinterpret_cast<const UInt32 *>(pos_page->data());
            UInt32 page_elems = pos_page->len / sizeof(UInt32);
            UInt32 avail = page_elems - pos_page_offset;
            UInt32 take = static_cast<UInt32>(std::min(static_cast<UInt64>(avail), count));
            encodePosDeltas(raw + pos_page_offset, take);
            pos_page_offset += take;
            count -= take;
            if (pos_page_offset == page_elems)
            {
                pos_page = pos_page->next;
                pos_page_offset = 0;
            }
        }
    }

    void flushLargeBlock()
    {
        /// Buffer large block metadata for columnar encoding at finish().
        large_block_ranges.push_back(current_block_first_doc_id);
        large_block_ranges.push_back(current_block_last_doc_id);
        large_block_pst_offsets.push_back(large_block_start_offset);

        if (write_block_index)
        {
            /// Encode this large block's pos deltas from pos pages directly to .pos.
            if (pos_out)
            {
                consumeAndEncodePos(packed_block_pos_cum_deltas.back());
                flushPositionTail();
            }

            UInt64 index_section_offset = index_out->count();
            large_block_pidx_offsets.push_back(index_section_offset);

            UInt32 num_packed_blocks = static_cast<UInt32>(packed_block_ranges.size() / 2);
            chassert(packed_block_cum_bytes.size() == num_packed_blocks);

            VarInt::writeUInt32(num_packed_blocks, *index_out);

            /// Interleaved ranges [f0, l0, f1, l1, ...] — strictly monotone, single TurboPFor stream.
            encodeDelta1(packed_block_ranges.data(), num_packed_blocks * 2, UInt32{0}, *index_out);
            encodeDelta1(packed_block_cum_bytes.data(), num_packed_blocks, static_cast<UInt32>(-1), *index_out);

            /// Phrase support: write pos_cum_deltas and pos_cum_bytes to .pidx
            if (pos_out)
            {
                chassert(packed_block_pos_cum_deltas.size() == num_packed_blocks + 1);

                encodeDelta1(packed_block_pos_cum_deltas.data(), num_packed_blocks + 1, static_cast<UInt64>(-1), *index_out);

                /// Safe cast: each pos block holds 256 positions, so exceeding UInt32::max blocks
                /// would require ~1 trillion positions per token per large block — not possible.
                UInt32 num_pos_blocks = static_cast<UInt32>(pos_block_cum_bytes.size());
                VarInt::writeUInt32(num_pos_blocks, *index_out);
                if (num_pos_blocks > 0)
                {
                    encodeDelta1(pos_block_cum_bytes.data(), num_pos_blocks, static_cast<UInt64>(-1), *index_out);
                }

                large_block_pos_offsets.push_back(pos_block_start_offset);
            }

            packed_block_ranges.clear();
            packed_block_cum_bytes.clear();

            if (pos_out)
            {
                packed_block_pos_cum_deltas.clear();
                packed_block_pos_cum_deltas.push_back(0);
                pos_block_cum_bytes.clear();
            }
        }

        docs_in_current_block = 0;
        ++num_large_blocks_written;
    }

    /// Write all large block metadata in columnar TurboPFor format to meta_out.
    /// Prefixed with total byte count so readers can skip without decoding.
    void writeLargeBlockMeta()
    {
        UInt32 n = num_large_blocks_written;
        if (n == 0)
            return;

        chassert(large_block_ranges.size() == n * 2);
        chassert(large_block_pst_offsets.size() == n);

        /// Encode into a temporary buffer to compute total size.
        PODArray<char> buf;
        {
            WriteBufferFromVector<PODArray<char>> tmp(buf);

            /// UInt32 doc_ids — interleaved [first, last, ...], TurboPFor delta-1 encoded.
            encodeDelta1(large_block_ranges.data(), n * 2, UInt32{0}, tmp);

            /// UInt64 offsets — TurboPFor delta-1 encoded.
            encodeDelta1(large_block_pst_offsets.data(), n, UInt64{0}, tmp);

            if (write_block_index)
            {
                chassert(large_block_pidx_offsets.size() == n);
                encodeDelta1(large_block_pidx_offsets.data(), n, UInt64{0}, tmp);

                if (pos_out)
                {
                    chassert(large_block_pos_offsets.size() == n);
                    encodeDelta1(large_block_pos_offsets.data(), n, UInt64{0}, tmp);
                }
            }

            tmp.finalize();
        }

        /// Write size prefix + payload.
        writeVarUInt(static_cast<UInt64>(buf.size()), meta_out);
        meta_out.write(buf.data(), buf.size());
    }

    WriteBuffer & meta_out;
    WriteBuffer & data_out;
    WriteBuffer * index_out;
    WriteBuffer * pos_out;

    UInt32 docs_per_large_block;
    bool write_block_index;
    UInt32 docs_in_current_block = 0;
    UInt32 current_block_first_doc_id = 0;
    UInt32 current_block_last_doc_id = 0;
    UInt32 num_large_blocks_written = 0;
    UInt64 large_block_start_offset = 0;
    UInt64 pos_block_start_offset = 0; /// .pos stream offset at start of current large block
    bool pos_offset_initialized = false; /// true if initPosStartOffset was called for current LB

    /// Large block metadata — buffered during flushLargeBlock, written columnar at finish.
    std::vector<UInt32> large_block_ranges; /// interleaved [first, last, first, last, ...]
    std::vector<UInt64> large_block_pst_offsets;
    std::vector<UInt64> large_block_pidx_offsets;
    std::vector<UInt64> large_block_pos_offsets;

    /// Doc block index data
    std::vector<UInt32> packed_block_ranges; /// interleaved [first, last, first, last, ...]
    std::vector<UInt32> packed_block_cum_bytes; /// doc_bytes + freq_bytes combined

    /// Position index data (phrase mode only)
    std::vector<UInt64> packed_block_pos_cum_deltas; /// cumulative freq sum per doc block
    std::vector<UInt64> pos_block_cum_bytes; /// cumulative bytes per pos TurboPFor block

    /// Pos page walker state — walks the 256-aligned raw UInt32 delta pages
    /// from the tokenize phase, encoding directly to .pos in flushLargeBlock.
    PostingListChunk * pos_page = nullptr;
    UInt32 pos_page_offset = 0;
};

void PostingListWriter::finish(
    WriteBuffer & wb,
    LargePostingListWriterStream & stream,
    const MergeTreeIndexTextParams & index_params,
    WriteBuffer * index_stream,
    LargePostingListWriterStream * pos_stream) const
{
    WriteBuffer * pos_buf = pos_stream ? &pos_stream->plain_hashing : nullptr;
    bool has_positions = pos_stream != nullptr;

    /// Buffer the entire dict entry so we can write a size prefix before it.
    /// Format: [entry_bytes][doc_count][...entry content...]
    /// This enables O(1) skip: readVarUInt(size), ignore(size).
    PODArray<char> entry_buf;
    WriteBufferFromVector<PODArray<char>> entry_wb(entry_buf);

    VarInt::writeUInt32(doc_count, entry_wb);
    if (doc_count == 0)
    {
        entry_wb.finalize();
        writeVarUInt(static_cast<UInt64>(entry_buf.size()), wb);
        wb.write(entry_buf.data(), entry_buf.size());
        return;
    }

    VarInt::writeUInt32(first_doc_id, entry_wb);

    /// Phrase header: first_doc_freq + pos_offset for first_doc's position data.
    if (has_positions)
    {
        VarInt::writeUInt32(first_doc_freq, entry_wb);
        UInt64 first_doc_pos_offset = pos_buf->count();
        writeVarUInt(first_doc_pos_offset, entry_wb);
    }

    if (doc_count == 1)
    {
        /// Write first_doc's pos deltas directly to .pos (no LargePostingBlockWriter needed).
        if (has_positions && first_doc_freq > 0 && blocks_head)
        {
            LargePostingBlockWriter pos_writer_helper(entry_wb, stream.plain_hashing, 1, false, stream, nullptr, pos_buf);
            /// Gather all pos deltas across pages first, then encode once: emitting one
            /// tail block per page would produce an invalid layout for the reader.
            std::vector<UInt32> deltas;
            for (PostingListChunk * pos_page = blocks_head; pos_page; pos_page = pos_page->next)
            {
                const UInt32 * raw = reinterpret_cast<const UInt32 *>(pos_page->data());
                UInt32 count = pos_page->len / sizeof(UInt32);
                deltas.insert(deltas.end(), raw, raw + count);
            }
            if (!deltas.empty())
                pos_writer_helper.feedPositionDeltas(deltas.data(), static_cast<UInt32>(deltas.size()));
            pos_writer_helper.finish(0);
        }

        entry_wb.finalize();
        writeVarUInt(static_cast<UInt64>(entry_buf.size()), wb);
        wb.write(entry_buf.data(), entry_buf.size());
        return;
    }

    /// ---- Large posting list (doc_count > 1) ----
    const UInt32 docs_per_large_block
        = (static_cast<UInt32>(index_params.posting_list_block_size) + TURBOPFOR_BLOCK_SIZE - 1) & ~(TURBOPFOR_BLOCK_SIZE - 1);
    const UInt32 large_doc_count = doc_count - 1;
    const UInt32 num_large_blocks = (large_doc_count + docs_per_large_block - 1) / docs_per_large_block;

    chassert(num_large_blocks >= 1);
    VarInt::writeUInt32(num_large_blocks, entry_wb);

    LargePostingBlockWriter block_writer(
        entry_wb, stream.plain_hashing, docs_per_large_block, index_params.has_block_index, stream, index_stream, pos_buf);

    UInt32 full_chunks = large_doc_count / TURBOPFOR_BLOCK_SIZE;
    UInt32 tail_docs = large_doc_count % TURBOPFOR_BLOCK_SIZE;
    UInt32 num_doc_chunks = full_chunks + (tail_docs > 0 ? 1 : 0);

    /// Walk the interleaved chain:
    ///   phrase=false:  [doc0] → [doc1] → ... → [doc_tail]
    ///   phrase=true:   [doc0] → [freq0] → [doc1] → [freq1] → ... → [pos_page0] → [pos_page1] → ...
    PostingListChunk * it = blocks_head;

    /// Set up pos page walker — pos pages follow all [doc][freq] pairs in the chain.
    if (has_positions)
    {
        PostingListChunk * pos_pages_start = blocks_head;
        for (UInt32 i = 0; i < num_doc_chunks * 2; ++i)
        {
            chassert(pos_pages_start);
            pos_pages_start = pos_pages_start->next;
        }
        block_writer.setPositionPages(pos_pages_start);
        block_writer.setPosBaseline(first_doc_freq);
    }

    for (UInt32 chunk_idx = 0; chunk_idx < num_doc_chunks; ++chunk_idx)
    {
        chassert(it);
        bool is_tail = (tail_docs > 0) && (chunk_idx == full_chunks);
        UInt32 block_doc_count = is_tail ? tail_docs : TURBOPFOR_BLOCK_SIZE;

        const char * doc_data = reinterpret_cast<const char *>(it->data());
        UInt32 doc_bytes = it->len;
        UInt32 doc_first_id = it->first_doc_id;
        UInt32 doc_last_id = it->last_doc_id;
        it = it->next;

        const char * freq_data = nullptr;
        UInt32 freq_bytes = 0;
        UInt32 freq_sum = 0;

        if (has_positions)
        {
            chassert(it);
            freq_data = reinterpret_cast<const char *>(it->data());
            freq_bytes = it->len;
            freq_sum = it->freq_sum;
            it = it->next;
        }

        block_writer.addBlock(doc_first_id, doc_last_id, doc_data, doc_bytes, freq_data, freq_bytes, freq_sum, block_doc_count);
    }

    block_writer.finish(num_large_blocks);

    entry_wb.finalize();
    writeVarUInt(static_cast<UInt64>(entry_buf.size()), wb);
    wb.write(entry_buf.data(), entry_buf.size());
}

ReaderStreamEntry::ReaderStreamEntry(
    LargePostingListReaderStreamPtr stream_, UInt32 first_doc_id_, UInt32 doc_count_, LargePostingBlockMetas large_posting_blocks_)
    : stream(std::move(stream_))
    , first_doc_id(first_doc_id_)
    , doc_count(doc_count_)
    , large_posting_blocks(std::move(large_posting_blocks_))
{
    chassert(stream);
}

ReaderStreamVector::ReaderStreamVector(
    LargePostingListReaderStreamPtr stream, UInt32 first_doc_id, UInt32 doc_count, LargePostingBlockMetas large_posting_blocks)
    : entries({{std::move(stream), first_doc_id, doc_count, std::move(large_posting_blocks)}})
{
}

void ReaderStreamVector::merge(ReaderStreamVector & other)
{
    for (auto & oe : other.entries)
    {
        for (const auto & e : entries)
        {
            if (e == oe)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate LargePostingListReaderStream detected in merge");
        }
        entries.emplace_back(std::move(oe));
    }
    other.entries.clear();
}

struct ReaderStreamCursor
{
    LargePostingListReaderStream * stream;

    UInt32 * doc_buffer;
    UInt32 last_doc_id = 0;
    UInt32 remaining_count;
    UInt32 buf_size;
    UInt32 pos;
    UInt64 offset;
    bool do_seek;
    bool has_freq = false; /// true when phrase mode: skip freq block after each doc block

    /// Disk-based c'tor
    ReaderStreamCursor(
        LargePostingListReaderStream * s,
        UInt32 first_doc_id,
        UInt32 remaining_count_,
        UInt64 offset_,
        bool do_seek_,
        bool include_first_doc,
        bool has_freq_ = false)
        : stream(s)
        , doc_buffer(stream->doc_buffer)
        , last_doc_id(first_doc_id)
        , remaining_count(remaining_count_)
        , buf_size(1)
        , pos(0)
        , offset(offset_)
        , do_seek(do_seek_)
        , has_freq(has_freq_)
    {
        chassert(stream);
        chassert(doc_buffer);

        if (!do_seek && remaining_count > 0)
            chassert(static_cast<UInt64>(stream->getPosition()) == offset);

        if (include_first_doc)
        {
            doc_buffer[0] = first_doc_id;
            if (stream->merged_part_offsets)
                stream->merged_part_offsets->mapOffsets(stream->part_index, doc_buffer, 1);
        }
        else
        {
            next();
        }
    }
    UInt32 ALWAYS_INLINE current() const
    {
        chassert(pos < buf_size);
        return doc_buffer[pos];
    }

    void ALWAYS_INLINE next()
    {
        chassert(pos < buf_size);
        ++pos;
        if (pos >= buf_size)
            loadNextBlock();
    }

    bool ALWAYS_INLINE empty() const { return buf_size == pos && remaining_count == 0; }

    /// Batch emit all remaining documents in current buffer and beyond
    template <typename Emit>
    void emitAll(Emit && emit)
    {
        while (!empty())
        {
            size_t remaining = buf_size - pos;
            for (size_t i = 0; i < remaining; ++i)
                emit(doc_buffer[pos + i]);
            pos = buf_size;
            loadNextBlock();
        }
    }

    /// Emit up to next_min (exclusive), loading blocks as needed
    template <typename Emit>
    void emitUntil(UInt32 next_min, Emit && emit)
    {
        chassert(current() < next_min);
        while (!empty())
        {
            if (doc_buffer[buf_size - 1] < next_min)
            {
                for (size_t i = pos; i < buf_size; ++i)
                    emit(doc_buffer[i]);
                pos = buf_size;
                loadNextBlock();
            }
            else
            {
                const UInt32 * it = std::lower_bound(doc_buffer + pos, doc_buffer + buf_size, next_min);
                for (const UInt32 * p = doc_buffer + pos; p != it; ++p)
                    emit(*p);
                pos = static_cast<UInt32>(it - doc_buffer);
                break;
            }
        }
    }

    PostingListPtr materializeIntoBitmap()
    {
        auto bitmap = std::make_shared<PostingList>();
        while (!empty())
        {
            chassert(pos == 0);
            bitmap->addMany(buf_size, doc_buffer);
            pos = buf_size;
            loadNextBlock();
        }
        return bitmap;
    }

    bool ALWAYS_INLINE operator<(const ReaderStreamCursor & rhs) const { return current() < rhs.current(); }

private:
    void loadNextBlock()
    {
        if (remaining_count == 0)
            return;

        if (do_seek)
        {
            stream->seek(offset);
            do_seek = false;
        }

        UInt32 count = std::min(remaining_count, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));
        auto & dbuf = stream->decodeBuffer();

        /// Decode doc deltas
        {
            const uint8_t * p = dbuf.ptr();
            const uint8_t * end = nullptr;
            if (count == TURBOPFOR_BLOCK_SIZE)
                end = turbopfor::p4D1Dec256v32(p, TURBOPFOR_BLOCK_SIZE, doc_buffer, last_doc_id);
            else
                end = turbopfor::p4D1Dec32(p, count, doc_buffer, last_doc_id);
            dbuf.advance(static_cast<size_t>(end - p));
        }

        /// For phrase mode, skip the freq block that follows each doc block.
        if (has_freq)
        {
            UInt32 * freq_discard = stream->scratch_a;
            const uint8_t * p = dbuf.ptr();
            const uint8_t * end = nullptr;
            if (count == TURBOPFOR_BLOCK_SIZE)
                end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, freq_discard);
            else
                end = turbopfor::p4Dec32(p, count, freq_discard);
            dbuf.advance(static_cast<size_t>(end - p));
        }

        last_doc_id = doc_buffer[count - 1];
        remaining_count -= count;
        buf_size = count;
        pos = 0;

        if (stream->merged_part_offsets)
            stream->merged_part_offsets->mapOffsets(stream->part_index, doc_buffer, count);
    }
};

struct ReaderStreamCursorNode
{
    ReaderStreamCursor * cursor;

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool ALWAYS_INLINE operator<(const ReaderStreamCursorNode & rhs) const { return cursor->current() > rhs.cursor->current(); }
};

using ReaderStreamQueue = std::priority_queue<ReaderStreamCursorNode, std::vector<ReaderStreamCursorNode>>;

template <typename EmitFirst, typename Emit>
void mergePostingCursors(std::vector<ReaderStreamCursor> & cursors, EmitFirst && emit_first, Emit && emit)
{
    cursors.erase(std::remove_if(cursors.begin(), cursors.end(), [](const auto & c) { return c.empty(); }), cursors.end());

    if (cursors.empty())
        return;

    if (cursors.size() == 1)
    {
        emit_first(cursors[0].current(), cursors[0].stream);
        cursors[0].next();
        cursors[0].emitAll([&](UInt32 doc_id) { emit(doc_id, cursors[0].stream); });
        return;
    }

    ReaderStreamQueue heap;
    for (auto & c : cursors)
        heap.push(ReaderStreamCursorNode{&c});

    /// Emit first doc
    {
        auto cur = heap.top();
        heap.pop();
        emit_first(cur.cursor->current(), cur.cursor->stream);
        cur.cursor->next();
        if (!cur.cursor->empty())
            heap.push(cur);
    }

    while (!heap.empty())
    {
        auto cur = heap.top();
        heap.pop();

        auto stream_emit = [&](UInt32 doc_id) { emit(doc_id, cur.cursor->stream); };

        if (heap.empty())
            cur.cursor->emitAll(stream_emit);
        else
            cur.cursor->emitUntil(heap.top().cursor->current(), stream_emit);

        if (!cur.cursor->empty())
            heap.push(cur);
    }
}

PostingListPtr ReaderStreamEntry::materializeLargeBlockIntoBitmap(
    LargePostingListReaderStream & stream, UInt32 last_doc_id, UInt32 block_doc_count, UInt64 offset, bool include_first_doc, bool has_freq)
{
    stream.seek(offset);

    ReaderStreamCursor cursor(&stream, last_doc_id, block_doc_count, offset, false /* do_seek */, include_first_doc, has_freq);
    return cursor.materializeIntoBitmap();
}

std::string LargePostingBlockMeta::toString() const
{
    return fmt::format(
        "{{first_doc_id: {}, last_doc_id: {}, block_doc_count: {}, offset: {}, index_offset: {}}}",
        first_doc_id,
        last_doc_id,
        block_doc_count,
        offset,
        index_offset);
}

std::string ReaderStreamEntry::toString() const
{
    std::vector<std::string> block_strs;
    block_strs.reserve(large_posting_blocks.size());
    for (const auto & block : large_posting_blocks)
        block_strs.push_back(block.toString());

    return fmt::format(
        "ReaderStreamEntry(stream_ptr: {}, first_doc_id: {}, doc_count: {}, blocks: [{}])",
        static_cast<const void *>(stream.get()),
        first_doc_id,
        doc_count,
        fmt::join(block_strs, ", "));
}

std::string ReaderStreamVector::toString() const
{
    if (entries.empty())
        return "ReaderStreamVector(empty)";

    std::vector<std::string> entry_strs;
    entry_strs.reserve(entries.size());
    for (const auto & entry : entries)
        entry_strs.push_back(entry.toString());

    return fmt::format("ReaderStreamVector(size: {}) [\n  {}\n]", entries.size(), fmt::join(entry_strs, ",\n  "));
}

LazyPostingStream::LazyPostingStream(ReaderStreamVector streams_)
    : streams(std::move(streams_))
{
}

LazyPostingStream::~LazyPostingStream() = default;

void PostingListStream::read(
    ReadBuffer & in,
    const LargePostingListReaderStreamPtr & stream,
    const MergeTreeIndexTextParams & index_params,
    const LargePostingListReaderStreamPtr & pos_stream)
{
    /// Read entry_bytes prefix, then ensure contiguous memory for the entire entry.
    UInt64 entry_bytes = 0;
    readVarUInt(entry_bytes, in);

    /// Cap entry_bytes to prevent unbounded allocations from corrupted varints.
    /// 1 GiB is well above any realistic posting list entry (a single entry encodes one token's
    /// document list; tokens with that many documents would have been split into large blocks).
    static constexpr UInt64 MAX_ENTRY_BYTES = 1ULL << 30;
    if (entry_bytes > MAX_ENTRY_BYTES)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted projection text index: posting list entry size {} exceeds maximum {}",
            entry_bytes, MAX_ENTRY_BYTES);

    PODArray<char> entry_storage;
    const uint8_t * ptr = nullptr;
    /// TurboPFor loadU64Fast may read up to 7 bytes past the last encoded byte.
    static constexpr size_t DECODE_PADDING = 7;
    if (static_cast<size_t>(in.available()) >= entry_bytes + DECODE_PADDING)
    {
        ptr = reinterpret_cast<const uint8_t *>(in.position());
    }
    else
    {
        entry_storage.resize_fill(entry_bytes + DECODE_PADDING, 0);
        in.readStrict(entry_storage.data(), entry_bytes);
        ptr = reinterpret_cast<const uint8_t *>(entry_storage.data());
    }
    const uint8_t * const entry_end = ptr + entry_bytes;

    /// Bounds-checked varint reader for the in-place decode path. The raw-pointer overload
    /// (`LengthPrefixedInt::readUInt32`) has no `entry_end` check; a corrupt 5-byte marker
    /// could otherwise advance past the declared entry and bleed into the next entry. Each
    /// read advances `ptr`; this lambda asserts the advance stayed within `entry_end`.
    auto read_varint_u32 = [&](UInt32 & out)
    {
        VarInt::readUInt32(out, ptr);
        if (ptr > entry_end) [[unlikely]]
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted projection text index: varint read advanced past declared entry end (entry_bytes={})",
                entry_bytes);
    };

    read_varint_u32(doc_count);
    if (doc_count == 0)
    {
        if (entry_storage.empty())
            in.position() += entry_bytes;
        return;
    }

    UInt32 last_doc_id = 0;
    read_varint_u32(last_doc_id);
    first_doc_id = last_doc_id;

    chassert(stream);

    UInt32 first_doc_freq_read = 0;
    UInt64 first_doc_pos_offset = 0;
    if (index_params.enable_phrase_query_support)
    {
        read_varint_u32(first_doc_freq_read);
        ptr = reinterpret_cast<const uint8_t *>(
            readVarUInt(first_doc_pos_offset, reinterpret_cast<const char *>(ptr), static_cast<size_t>(entry_end - ptr)));
    }

    if (doc_count == 1)
    {
        lazy = new LazyPostingStream(ReaderStreamVector{stream, first_doc_id, 1, LargePostingBlockMetas{}});
        if (first_doc_freq_read > 0 && !lazy->streams.entries.empty())
        {
            first_doc_freq = first_doc_freq_read;
            auto & entry = lazy->streams.entries.back();
            entry.first_doc_freq = first_doc_freq_read;
            entry.first_doc_pos_offset = first_doc_pos_offset;
        }
        if (pos_stream && !lazy->streams.empty())
            lazy->streams.entries.back().pos_stream = pos_stream;

        /// Advance ReadBuffer past the entry if we decoded in-place.
        if (entry_storage.empty())
            in.position() += entry_bytes;
        return;
    }

    UInt32 num_large_blocks = 0;
    read_varint_u32(num_large_blocks);
    if (num_large_blocks < 1) [[unlikely]]
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted projection text index: posting list with {} documents must have at least one large block",
            doc_count);

    /// Cap num_large_blocks to prevent unbounded vector allocations below. A large block holds
    /// `posting_list_block_size`-many docs; the count is bounded above by `doc_count` itself,
    /// which is at most UInt32::max(). Use that as the hard upper limit.
    if (num_large_blocks > doc_count) [[unlikely]]
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted projection text index: num_large_blocks {} exceeds doc_count {}",
            num_large_blocks, doc_count);

    /// Skip the large_block_meta_bytes size prefix written by writeLargeBlockMeta.
    /// The payload follows immediately and is decoded from the same contiguous buffer.
    UInt64 large_block_meta_bytes = 0;
    ptr = reinterpret_cast<const uint8_t *>(
        readVarUInt(large_block_meta_bytes, reinterpret_cast<const char *>(ptr), static_cast<size_t>(entry_end - ptr)));

    UInt32 remaining_docs = doc_count - 1;
    const UInt32 docs_per_large_block
        = (static_cast<UInt32>(index_params.posting_list_block_size) + TURBOPFOR_BLOCK_SIZE - 1) & ~(TURBOPFOR_BLOCK_SIZE - 1);

    /// Decode columnar large block metadata from contiguous memory — no TurboPForBlockDecodeBuffer needed.
    auto decode_delta1 = [](const uint8_t *& src, UInt32 count, UInt32 * out, uint32_t prev)
    {
        UInt32 pos = 0;
        UInt32 remaining = count;
        while (remaining >= TURBOPFOR_BLOCK_SIZE)
        {
            src = turbopfor::p4D1Dec256v32(src, TURBOPFOR_BLOCK_SIZE, out + pos, prev);
            prev = out[pos + TURBOPFOR_BLOCK_SIZE - 1];
            pos += TURBOPFOR_BLOCK_SIZE;
            remaining -= TURBOPFOR_BLOCK_SIZE;
        }
        if (remaining > 0)
            src = turbopfor::p4D1Dec32(src, remaining, out + pos, prev);
    };

    auto decode_delta1_64 = [](const uint8_t *& src, UInt32 count, UInt64 * out, uint64_t prev)
    {
        UInt32 pos = 0;
        UInt32 remaining = count;
        while (remaining >= TURBOPFOR_BLOCK_SIZE)
        {
            src = turbopfor::p4D1Dec256v64(src, TURBOPFOR_BLOCK_SIZE, out + pos, prev);
            prev = out[pos + TURBOPFOR_BLOCK_SIZE - 1];
            pos += TURBOPFOR_BLOCK_SIZE;
            remaining -= TURBOPFOR_BLOCK_SIZE;
        }
        if (remaining > 0)
            src = turbopfor::p4D1Dec64(src, remaining, out + pos, prev);
    };

    std::vector<UInt32> lb_ranges(num_large_blocks * 2);
    decode_delta1(ptr, num_large_blocks * 2, lb_ranges.data(), 0);

    std::vector<UInt64> pst_offsets(num_large_blocks);
    decode_delta1_64(ptr, num_large_blocks, pst_offsets.data(), 0);

    std::vector<UInt64> pidx_offsets(num_large_blocks, 0);
    if (index_params.has_block_index)
        decode_delta1_64(ptr, num_large_blocks, pidx_offsets.data(), 0);

    std::vector<UInt64> pos_offsets(num_large_blocks, 0);
    if (index_params.has_block_index && index_params.enable_phrase_query_support)
        decode_delta1_64(ptr, num_large_blocks, pos_offsets.data(), 0);

    if (ptr != entry_end) [[unlikely]]
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted projection text index: posting list entry decoded {} bytes, expected {}",
            static_cast<size_t>(ptr - reinterpret_cast<const uint8_t *>(entry_end - entry_bytes)),
            entry_bytes);

    LargePostingBlockMetas large_posting_blocks;
    large_posting_blocks.reserve(num_large_blocks);

    for (UInt32 i = 0; i < num_large_blocks; ++i)
    {
        UInt32 docs_in_large_block = std::min(remaining_docs, docs_per_large_block);
        remaining_docs -= docs_in_large_block;

        large_posting_blocks.emplace_back(
            lb_ranges[i * 2], lb_ranges[i * 2 + 1], docs_in_large_block, pst_offsets[i], pidx_offsets[i], pos_offsets[i]);
    }

    lazy = new LazyPostingStream(ReaderStreamVector{stream, last_doc_id, doc_count, std::move(large_posting_blocks)});

    if (pos_stream)
    {
        chassert(!lazy->streams.empty());
        auto & entry = lazy->streams.entries.back();
        entry.pos_stream = pos_stream;
    }

    if (first_doc_freq_read > 0 && !lazy->streams.entries.empty())
    {
        first_doc_freq = first_doc_freq_read;
        auto & entry = lazy->streams.entries.back();
        entry.first_doc_freq = first_doc_freq_read;
        entry.first_doc_pos_offset = first_doc_pos_offset;
    }

    /// Advance ReadBuffer past the entry if we decoded in-place.
    if (entry_storage.empty())
        in.position() += entry_bytes;
}

void PostingListStream::skip(ReadBuffer & in, const MergeTreeIndexTextParams & /*index_params*/)
{
    /// Skip the entire entry using the size prefix: [entry_bytes][...payload...]
    UInt64 entry_bytes = 0;
    readVarUInt(entry_bytes, in);
    in.ignore(entry_bytes);
}

void PostingListStream::write(
    WriteBuffer & wb,
    LargePostingListWriterStream & stream,
    const MergeTreeIndexTextParams & index_params,
    LargePostingListWriterStream * index_stream,
    WriteBuffer * pos_writer) const
{
    /// Buffer the entire dict entry so we can write a size prefix before it.
    PODArray<char> entry_buf;
    WriteBufferFromVector<PODArray<char>> entry_wb(entry_buf);

    VarInt::writeUInt32(doc_count, entry_wb);
    if (doc_count == 0)
    {
        entry_wb.finalize();
        writeVarUInt(static_cast<UInt64>(entry_buf.size()), wb);
        wb.write(entry_buf.data(), entry_buf.size());
        return;
    }

    if (doc_count == 1)
    {
        UInt32 mapped_doc_id = first_doc_id;
        if (lazy && !lazy->streams.empty())
        {
            auto & s = lazy->streams.entries.front();
            if (s.stream && s.stream->merged_part_offsets)
                s.stream->merged_part_offsets->mapOffsets(s.stream->part_index, &mapped_doc_id, 1);
        }
        VarInt::writeUInt32(mapped_doc_id, entry_wb);

        if (pos_writer && first_doc_freq > 0)
        {
            /// Embedded entries from `AggregateFunctionPostingList::deserialize` do not carry
            /// position bytes (only doc_ids). Advertising a `pos_offset` to the dictionary
            /// while writing zero position bytes leaves a phrase reader pointing into the
            /// next entry's position payload. Reject the combination so the caller fails
            /// fast at merge time rather than producing garbage phrase matches at query time.
            if (lazy && !lazy->streams.empty())
            {
                const auto & front = lazy->streams.entries.front();
                if (!front.pos_stream && front.first_doc_freq > 0)
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Projection text index: writing embedded single-document phrase positions "
                        "(first_doc_freq={}) is not supported because deserialize does not preserve "
                        "the .pos payload. Materialize the state before serialization.",
                        front.first_doc_freq);
            }

            VarInt::writeUInt32(first_doc_freq, entry_wb);
            UInt64 pos_offset = static_cast<UInt64>(pos_writer->count());
            writeVarUInt(pos_offset, entry_wb);

            /// Copy first_doc's pos deltas from source .pos to output .pos.
            if (lazy && !lazy->streams.empty())
            {
                auto & entry = lazy->streams.entries.front();
                if (entry.pos_stream && entry.first_doc_freq > 0)
                {
                    UInt64 src_offset = entry.large_posting_blocks.empty() ? entry.first_doc_pos_offset
                                                                           : entry.large_posting_blocks.front().pos_start_offset;
                    entry.pos_stream->seek(src_offset);
                    auto & pos_dbuf = entry.pos_stream->decodeBuffer();

                    UInt32 remaining = entry.first_doc_freq;
                    alignas(16) UInt32 decode_buf[TURBOPFOR_BLOCK_SIZE];
                    uint8_t encode_buf[TURBOPFOR_MAX_ENCODED_SIZE];

                    while (remaining > 0)
                    {
                        UInt32 n = std::min(remaining, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));

                        const uint8_t * p = pos_dbuf.ptr();
                        const uint8_t * end = nullptr;
                        if (n == TURBOPFOR_BLOCK_SIZE)
                            end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, decode_buf);
                        else
                            end = turbopfor::p4Dec32(p, n, decode_buf);
                        pos_dbuf.advance(static_cast<size_t>(end - p));

                        uint8_t * enc_end = nullptr;
                        if (n == TURBOPFOR_BLOCK_SIZE)
                            enc_end = turbopfor::p4Enc256v32(decode_buf, TURBOPFOR_BLOCK_SIZE, encode_buf);
                        else
                            enc_end = turbopfor::p4Enc32(decode_buf, n, encode_buf);
                        pos_writer->write(reinterpret_cast<const char *>(encode_buf), static_cast<size_t>(enc_end - encode_buf));

                        remaining -= n;
                    }
                }
            }
        }
        entry_wb.finalize();
        writeVarUInt(static_cast<UInt64>(entry_buf.size()), wb);
        wb.write(entry_buf.data(), entry_buf.size());
        return;
    }

    /// Large posting list

    chassert(lazy);

    UInt32 * doc_delta_buffer = stream.doc_buffer;
    uint8_t * packed_buffer = stream.packed_buffer;

    UInt32 last_doc_id = 0;

    /// Align to TURBOPFOR_BLOCK_SIZE-doc blocks
    const UInt32 docs_per_large_block
        = (static_cast<UInt32>(index_params.posting_list_block_size) + TURBOPFOR_BLOCK_SIZE - 1) & ~(TURBOPFOR_BLOCK_SIZE - 1);
    const UInt32 large_doc_count = doc_count - 1;
    const UInt32 num_large_blocks = (large_doc_count + docs_per_large_block - 1) / docs_per_large_block;
    LargePostingBlockWriter block_writer(
        entry_wb,
        stream.plain_hashing,
        docs_per_large_block,
        index_params.has_block_index,
        stream,
        index_stream ? &index_stream->plain_hashing : nullptr,
        pos_writer);

    /// SequentialPositionReader: yields per-doc (freq, pos_deltas) in doc_id order.
    ///
    /// During init(), pre-reads all freq values from .pst (doc_deltas are decoded
    /// and discarded), then seeks .pst back so ReaderStreamCursor can reuse it.
    /// During nextDoc(), reads pos_deltas from .pos sequentially, one 256-element
    /// TurboPFor block at a time. Memory: O(doc_count) for freq + O(256) for pos buffer.
    struct SequentialPositionReader
    {
        LargePostingListReaderStream * pst_stream = nullptr;
        LargePostingListReaderStream * pos_stream = nullptr;
        const LargePostingBlockMetas * blocks = nullptr;
        UInt32 first_doc_freq = 0;
        UInt32 total_doc_count = 0;

        /// All freq values pre-read from .pst (index 0 = first non-first doc)
        std::vector<UInt32> all_freq;
        UInt32 freq_idx = 0;

        /// Pos decode buffer — 256-element TurboPFor blocks from .pos, consumed per-doc.
        /// Pointer is bound to pos_stream->pos_decoded inside init(); null until then.
        UInt32 * pos_decoded = nullptr;
        UInt32 pos_buf_pos = 0;
        UInt32 pos_buf_count = 0;

        /// Per-large-block pos tracking: .pos data is written per-LB with independent
        /// TurboPFor block layout (each LB ends with a tail block). We must read each
        /// LB's pos data independently and seek to the next LB's pos_start_offset at
        /// LB boundaries. Without this, loadNextPosBlock would try to decode a 256-block
        /// across an LB boundary where a tail block ends and a new full block begins.
        UInt32 lb_pos_remaining = 0; /// pos deltas remaining in current large block
        size_t cur_lb = 0;
        UInt32 docs_in_cur_lb = 0; /// docs consumed within current large block
        UInt32 docs_per_cur_lb = 0; /// total docs in current large block

        UInt32 docs_consumed = 0;
        UInt64 pending_pos_seek = UINT64_MAX;

        void init()
        {
            if (!pst_stream || !pos_stream || !blocks || blocks->empty())
                return;

            pos_decoded = pos_stream->scratch_b;

            all_freq.reserve(total_doc_count > 0 ? total_doc_count - 1 : 0);
            for (const auto & block : *blocks)
            {
                UInt32 block_docs = block.block_doc_count;
                pst_stream->seek(block.offset);
                auto & dbuf = pst_stream->decodeBuffer();

                UInt32 remaining = block_docs;
                UInt32 * init_discard = pst_stream->scratch_a;
                UInt32 * init_freq_buf = pst_stream->scratch_b;

                while (remaining > 0)
                {
                    UInt32 n = std::min(remaining, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));

                    /// Decode doc deltas (discard — we only need freq)
                    {
                        const uint8_t * p = dbuf.ptr();
                        const uint8_t * end = (n == TURBOPFOR_BLOCK_SIZE)
                            ? turbopfor::p4D1Dec256v32(p, TURBOPFOR_BLOCK_SIZE, init_discard, 0)
                            : turbopfor::p4D1Dec32(p, n, init_discard, 0);
                        dbuf.advance(static_cast<size_t>(end - p));
                    }

                    /// Decode freq values
                    {
                        const uint8_t * p = dbuf.ptr();
                        const uint8_t * end = (n == TURBOPFOR_BLOCK_SIZE) ? turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, init_freq_buf)
                                                                          : turbopfor::p4Dec32(p, n, init_freq_buf);
                        dbuf.advance(static_cast<size_t>(end - p));
                    }

                    all_freq.insert(all_freq.end(), init_freq_buf, init_freq_buf + n);
                    remaining -= n;
                }
            }

            pst_stream->seek((*blocks)[0].offset);
            pending_pos_seek = (*blocks)[0].pos_start_offset;

            /// Compute pos deltas for the first large block:
            /// first_doc_freq (for the separately stored first doc) + freq of first block_doc_count docs.
            lb_pos_remaining = first_doc_freq;
            docs_per_cur_lb = (*blocks)[0].block_doc_count;
            for (UInt32 i = 0; i < docs_per_cur_lb && i < all_freq.size(); ++i)
                lb_pos_remaining += all_freq[i];
        }

        void nextDoc(UInt32 & out_freq, std::vector<UInt32> & out_deltas)
        {
            out_deltas.clear();

            if (docs_consumed == 0)
            {
                ++docs_consumed;
                ++docs_in_cur_lb;
                out_freq = first_doc_freq;
                if (first_doc_freq > 0)
                    consumePos(first_doc_freq, out_deltas);
                return;
            }

            out_freq = (freq_idx < all_freq.size()) ? all_freq[freq_idx++] : 0;
            ++docs_consumed;
            ++docs_in_cur_lb;

            if (out_freq > 0)
                consumePos(out_freq, out_deltas);

            /// Check if we've consumed all docs in the current large block.
            /// The first LB has docs_per_cur_lb packed docs + 1 (first_doc), so
            /// total docs in LB 0 = docs_per_cur_lb + 1.
            /// Subsequent LBs have docs_per_cur_lb docs.
            UInt32 total_docs_in_lb = docs_per_cur_lb + (cur_lb == 0 ? 1 : 0);
            if (docs_in_cur_lb >= total_docs_in_lb && blocks && cur_lb + 1 < blocks->size())
            {
                advanceToNextLargeBlock();
            }
        }

    private:
        void advanceToNextLargeBlock()
        {
            ++cur_lb;
            docs_in_cur_lb = 0;
            docs_per_cur_lb = (*blocks)[cur_lb].block_doc_count;

            /// Seek to this LB's pos data
            pending_pos_seek = (*blocks)[cur_lb].pos_start_offset;
            pos_buf_pos = 0;
            pos_buf_count = 0;

            /// Compute pos deltas for this large block
            lb_pos_remaining = 0;
            UInt32 freq_start = 0;
            for (size_t b = 0; b < cur_lb; ++b)
                freq_start += (*blocks)[b].block_doc_count;
            for (UInt32 i = freq_start; i < freq_start + docs_per_cur_lb && i < all_freq.size(); ++i)
                lb_pos_remaining += all_freq[i];
        }

        void consumePos(UInt32 n, std::vector<UInt32> & out)
        {
            chassert(pos_stream && pos_decoded);
            if (pending_pos_seek != UINT64_MAX)
            {
                pos_stream->seek(pending_pos_seek);
                pending_pos_seek = UINT64_MAX;
            }
            while (n > 0)
            {
                if (pos_buf_pos >= pos_buf_count)
                    loadNextPosBlock();
                if (pos_buf_count == 0)
                    break;
                UInt32 avail = pos_buf_count - pos_buf_pos;
                UInt32 take = std::min(avail, n);
                out.insert(out.end(), pos_decoded + pos_buf_pos, pos_decoded + pos_buf_pos + take);
                pos_buf_pos += take;
                n -= take;
            }
        }

        void loadNextPosBlock()
        {
            if (lb_pos_remaining == 0)
            {
                pos_buf_count = 0;
                return;
            }
            UInt32 n = std::min(lb_pos_remaining, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));
            auto & dbuf = pos_stream->decodeBuffer();

            const uint8_t * p = dbuf.ptr();
            const uint8_t * end = nullptr;
            if (n == TURBOPFOR_BLOCK_SIZE)
                end = turbopfor::p4Dec256v32(p, TURBOPFOR_BLOCK_SIZE, pos_decoded);
            else
                end = turbopfor::p4Dec32(p, n, pos_decoded);
            dbuf.advance(static_cast<size_t>(end - p));

            pos_buf_count = n;
            pos_buf_pos = 0;
            lb_pos_remaining -= n;
        }
    };

    /// Build per-source position readers BEFORE constructing cursors.
    /// init() pre-reads freq values from .pst streams; doing this first ensures
    /// cursor construction sees clean stream state after init() seeks back.
    std::vector<SequentialPositionReader> pos_readers;
    std::unordered_map<LargePostingListReaderStream *, size_t> stream_to_reader;

    if (pos_writer)
    {
        for (const auto & entry : lazy->streams.entries)
        {
            if (!entry.pos_stream && entry.first_doc_freq == 0)
                continue;

            size_t reader_idx = pos_readers.size();
            pos_readers.emplace_back();
            auto & reader = pos_readers.back();

            if (entry.doc_count == 1 && entry.large_posting_blocks.empty())
            {
                reader.first_doc_freq = entry.first_doc_freq;
                reader.total_doc_count = 1;
                if (entry.pos_stream && entry.first_doc_freq > 0)
                {
                    reader.pos_stream = entry.pos_stream.get();
                    reader.pos_decoded = reader.pos_stream->scratch_b;
                    reader.pending_pos_seek = entry.first_doc_pos_offset;
                    reader.lb_pos_remaining = entry.first_doc_freq;
                }
            }
            else
            {
                reader.pst_stream = entry.stream.get();
                reader.pos_stream = entry.pos_stream.get();
                reader.blocks = &entry.large_posting_blocks;
                reader.first_doc_freq = entry.first_doc_freq;
                reader.total_doc_count = entry.doc_count;
                reader.init();
            }

            if (entry.stream)
                stream_to_reader[entry.stream.get()] = reader_idx;
        }
    }

    /// Construct merge cursors AFTER position reader init, so .pst streams
    /// are in a clean state (seeked to block start, decode buffer reset).
    ///
    /// Multi-doc embedded entries (`stream == nullptr`, `embedded_postings != nullptr`,
    /// `doc_count > 1`) cannot drive the disk-backed `ReaderStreamCursor` because the
    /// cursor constructor unconditionally dereferences `stream->doc_buffer`. They arise
    /// when an `AggregateFunctionPostingList::deserialize` state with `doc_count > 1`
    /// flows into a `write()` call (e.g. distributed aggregation followed by a write
    /// of the merged state). Reject deterministically with `NOT_IMPLEMENTED` instead of
    /// SIGSEGV; the caller must materialize embedded states into a disk-backed stream
    /// before the merge serialization path. Single-doc embedded entries are unaffected
    /// because the doc_count==1 fast path above already special-cases them.
    for (const auto & lazy_stream : lazy->streams)
    {
        if (!lazy_stream.stream && lazy_stream.embedded_postings && lazy_stream.doc_count > 1)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Projection text index: writing a merge-state with multi-document embedded posting list "
                "(doc_count={}, first_doc_id={}) is not yet supported. The embedded state must be "
                "materialised into a disk-backed posting stream before serialization.",
                lazy_stream.doc_count, lazy_stream.first_doc_id);
    }

    std::vector<ReaderStreamCursor> cursors;
    for (const auto & lazy_stream : lazy->streams)
    {
        if (lazy_stream.large_posting_blocks.empty() || lazy_stream.large_posting_blocks.front().block_doc_count == 0)
        {
            /// doc_count=1 entry: emit first_doc_id only, no .pst data to read.
            cursors.emplace_back(
                lazy_stream.stream.get(),
                lazy_stream.first_doc_id,
                0, /// remaining_count = 0 (no doc_deltas)
                0, /// offset (unused)
                false,
                true, /// include_first_doc
                pos_writer != nullptr);
            continue;
        }
        const auto & blocks = lazy_stream.large_posting_blocks;

        lazy_stream.stream->seek(blocks.front().offset);

        cursors.emplace_back(
            lazy_stream.stream.get(),
            lazy_stream.first_doc_id,
            lazy_stream.doc_count - 1,
            static_cast<UInt64>(lazy_stream.stream->getPosition()),
            false,
            true,
            pos_writer != nullptr);
    }

    auto lookup_reader = [&](LargePostingListReaderStream * src_stream) -> SequentialPositionReader *
    {
        if (!src_stream)
            return nullptr;
        auto it = stream_to_reader.find(src_stream);
        if (it != stream_to_reader.end())
            return &pos_readers[it->second];
        return nullptr;
    };

    UInt32 buffered = 0;
    UInt32 block_first_doc_id_merge = 0;
    UInt32 * freq_buffer_merge = stream.freq_accum;
    std::vector<UInt32> pos_deltas_buffer;
    UInt32 freq_buffered = 0;

    auto flush_block = [&]()
    {
        const char * freq_data = nullptr;
        UInt32 freq_bytes = 0;
        uint8_t freq_packed[TURBOPFOR_MAX_ENCODED_SIZE];
        if (pos_writer && freq_buffered > 0)
        {
            chassert(freq_buffered == TURBOPFOR_BLOCK_SIZE);
            uint8_t * freq_end = turbopfor::p4Enc256v32(freq_buffer_merge, TURBOPFOR_BLOCK_SIZE, freq_packed);
            freq_bytes = static_cast<UInt32>(freq_end - freq_packed);
            freq_data = reinterpret_cast<const char *>(freq_packed);
        }

        /// Capture .pos offset before writing pos data if we're at a new large block boundary.
        block_writer.initPosStartOffset();

        /// Feed position deltas BEFORE encoding doc deltas into packed_buffer,
        /// because feedPositionDeltas uses the same packed_buffer (via pos_encode_buf alias)
        /// as a scratch buffer for TurboPFor encoding of position data to .pos.
        if (!pos_deltas_buffer.empty())
            block_writer.feedPositionDeltas(pos_deltas_buffer.data(), static_cast<UInt32>(pos_deltas_buffer.size()));

        uint8_t * end = turbopfor::p4Enc256v32(doc_delta_buffer, TURBOPFOR_BLOCK_SIZE, packed_buffer);
        UInt32 doc_bytes = static_cast<UInt32>(end - packed_buffer);

        block_writer.addBlock(
            block_first_doc_id_merge,
            last_doc_id,
            reinterpret_cast<const char *>(packed_buffer),
            doc_bytes,
            freq_data,
            freq_bytes,
            static_cast<UInt32>(pos_deltas_buffer.size()));
        buffered = 0;
        freq_buffered = 0;
        pos_deltas_buffer.clear();
    };

    auto flush_tail = [&]()
    {
        if (buffered == 0)
            return;

        const char * freq_data = nullptr;
        UInt32 freq_bytes = 0;
        uint8_t freq_packed[TURBOPFOR_MAX_ENCODED_SIZE];
        if (pos_writer && freq_buffered > 0)
        {
            uint8_t * freq_end = turbopfor::p4Enc32(freq_buffer_merge, freq_buffered, freq_packed);
            freq_bytes = static_cast<UInt32>(freq_end - freq_packed);
            freq_data = reinterpret_cast<const char *>(freq_packed);
        }

        /// Capture .pos offset before writing pos data if we're at a new large block boundary.
        block_writer.initPosStartOffset();

        /// Feed position deltas BEFORE encoding doc deltas — same aliasing reason as flush_block.
        if (!pos_deltas_buffer.empty())
            block_writer.feedPositionDeltas(pos_deltas_buffer.data(), static_cast<UInt32>(pos_deltas_buffer.size()));

        uint8_t * end = turbopfor::p4Enc32(doc_delta_buffer, buffered, packed_buffer);
        UInt32 doc_bytes = static_cast<UInt32>(end - packed_buffer);

        block_writer.addBlock(
            block_first_doc_id_merge,
            last_doc_id,
            reinterpret_cast<const char *>(packed_buffer),
            doc_bytes,
            freq_data,
            freq_bytes,
            static_cast<UInt32>(pos_deltas_buffer.size()),
            buffered);
        buffered = 0;
        freq_buffered = 0;
        pos_deltas_buffer.clear();
    };

    mergePostingCursors(
        cursors,
        [&](UInt32 merged_first_doc_id, LargePostingListReaderStream * src_stream)
        {
            last_doc_id = merged_first_doc_id;
            VarInt::writeUInt32(merged_first_doc_id, entry_wb);

            if (pos_writer)
            {
                auto * reader = lookup_reader(src_stream);
                UInt32 freq = 0;
                std::vector<UInt32> deltas;
                if (reader)
                    reader->nextDoc(freq, deltas);
                VarInt::writeUInt32(freq, entry_wb);

                UInt64 pos_offset = static_cast<UInt64>(pos_writer->count());
                writeVarUInt(pos_offset, entry_wb);

                /// Capture .pos offset before writing first_doc's pos deltas,
                /// so that pos_start_offset includes first_doc's pos data.
                block_writer.initPosStartOffset();

                if (!deltas.empty())
                    block_writer.feedPositionDeltas(deltas.data(), static_cast<UInt32>(deltas.size()));

                /// Record first_doc_freq in `packed_block_pos_cum_deltas[0]` so the reader
                /// knows the first large block's pos data starts with this many extra deltas.
                block_writer.setPosBaseline(freq);
            }

            VarInt::writeUInt32(num_large_blocks, entry_wb);
        },
        [&](UInt32 doc_id, LargePostingListReaderStream * src_stream)
        {
            chassert(doc_id > last_doc_id);
            if (buffered == 0)
                block_first_doc_id_merge = doc_id;
            doc_delta_buffer[buffered++] = doc_id - last_doc_id - 1;
            last_doc_id = doc_id;

            if (pos_writer)
            {
                auto * reader = lookup_reader(src_stream);
                UInt32 freq = 0;
                std::vector<UInt32> deltas;
                if (reader)
                    reader->nextDoc(freq, deltas);
                freq_buffer_merge[freq_buffered++] = freq;
                pos_deltas_buffer.insert(pos_deltas_buffer.end(), deltas.begin(), deltas.end());
            }

            if (buffered == TURBOPFOR_BLOCK_SIZE)
                flush_block();
        });

    flush_tail();
    block_writer.finish(num_large_blocks);

    entry_wb.finalize();
    writeVarUInt(static_cast<UInt64>(entry_buf.size()), wb);
    wb.write(entry_buf.data(), entry_buf.size());
}
void PostingListStream::collect(UInt32 * buf) const
{
    if (doc_count == 0)
        return;

    /// --------------------------------------------
    /// Small posting list
    /// --------------------------------------------
    if (doc_count == 1)
    {
        UInt32 mapped_doc_id = first_doc_id;
        if (lazy && !lazy->streams.empty())
        {
            auto & s = lazy->streams.entries.front();
            if (s.stream && s.stream->merged_part_offsets)
                s.stream->merged_part_offsets->mapOffsets(s.stream->part_index, &mapped_doc_id, 1);
        }
        buf[0] = mapped_doc_id;
        return;
    }

    chassert(lazy);

    /// Check if all entries have embedded postings (deserialized from merge state).
    /// If so, collect directly from bitmaps without stream access.
    bool all_embedded = true;
    for (const auto & entry : lazy->streams.entries)
    {
        if (!entry.embedded_postings)
        {
            all_embedded = false;
            break;
        }
    }

    if (all_embedded)
    {
        UInt32 buffered = 0;
        for (const auto & entry : lazy->streams.entries)
        {
            auto it = entry.embedded_postings->begin();
            while (it != entry.embedded_postings->end())
            {
                buf[buffered++] = *it;
                ++it;
            }
        }
        return;
    }

    std::vector<ReaderStreamCursor> cursors;
    bool phrase_mode = first_doc_freq > 0;
    for (const auto & lazy_stream : lazy->streams)
    {
        if (lazy_stream.large_posting_blocks.empty() || lazy_stream.large_posting_blocks.front().block_doc_count == 0)
        {
            cursors.emplace_back(lazy_stream.stream.get(), lazy_stream.first_doc_id, 0, 0, false, true, phrase_mode);
            continue;
        }
        const auto & blocks = lazy_stream.large_posting_blocks;

        lazy_stream.stream->seek(blocks.front().offset);

        cursors.emplace_back(
            lazy_stream.stream.get(),
            lazy_stream.first_doc_id,
            lazy_stream.doc_count - 1,
            static_cast<UInt64>(lazy_stream.stream->getPosition()),
            false,
            true,
            phrase_mode);
    }

    UInt32 buffered = 0;
    auto emit = [&](UInt32 doc_id, LargePostingListReaderStream *) { buf[buffered++] = doc_id; };
    mergePostingCursors(cursors, emit, emit);
}

void PostingListStream::merge(PostingListStream & other)
{
    if (other.doc_count == 0)
        return;

    if (doc_count == 0)
    {
        first_doc_id = other.first_doc_id;
        first_doc_freq = other.first_doc_freq;
        lazy = other.lazy;
        other.lazy = nullptr;
        doc_count = other.doc_count;
        other.doc_count = 0;
        return;
    }

    if (!lazy)
        lazy = new LazyPostingStream();

    if (other.lazy)
        lazy->streams.merge(other.lazy->streams);

    doc_count += other.doc_count;
    other.doc_count = 0;
}

std::shared_ptr<LargeBlockData> LargeBlockData::decodeFromIndex(
    LargePostingListReaderStream & idx_stream,
    const LargePostingBlockMeta & meta,
    bool decode_positions)
{
    idx_stream.seek(meta.index_offset);
    auto & idx_buf = *idx_stream.getDataBuffer();

    UInt32 num_packed_blocks = 0;
    VarInt::readUInt32(num_packed_blocks, idx_buf);

    if (num_packed_blocks == 0)
        return nullptr;

    auto lb = std::make_shared<LargeBlockData>();
    lb->doc_count = meta.block_doc_count;
    lb->data_section_start = meta.offset;

    size_t full = lb->doc_count / TURBOPFOR_BLOCK_SIZE;
    lb->tail_size = lb->doc_count % TURBOPFOR_BLOCK_SIZE;
    lb->block_count = full + (lb->tail_size > 0 ? 1 : 0);

    /// `num_packed_blocks` is derived from the large block's `doc_count`; if the on-disk value
    /// disagrees with the count we just computed, treat it as corruption rather than allocating
    /// vectors sized off an untrusted varint.
    if (num_packed_blocks != lb->block_count) [[unlikely]]
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted projection text index: num_packed_blocks {} does not match derived block_count {} (doc_count={})",
            num_packed_blocks, lb->block_count, lb->doc_count);

    lb->packed_block_ranges.resize(num_packed_blocks * 2);
    lb->packed_block_cum_bytes.resize(num_packed_blocks);

    auto & dbuf = idx_stream.decodeBuffer();

    auto decode_delta1_32 = [&dbuf](UInt32 count, UInt32 * out, uint32_t prev)
    {
        UInt32 p = 0;
        UInt32 remaining = count;
        while (remaining >= TURBOPFOR_BLOCK_SIZE)
        {
            const uint8_t * ptr = dbuf.ptr();
            const uint8_t * end = turbopfor::p4D1Dec256v32(ptr, TURBOPFOR_BLOCK_SIZE, out + p, prev);
            dbuf.advance(static_cast<size_t>(end - ptr));
            prev = out[p + TURBOPFOR_BLOCK_SIZE - 1];
            p += TURBOPFOR_BLOCK_SIZE;
            remaining -= TURBOPFOR_BLOCK_SIZE;
        }
        if (remaining > 0)
        {
            const uint8_t * ptr = dbuf.ptr();
            const uint8_t * end = turbopfor::p4D1Dec32(ptr, remaining, out + p, prev);
            dbuf.advance(static_cast<size_t>(end - ptr));
        }
    };

    decode_delta1_32(num_packed_blocks * 2, lb->packed_block_ranges.data(), 0);
    decode_delta1_32(num_packed_blocks, lb->packed_block_cum_bytes.data(), static_cast<uint32_t>(-1));

    /// `packed_block_cum_bytes` is a strictly monotonic cumulative-bytes table; consumers do
    /// `cum[i] - cum[i-1]` to derive per-block sizes (see ProjectionTokenInfo.cpp). A corrupt
    /// non-monotonic decoded value would underflow that subtraction and drive `dbuf.advance`
    /// with an enormous "byte count". Validate once here on the cold lazy-load path.
    for (UInt32 i = 1; i < num_packed_blocks; ++i)
    {
        if (lb->packed_block_cum_bytes[i] <= lb->packed_block_cum_bytes[i - 1]) [[unlikely]]
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted projection text index: packed_block_cum_bytes is not strictly monotonic at index {} ({} <= {})",
                i, lb->packed_block_cum_bytes[i], lb->packed_block_cum_bytes[i - 1]);
    }

    if (decode_positions)
    {
        auto decode_delta1_64 = [&dbuf](UInt32 count, UInt64 * out, uint64_t prev)
        {
            UInt32 p = 0;
            UInt32 remaining = count;
            while (remaining >= TURBOPFOR_BLOCK_SIZE)
            {
                const uint8_t * ptr = dbuf.ptr();
                const uint8_t * end = turbopfor::p4D1Dec256v64(ptr, TURBOPFOR_BLOCK_SIZE, out + p, prev);
                dbuf.advance(static_cast<size_t>(end - ptr));
                prev = out[p + TURBOPFOR_BLOCK_SIZE - 1];
                p += TURBOPFOR_BLOCK_SIZE;
                remaining -= TURBOPFOR_BLOCK_SIZE;
            }
            if (remaining > 0)
            {
                const uint8_t * ptr = dbuf.ptr();
                const uint8_t * end = turbopfor::p4D1Dec64(ptr, remaining, out + p, prev);
                dbuf.advance(static_cast<size_t>(end - ptr));
            }
        };

        lb->pos_cum_deltas.resize(num_packed_blocks + 1);
        decode_delta1_64(num_packed_blocks + 1, lb->pos_cum_deltas.data(), static_cast<uint64_t>(-1));

        {
            const uint8_t * p = dbuf.ptr();
            const uint8_t * q = p;
            VarInt::readUInt32(lb->num_pos_blocks, q);
            dbuf.advance(static_cast<size_t>(q - p));
        }
        /// Cap `num_pos_blocks` against the total positions implied by `pos_cum_deltas`. Each
        /// position block holds `TURBOPFOR_BLOCK_SIZE` positions, so a sane upper bound is
        /// `ceil(total_positions / TURBOPFOR_BLOCK_SIZE)`. A corrupt count beyond that would
        /// drive an unbounded vector allocation below.
        if (lb->num_pos_blocks > 0)
        {
            const UInt64 total_positions = lb->pos_cum_deltas.back();
            const UInt64 max_pos_blocks = (total_positions + TURBOPFOR_BLOCK_SIZE - 1) / TURBOPFOR_BLOCK_SIZE;
            if (lb->num_pos_blocks > max_pos_blocks) [[unlikely]]
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted projection text index: num_pos_blocks {} exceeds maximum {} derived from total positions {}",
                    lb->num_pos_blocks, max_pos_blocks, total_positions);

            lb->pos_cum_bytes.resize(lb->num_pos_blocks);
            decode_delta1_64(lb->num_pos_blocks, lb->pos_cum_bytes.data(), static_cast<uint64_t>(-1));

            /// Same monotonicity invariant as packed_block_cum_bytes — consumers compute
            /// `pos_cum_bytes[i] - pos_cum_bytes[i-1]` to derive per-block byte sizes.
            for (UInt32 i = 1; i < lb->num_pos_blocks; ++i)
            {
                if (lb->pos_cum_bytes[i] <= lb->pos_cum_bytes[i - 1]) [[unlikely]]
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Corrupted projection text index: pos_cum_bytes is not strictly monotonic at index {} ({} <= {})",
                        i, lb->pos_cum_bytes[i], lb->pos_cum_bytes[i - 1]);
            }
        }
        lb->pos_start_offset = meta.pos_start_offset;
        lb->has_positions = true;
    }

    return lb;
}

}
