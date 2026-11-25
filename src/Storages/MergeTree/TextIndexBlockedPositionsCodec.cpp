#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>

#include <Compression/PFor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <bit>
#include <limits>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Varint into a staged buffer — payloads are assembled before the directory that sizes them.
size_t writeVarUIntTo(uint8_t * out, UInt64 v)
{
    size_t n = 0;
    while (v >= 0x80)
    {
        out[n++] = static_cast<uint8_t>(v) | 0x80;
        v >>= 7;
    }
    out[n++] = static_cast<uint8_t>(v);
    return n;
}

UInt64 readVarUIntFrom(const uint8_t *& pos, const uint8_t * end)
{
    UInt64 value = 0;
    int shift = 0;
    while (true)
    {
        if (pos >= end)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions: truncated varint in block payload");
        const uint8_t byte = *pos++;
        /// Only bit 63 fits at the last shift, so a larger payload or a continuation would truncate silently.
        if (shift == 63 && byte > 1)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions: overlong varint in block payload");
        value |= static_cast<UInt64>(byte & 0x7f) << shift;
        if (!(byte & 0x80))
            return value;
        shift += 7;
    }
}

/// Parse a block payload into the delta value lane and per-rank bounds: rank r spans [doc_offsets[r], doc_offsets[r + 1]).
/// The bounds are derived here and never persisted, so they are UInt64 and put no ceiling on a block's position count.
void parseBlock(
    const uint8_t * payload,
    size_t payload_bytes,
    size_t docs_in_block,
    PaddedPODArray<UInt64> & doc_offsets,
    PaddedPODArray<UInt32> & values)
{
    const uint8_t * pos = payload;
    const uint8_t * end = payload + payload_bytes;

    doc_offsets.resize(docs_in_block + 1);
    for (size_t i = 0; i < docs_in_block; ++i)
        doc_offsets[i] = 1;

    const UInt64 num_exceptions = readVarUIntFrom(pos, end);
    if (num_exceptions > docs_in_block)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: {} frequency exceptions for {} documents", num_exceptions, docs_in_block);

    /// PFor emits >= 1 byte per block, so payload size bounds the value count (reject before alloc).
    const UInt64 max_total = docs_in_block + payload_bytes * PFor::BLOCK;

    UInt64 total = docs_in_block;
    UInt64 prev_rank = 0;
    for (UInt64 e = 0; e < num_exceptions; ++e)
    {
        const UInt64 local_rank = readVarUIntFrom(pos, end);
        const UInt64 freq = readVarUIntFrom(pos, end);
        if (local_rank >= docs_in_block || (e > 0 && local_rank <= prev_rank))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: exception rank {} out of order or out of range {}", local_rank, docs_in_block);
        if ((freq < 2) || (freq > std::numeric_limits<UInt32>::max()) || (total + (freq - 1) > max_total))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: invalid frequency {} (total would exceed {})", freq, max_total);
        doc_offsets[local_rank] = freq;
        total += freq - 1;
        prev_rank = local_rank;
    }

    /// Frequencies become exclusive prefix sums. The tail entry closes the last document's slice.
    std::exclusive_scan(doc_offsets.begin(), doc_offsets.begin() + docs_in_block, doc_offsets.begin(), UInt64{});
    doc_offsets[docs_in_block] = total;

    values.resize(total);
    const size_t consumed = PFor::decodeBlocks<UInt32>(pos, total, PFor::Delta::none, values.data(), end);
    if (consumed == 0 || pos + consumed != end)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: malformed position lane ({} of {} payload bytes consumed)",
            static_cast<size_t>(pos - payload) + consumed, payload_bytes);
}

/// Append each requested document's absolute positions to `positions`, and its end offset to `offsets`.
void emitRanks(
    const PaddedPODArray<UInt64> & doc_offsets,
    const PaddedPODArray<UInt32> & values,
    std::span<const UInt32> local_ranks,
    PaddedPODArray<UInt32> & offsets,
    PaddedPODArray<UInt32> & positions)
{
    const size_t num_positions = std::accumulate(local_ranks.begin(), local_ranks.end(), size_t{},
        [&doc_offsets](size_t sum, UInt32 rank) { return sum + (doc_offsets[rank + 1] - doc_offsets[rank]); });

    const size_t base = positions.size();
    positions.resize(base + num_positions);
    UInt32 * out = positions.data() + base;
    offsets.reserve(offsets.size() + local_ranks.size());

    for (UInt32 rank : local_ranks)
    {
        const UInt64 lane_end = doc_offsets[rank + 1];
        /// Every document has at least one position, and its first one is absolute; the rest are deltas.
        UInt32 position = values[doc_offsets[rank]];
        *out++ = position;
        for (UInt64 k = doc_offsets[rank] + 1; k < lane_end; ++k)
        {
            /// The writer emits strictly increasing positions, so a zero delta or a wrap means corruption.
            const UInt32 delta = values[k];
            if ((delta == 0) || (position > std::numeric_limits<UInt32>::max() - delta))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupt text index positions: positions do not increase within a document");
            position += delta;
            *out++ = position;
        }

        offsets.push_back(static_cast<UInt32>(out - positions.data()));
    }
}

}


void TextIndexBlockedPositionsCodec::encode(std::span<const RoaringishEntry> entries, WriteBuffer & out)
{
    /// Flatten the sorted roaringish buckets into per-document freqs + within-document delta positions.
    PaddedPODArray<UInt32> freqs;
    PaddedPODArray<UInt32> values;
    UInt32 current_doc = 0;
    UInt32 prev_position = 0;
    bool has_doc = false;
    for (const auto & entry : entries)
    {
        if (entry.bitmap == 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Text index positions: empty bitmap in roaringish entry");

        if (!has_doc || entry.doc_id != current_doc)
        {
            current_doc = entry.doc_id;
            has_doc = true;
            freqs.push_back(0);
        }
        const UInt32 base = entry.group * RoaringishEntry::BITMAP_BITS;
        UInt32 bitmap = entry.bitmap;
        while (bitmap)
        {
            const UInt32 position = base + static_cast<UInt32>(std::countr_zero(bitmap));
            values.push_back(freqs.back() == 0 ? position : position - prev_position);
            prev_position = position;
            ++freqs.back();
            bitmap &= bitmap - 1;
        }
    }

    /// Decoders address the whole-token stream with UInt32 offsets; refuse to write an unreadable part.
    if (values.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Text index positions: more than {} positions for a single token", std::numeric_limits<UInt32>::max());

    const UInt64 num_docs = freqs.size();
    const UInt64 num_blocks = (num_docs + BLOCK_DOCS - 1) / BLOCK_DOCS;
    writeVarUInt(num_docs, out);
    writeVarUInt(num_blocks, out);
    if (num_docs == 0)
        return;

    /// Stage the block payloads to learn their sizes; the directory precedes them.
    std::vector<size_t> block_totals(num_blocks);
    std::vector<size_t> block_bytes(num_blocks);
    std::vector<uint8_t> staged;
    {
        size_t reserve_bytes = 0;
        size_t doc_index = 0;
        for (UInt64 b = 0; b < num_blocks; ++b)
        {
            const size_t docs_in_block = std::min<size_t>(BLOCK_DOCS, num_docs - b * BLOCK_DOCS);
            size_t total = 0;
            for (size_t i = 0; i < docs_in_block; ++i)
                total += freqs[doc_index++];
            block_totals[b] = total;
            reserve_bytes += 10 + docs_in_block * 10 + PFor::maxCompressedBytes<UInt32>(total);
        }
        staged.resize(reserve_bytes);
    }

    size_t staged_offset = 0;
    size_t value_offset = 0;
    for (UInt64 b = 0; b < num_blocks; ++b)
    {
        const size_t doc_begin = b * BLOCK_DOCS;
        const size_t docs_in_block = std::min<size_t>(BLOCK_DOCS, num_docs - doc_begin);
        uint8_t * block_begin = staged.data() + staged_offset;
        size_t offset = 0;

        const size_t total = block_totals[b];
        size_t num_exceptions = 0;
        for (size_t i = 0; i < docs_in_block; ++i)
            if (freqs[doc_begin + i] > 1)
                ++num_exceptions;
        offset += writeVarUIntTo(block_begin + offset, num_exceptions);
        for (size_t i = 0; i < docs_in_block; ++i)
        {
            if (freqs[doc_begin + i] > 1)
            {
                offset += writeVarUIntTo(block_begin + offset, i);
                offset += writeVarUIntTo(block_begin + offset, freqs[doc_begin + i]);
            }
        }

        offset += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(values.data() + value_offset, total), PFor::Delta::none, block_begin + offset);
        value_offset += total;

        block_bytes[b] = offset;
        staged_offset += offset;
    }
    chassert(value_offset == values.size());

    for (UInt64 b = 0; b < num_blocks; ++b)
        writeVarUInt(block_bytes[b], out);
    out.write(reinterpret_cast<const char *>(staged.data()), staged_offset);
}

TextIndexBlockedPositionsCodec::Directory TextIndexBlockedPositionsCodec::readDirectory(
    ReadBuffer & in, UInt64 blob_offset, UInt64 expected_num_docs, size_t available_bytes)
{
    const size_t count_before = in.count();

    Directory dir;
    readVarUInt(dir.num_docs, in);
    if (dir.num_docs != expected_num_docs)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: stored document count {} does not match the posting list ({})",
            dir.num_docs, expected_num_docs);

    UInt64 num_blocks = 0;
    readVarUInt(num_blocks, in);
    if (num_blocks != (dir.num_docs + BLOCK_DOCS - 1) / BLOCK_DOCS)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: {} blocks for {} documents", num_blocks, dir.num_docs);
    if (dir.num_docs == 0)
        return dir;

    /// Every block holds >= 1 document: 1 byte exception count + >= 1 byte PFor lane.
    if (num_blocks * 2 > available_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: {} blocks cannot fit into {} available bytes", num_blocks, available_bytes);

    dir.block_offsets.resize(num_blocks + 1);
    UInt64 payload_total = 0;
    for (UInt64 b = 0; b < num_blocks; ++b)
    {
        UInt64 bytes = 0;
        readVarUInt(bytes, in);
        if (bytes < 2 || bytes > available_bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: block payload of {} bytes outside the valid range", bytes);
        dir.block_offsets[b] = payload_total; /// relative for now
        payload_total += bytes;
    }
    dir.block_offsets[num_blocks] = payload_total;

    const size_t directory_bytes = in.count() - count_before;
    if (directory_bytes + payload_total > available_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: declared size {} exceeds {} bytes available for this token",
            directory_bytes + payload_total, available_bytes);

    const UInt64 payload_start = blob_offset + directory_bytes;
    for (auto & offset : dir.block_offsets)
        offset += payload_start;

    return dir;
}

void TextIndexBlockedPositionsCodec::decodeBlock(
    ReadBuffer & in,
    const Directory & dir,
    size_t block_idx,
    std::span<const UInt32> local_ranks,
    PaddedPODArray<UInt32> & offsets,
    PaddedPODArray<UInt32> & positions,
    DecodeScratch & scratch)
{
    const size_t payload_bytes = dir.block_offsets[block_idx + 1] - dir.block_offsets[block_idx];
    scratch.payload.resize(payload_bytes);
    in.readStrict(scratch.payload.data(), payload_bytes);

    parseBlock(reinterpret_cast<const uint8_t *>(scratch.payload.data()), payload_bytes,
               dir.docsInBlock(block_idx), scratch.doc_offsets, scratch.values);
    emitRanks(scratch.doc_offsets, scratch.values, local_ranks, offsets, positions);
}

void TextIndexBlockedPositionsCodec::decodeAll(
    ReadBuffer & in,
    UInt64 expected_num_docs,
    size_t available_bytes,
    PaddedPODArray<UInt32> & doc_offsets,
    PaddedPODArray<UInt32> & positions,
    DecodeScratch & scratch)
{
    /// Sequential read: directory + every block (offsets stay token-relative, unused here).
    const auto dir = readDirectory(in, /*blob_offset=*/ 0, expected_num_docs, available_bytes);

    doc_offsets.clear();
    positions.clear();
    doc_offsets.push_back(0);

    std::vector<UInt32> all_ranks(BLOCK_DOCS);
    for (size_t b = 0; b < dir.numBlocks(); ++b)
    {
        const size_t docs_in_block = dir.docsInBlock(b);
        all_ranks.resize(docs_in_block);
        for (size_t i = 0; i < docs_in_block; ++i)
            all_ranks[i] = static_cast<UInt32>(i);
        decodeBlock(in, dir, b, all_ranks, doc_offsets, positions, scratch);
    }

    chassert(doc_offsets.size() == expected_num_docs + 1);
}

}
