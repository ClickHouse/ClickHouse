#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Compression/PFor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cstring>
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
    TextIndexBlockedPositionsCodec::DecodeScratch & scratch)
{
    const uint8_t * pos = payload;
    const uint8_t * end = payload + payload_bytes;
    auto & doc_offsets = scratch.doc_offsets;
    doc_offsets.resize(docs_in_block + 1);

    const UInt64 freq_bytes = readVarUIntFrom(pos, end);
    if (freq_bytes > static_cast<UInt64>(end - pos))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: frequency run of {} bytes exceeds the {}-byte block", freq_bytes, payload_bytes);

    /// PFor emits >= 1 byte per block, so payload size bounds the value count (reject before alloc).
    const UInt64 max_total = payload_bytes * PFor::BLOCK;
    UInt64 total = 0;
    if (freq_bytes == 0)
    {
        std::iota(doc_offsets.begin(), doc_offsets.begin() + docs_in_block, UInt64{0});
        total = docs_in_block;
    }
    else
    {
        scratch.freqs.resize(docs_in_block);
        if (PFor::decodeBlocks<UInt32>(pos, docs_in_block, PFor::Delta::none, scratch.freqs.data(), pos + freq_bytes) != freq_bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions: malformed frequency run");
        pos += freq_bytes;

        for (size_t i = 0; i < docs_in_block; ++i)
        {
            doc_offsets[i] = total;
            total += UInt64{scratch.freqs[i]} + 1;
            /// Positions are addressed with UInt32 offsets.
            if (total > std::min<UInt64>(max_total, std::numeric_limits<UInt32>::max()))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupt text index positions: frequencies exceed the {} positions a {}-byte block can hold", max_total, payload_bytes);
        }
    }
    doc_offsets[docs_in_block] = total;

    scratch.values.resize(total);
    const size_t consumed = PFor::decodeBlocks<UInt32>(pos, total, PFor::Delta::none, scratch.values.data(), end);
    if (consumed == 0 || pos + consumed != end)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: malformed position lane ({} of {} payload bytes consumed)",
            static_cast<size_t>(pos - payload) + consumed, payload_bytes);
}

}


void TextIndexBlockedPositionsCodec::Encoder::finishDocument(bool sorted)
{
    auto * first = values.begin() + open_document_begin;
    auto * last = values.end();
    chassert(first != last);

    if (!sorted)
    {
        std::sort(first, last);
        last = std::unique(first, last);
        values.resize(last - values.begin());
    }

    const size_t freq = last - first;

    /// Decoders address the whole-token stream with UInt32 offsets; refuse to write an unreadable part.
    num_positions += freq;
    if (num_positions > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Text index positions: more than {} positions for a single token", std::numeric_limits<UInt32>::max());

    /// The first position of a document is stored absolute, the rest as deltas.
    std::adjacent_difference(first, last, first);

    frequencies_minus_one.push_back(static_cast<UInt32>(freq - 1));
    has_frequency_run |= freq > 1;

    ++docs_in_block;
    ++num_docs;
    open_document_begin = values.size();

    if (docs_in_block == BLOCK_DOCS)
        block_bytes.push_back(encodeBlock(staged));
}

void TextIndexBlockedPositionsCodec::Encoder::addDocument(std::span<const UInt32> positions)
{
    chassert(!positions.empty());
    chassert(std::ranges::adjacent_find(positions, std::greater_equal<UInt32>{}) == positions.end());

    values.insert(positions.begin(), positions.end());
    finishDocument(true);
}

size_t TextIndexBlockedPositionsCodec::Encoder::encodeBlock(PaddedPODArray<UInt8> & out)
{
    const size_t max_freq_bytes = PFor::maxCompressedBytes<UInt32>(docs_in_block);

    /// The frequency size precedes the run, so the run is encoded to `runs` first.
    runs.resize(max_freq_bytes);
    auto * runs_data = reinterpret_cast<uint8_t *>(runs.data());

    const size_t begin = out.size();
    out.resize(begin + 10 + max_freq_bytes + PFor::maxCompressedBytes<UInt32>(values.size()));
    auto * block_begin = reinterpret_cast<char *>(out.data() + begin);

    const size_t freq_bytes = has_frequency_run
        ? PFor::encodeBlocks<UInt32>(std::span<const UInt32>(frequencies_minus_one.data(), frequencies_minus_one.size()), PFor::Delta::none, runs_data)
        : 0;
    size_t offset = writeVarUInt(freq_bytes, block_begin) - block_begin;
    memcpy(block_begin + offset, runs_data, freq_bytes);
    offset += freq_bytes;
    offset += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(values.data(), values.size()), PFor::Delta::none, reinterpret_cast<uint8_t *>(block_begin + offset));

    chassert(offset <= out.size() - begin);
    out.resize(begin + offset);
    values.clear();
    frequencies_minus_one.clear();
    has_frequency_run = false;
    open_document_begin = 0;
    docs_in_block = 0;
    return offset;
}

void TextIndexBlockedPositionsCodec::Encoder::finalize(WriteBuffer & out)
{
    /// Freed on return, so the allocator reuses it for the next token.
    PaddedPODArray<UInt8> last_block;
    const size_t num_blocks = block_bytes.size() + (docs_in_block != 0);
    const size_t last_block_bytes = docs_in_block != 0 ? encodeBlock(last_block) : 0;

    writeVarUInt(num_docs, out);
    writeVarUInt(num_blocks, out);
    for (UInt64 bytes : block_bytes)
        writeVarUInt(bytes, out);
    if (num_blocks != block_bytes.size())
        writeVarUInt(last_block_bytes, out);
    out.write(reinterpret_cast<const char *>(staged.data()), staged.size());
    out.write(reinterpret_cast<const char *>(last_block.data()), last_block.size());
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

    /// Every block holds >= 1 document: a 1-byte frequency size + >= 1 byte of PFor.
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
    readBlock(in, dir, block_idx, scratch);
    emitRanks(scratch, local_ranks, offsets, positions);
}

void TextIndexBlockedPositionsCodec::readBlock(ReadBuffer & in, const Directory & dir, size_t block_idx, DecodeScratch & scratch)
{
    const size_t payload_bytes = dir.block_offsets[block_idx + 1] - dir.block_offsets[block_idx];
    scratch.payload.resize(payload_bytes);
    in.readStrict(scratch.payload.data(), payload_bytes);

    parseBlock(reinterpret_cast<const uint8_t *>(scratch.payload.data()), payload_bytes, dir.docsInBlock(block_idx), scratch);
}

void TextIndexBlockedPositionsCodec::emitRanks(
    const DecodeScratch & scratch,
    std::span<const UInt32> local_ranks,
    PaddedPODArray<UInt32> & offsets,
    PaddedPODArray<UInt32> & positions)
{
    const size_t num_positions = std::accumulate(local_ranks.begin(), local_ranks.end(), size_t{},
        [&scratch](size_t sum, UInt32 rank) { return sum + (scratch.doc_offsets[rank + 1] - scratch.doc_offsets[rank]); });

    const size_t base = positions.size();
    positions.resize(base + num_positions);
    UInt32 * out = positions.data() + base;
    offsets.reserve(offsets.size() + local_ranks.size());

    for (UInt32 rank : local_ranks)
    {
        const UInt64 lane_end = scratch.doc_offsets[rank + 1];
        /// Every document has at least one position, and its first one is absolute; the rest are deltas.
        UInt32 position = scratch.values[scratch.doc_offsets[rank]];
        *out++ = position;
        for (UInt64 k = scratch.doc_offsets[rank] + 1; k < lane_end; ++k)
        {
            /// The writer emits strictly increasing positions, so a zero delta or a wrap means corruption.
            const UInt32 delta = scratch.values[k];
            if ((delta == 0) || (position > std::numeric_limits<UInt32>::max() - delta))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupt text index positions: positions do not increase within a document");
            position += delta;
            *out++ = position;
        }

        offsets.push_back(static_cast<UInt32>(out - positions.data()));
    }
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

void validateTokenPositions(const TokenPostingsInfo & info, size_t file_size)
{
    if (!(info.header & PostingsSerialization::Flags::HasPositions))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data in text index: a token with {} postings has no positions", info.cardinality);

    if ((info.position_bytes == 0)
        || (info.position_offset > file_size)
        || (info.position_bytes > file_size - info.position_offset))
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: blob of {} bytes at offset {} is outside the {}-byte stream",
            info.position_bytes, info.position_offset, file_size);
    }
}

}
