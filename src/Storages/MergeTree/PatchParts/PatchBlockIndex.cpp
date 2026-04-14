#include <Storages/MergeTree/PatchParts/PatchBlockIndex.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

#include <codecfactory.h>
#include <deltautil.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

PatchBlockIndex::PatchBlockIndex() = default;
PatchBlockIndex::~PatchBlockIndex() = default;

// ───────────────────── Writer ─────────────────────

void PatchBlockIndexWriter::addRows(
    const PaddedPODArray<UInt64> & block_numbers,
    const PaddedPODArray<UInt64> & block_offsets,
    UInt32 base_row_index,
    size_t num_rows)
{
    rows.reserve(rows.size() + num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        rows.push_back({block_numbers[i], block_offsets[i], static_cast<UInt32>(base_row_index + i)});
}

/// SIMDFastPFor processes data in blocks of 128 uint32_t's using SIMD instructions.
/// Both encode and decode require input/output buffers padded to a multiple of this.
static constexpr size_t PFOR_BLOCK_SIZE = 128;

static size_t padToBlock(size_t n)
{
    return (n + PFOR_BLOCK_SIZE - 1) / PFOR_BLOCK_SIZE * PFOR_BLOCK_SIZE;
}

/// Encode a uint32_t array with PFOR compression. Returns the compressed output.
/// The input array is padded to PFOR_BLOCK_SIZE with zeros if needed.
static PaddedPODArray<uint32_t> pforEncode(
    FastPForLib::IntegerCODEC & codec,
    PaddedPODArray<uint32_t> & values)
{
    size_t padded_size = padToBlock(values.size());
    values.resize_fill(padded_size, 0);

    PaddedPODArray<uint32_t> compressed(padded_size * 2 + 1024);
    size_t compressed_size = compressed.size();
    codec.encodeArray(values.data(), padded_size, compressed.data(), compressed_size);
    compressed.resize(compressed_size);
    return compressed;
}

void PatchBlockIndexWriter::finalize(
    IDataPartStorage & storage,
    MergeTreeDataPartChecksums & checksums)
{
    if (rows.empty())
        return;

    auto codec = FastPForLib::simdfastpfor128_codec();

    /// 1. Sort by (block_number, block_offset).
    std::sort(rows.begin(), rows.end(), [](const Row & a, const Row & b)
    {
        if (a.block_number != b.block_number)
            return a.block_number < b.block_number;
        return a.block_offset < b.block_offset;
    });

    /// 2. Group consecutive rows by block_number.
    struct Group
    {
        UInt64 block_number;
        size_t begin;
        size_t end;
    };

    std::vector<Group> groups;
    size_t i = 0;
    while (i < rows.size())
    {
        size_t j = i;
        while (j < rows.size() && rows[j].block_number == rows[i].block_number)
            ++j;
        groups.push_back({rows[i].block_number, i, j});
        i = j;
    }

    /// 3. Write .bin and collect index entries for .idx.
    auto bin_out_raw = storage.writeFile(PatchBlockIndex::BIN_FILE, 4096, {});
    HashingWriteBuffer bin_out(*bin_out_raw);

    std::vector<PatchBlockIndexEntry> entries;
    entries.reserve(groups.size());

    for (const auto & group : groups)
    {
        size_t group_size = group.end - group.begin;

        PatchBlockIndexEntry entry;
        entry.block_number = group.block_number;
        entry.num_offsets = static_cast<UInt32>(group_size);
        entry.min_offset = rows[group.begin].block_offset;
        entry.max_offset = rows[group.end - 1].block_offset;
        entry.byte_offset = bin_out.count();

        /// Split into chunks of CHUNK_SIZE.
        size_t num_chunks = (group_size + PatchBlockIndex::CHUNK_SIZE - 1) / PatchBlockIndex::CHUNK_SIZE;
        writeIntBinary(static_cast<UInt32>(num_chunks), bin_out);

        for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
        {
            size_t chunk_begin = group.begin + chunk_idx * PatchBlockIndex::CHUNK_SIZE;
            size_t chunk_end = std::min(chunk_begin + PatchBlockIndex::CHUNK_SIZE, group.end);
            size_t chunk_size = chunk_end - chunk_begin;

            UInt64 chunk_max_offset = rows[chunk_end - 1].block_offset;
            UInt32 chunk_last_row_index = rows[chunk_end - 1].row_index;

            /// Write chunk header.
            writeIntBinary(chunk_max_offset, bin_out);
            writeIntBinary(chunk_last_row_index, bin_out);
            writeIntBinary(static_cast<UInt32>(chunk_size), bin_out);

            /// Delta-encode offsets: compute deltas from sorted offsets.
            /// First value is delta from previous chunk's last offset (or min_offset for first chunk).
            PaddedPODArray<uint32_t> offset_deltas(chunk_size);
            UInt64 prev_offset = (chunk_idx == 0) ? entry.min_offset : rows[chunk_begin - 1].block_offset;
            for (size_t k = 0; k < chunk_size; ++k)
            {
                UInt64 delta = rows[chunk_begin + k].block_offset - prev_offset;
                offset_deltas[k] = static_cast<uint32_t>(delta);
                prev_offset = rows[chunk_begin + k].block_offset;
            }

            auto compressed_offsets = pforEncode(*codec, offset_deltas);
            writeIntBinary(static_cast<UInt32>(compressed_offsets.size()), bin_out);
            bin_out.write(reinterpret_cast<const char *>(compressed_offsets.data()),
                         compressed_offsets.size() * sizeof(uint32_t));

            /// Row indices are not monotonically increasing after sorting by (block_number, block_offset),
            /// so store raw values and use PFOR directly (not delta-encoded).
            PaddedPODArray<uint32_t> row_index_values(chunk_size);
            for (size_t k = 0; k < chunk_size; ++k)
                row_index_values[k] = rows[chunk_begin + k].row_index;

            auto compressed_row_indices = pforEncode(*codec, row_index_values);
            writeIntBinary(static_cast<UInt32>(compressed_row_indices.size()), bin_out);
            bin_out.write(reinterpret_cast<const char *>(compressed_row_indices.data()),
                         compressed_row_indices.size() * sizeof(uint32_t));
        }

        entries.push_back(entry);
    }

    bin_out.finalize();
    checksums.files[PatchBlockIndex::BIN_FILE].file_size = bin_out.count();
    checksums.files[PatchBlockIndex::BIN_FILE].file_hash = bin_out.getHash();
    bin_out_raw->preFinalize();
    bin_out_raw->finalize();

    /// 4. Write .idx.
    auto idx_out_raw = storage.writeFile(PatchBlockIndex::IDX_FILE, 4096, {});
    HashingWriteBuffer idx_out(*idx_out_raw);

    writeIntBinary(PatchBlockIndex::FORMAT_VERSION, idx_out);
    writeIntBinary(static_cast<UInt32>(entries.size()), idx_out);

    for (const auto & entry : entries)
    {
        writeIntBinary(entry.block_number, idx_out);
        writeIntBinary(entry.num_offsets, idx_out);
        writeIntBinary(entry.min_offset, idx_out);
        writeIntBinary(entry.max_offset, idx_out);
        writeIntBinary(entry.byte_offset, idx_out);
    }

    idx_out.finalize();
    checksums.files[PatchBlockIndex::IDX_FILE].file_size = idx_out.count();
    checksums.files[PatchBlockIndex::IDX_FILE].file_hash = idx_out.getHash();
    idx_out_raw->preFinalize();
    idx_out_raw->finalize();
}

// ───────────────────── Reader ─────────────────────

void PatchBlockIndex::load(const IDataPartStorage & storage)
{
    codec = FastPForLib::simdfastpfor128_codec();

    /// 1. Read .idx entirely.
    {
        auto idx_in = storage.readFile(IDX_FILE, {}, std::nullopt);

        UInt8 version = 0;
        readIntBinary(version, *idx_in);
        if (version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unsupported patch block index version: {}", static_cast<int>(version));

        UInt32 num_groups = 0;
        readIntBinary(num_groups, *idx_in);

        entries.resize(num_groups);
        for (UInt32 g = 0; g < num_groups; ++g)
        {
            auto & entry = entries[g];
            readIntBinary(entry.block_number, *idx_in);
            readIntBinary(entry.num_offsets, *idx_in);
            readIntBinary(entry.min_offset, *idx_in);
            readIntBinary(entry.max_offset, *idx_in);
            readIntBinary(entry.byte_offset, *idx_in);
        }
    }

    /// 2. Read entire .bin into a single contiguous buffer (zero-copy for cursors).
    {
        auto bin_in = storage.readFile(BIN_FILE, {}, std::nullopt);

        /// Determine file size from last group's byte_offset + data.
        /// Just read until EOF into the string.
        WriteBufferFromString wb(bin_data);
        copyData(*bin_in, wb);
    }

    is_loaded = true;
}

UInt64 PatchBlockIndex::minBlockNumber() const
{
    if (entries.empty())
        return 0;
    return entries.front().block_number;
}

UInt64 PatchBlockIndex::maxBlockNumber() const
{
    if (entries.empty())
        return 0;
    return entries.back().block_number;
}

const PatchBlockIndexEntry * PatchBlockIndex::findGroup(UInt64 block_number) const
{
    auto it = std::lower_bound(entries.begin(), entries.end(), block_number,
        [](const PatchBlockIndexEntry & e, UInt64 bn) { return e.block_number < bn; });

    if (it != entries.end() && it->block_number == block_number)
        return &*it;
    return nullptr;
}

PatchBlockIndex::GroupCursor PatchBlockIndex::createCursor(const PatchBlockIndexEntry & entry)
{
    GroupCursor cursor;

    /// Point directly into the contiguous .bin buffer at this group's byte offset.
    cursor.chunk_ptr = bin_data.data() + entry.byte_offset;
    cursor.chunk_end = bin_data.data() + bin_data.size();
    cursor.last_offset = entry.min_offset;
    cursor.last_row_index = 0;

    /// Read num_chunks from the buffer.
    UInt32 num_chunks = 0;
    memcpy(&num_chunks, cursor.chunk_ptr, sizeof(UInt32));
    cursor.chunk_ptr += sizeof(UInt32);
    cursor.chunks_remaining = num_chunks;

    /// Decompress the first chunk.
    if (cursor.chunks_remaining > 0)
        cursor.decompressNextChunk(*codec);

    return cursor;
}

// ───────────────────── GroupCursor ─────────────────────

bool PatchBlockIndex::GroupCursor::decompressNextChunk(FastPForLib::IntegerCODEC & codec_ref)
{
    if (chunks_remaining == 0)
        return false;

    /// Read chunk header.
    UInt64 max_offset = 0;
    UInt32 chunk_last_row_index = 0;
    UInt32 num_values = 0;

    memcpy(&max_offset, chunk_ptr, sizeof(UInt64));
    chunk_ptr += sizeof(UInt64);
    memcpy(&chunk_last_row_index, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);
    memcpy(&num_values, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);

    /// Read and decode compressed offsets.
    UInt32 compressed_offsets_count = 0;
    memcpy(&compressed_offsets_count, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);

    const auto * compressed_offsets_data = reinterpret_cast<const uint32_t *>(chunk_ptr);
    chunk_ptr += compressed_offsets_count * sizeof(uint32_t);

    /// Decode buffers must be padded to PFOR block size (128) because
    /// SIMDFastPFor writes full SIMD blocks during decompression.
    size_t padded = padToBlock(num_values);
    PaddedPODArray<uint32_t> decoded_offset_deltas(padded, 0);
    size_t decoded_count = padded;
    codec_ref.decodeArray(compressed_offsets_data, compressed_offsets_count,
                          decoded_offset_deltas.data(), decoded_count);

    /// Reconstruct offsets from deltas using prefix sum with base = last_offset.
    offsets.resize(num_values);
    UInt64 running_offset = last_offset;
    for (UInt32 k = 0; k < num_values; ++k)
    {
        running_offset += decoded_offset_deltas[k];
        offsets[k] = running_offset;
    }

    /// Read and decode compressed row_indices (stored as raw values, not delta-encoded).
    UInt32 compressed_row_indices_count = 0;
    memcpy(&compressed_row_indices_count, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);

    const auto * compressed_row_indices_data = reinterpret_cast<const uint32_t *>(chunk_ptr);
    chunk_ptr += compressed_row_indices_count * sizeof(uint32_t);

    PaddedPODArray<uint32_t> decoded_row_indices(padded, 0);
    decoded_count = padded;
    codec_ref.decodeArray(compressed_row_indices_data, compressed_row_indices_count,
                          decoded_row_indices.data(), decoded_count);

    row_indices.resize(num_values);
    memcpy(row_indices.data(), decoded_row_indices.data(), num_values * sizeof(uint32_t));

    /// Update running state.
    last_offset = max_offset;
    last_row_index = chunk_last_row_index;
    count = num_values;
    pos = 0;
    --chunks_remaining;

    return true;
}

bool PatchBlockIndex::GroupCursor::skipNextChunk()
{
    if (chunks_remaining == 0)
        return false;

    /// Read chunk header.
    UInt64 max_offset = 0;
    UInt32 chunk_last_row_index = 0;
    UInt32 num_values = 0;

    memcpy(&max_offset, chunk_ptr, sizeof(UInt64));
    chunk_ptr += sizeof(UInt64);
    memcpy(&chunk_last_row_index, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);
    memcpy(&num_values, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);

    /// Skip compressed offsets.
    UInt32 compressed_offsets_count = 0;
    memcpy(&compressed_offsets_count, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);
    chunk_ptr += compressed_offsets_count * sizeof(uint32_t);

    /// Skip compressed row_indices.
    UInt32 compressed_row_indices_count = 0;
    memcpy(&compressed_row_indices_count, chunk_ptr, sizeof(UInt32));
    chunk_ptr += sizeof(UInt32);
    chunk_ptr += compressed_row_indices_count * sizeof(uint32_t);

    /// Update running state.
    last_offset = max_offset;
    last_row_index = chunk_last_row_index;
    count = 0;
    pos = 0;
    --chunks_remaining;

    return true;
}

bool PatchBlockIndex::GroupCursor::advanceTo(UInt64 target_offset, FastPForLib::IntegerCODEC & codec_ref)
{
    while (chunks_remaining > 0)
    {
        /// Peek at next chunk's max_offset (first field of header).
        UInt64 next_max_offset = 0;
        memcpy(&next_max_offset, chunk_ptr, sizeof(UInt64));

        if (target_offset > next_max_offset)
        {
            /// Target is beyond this chunk — skip it.
            skipNextChunk();
        }
        else
        {
            /// This chunk may contain the target — decompress it.
            return decompressNextChunk(codec_ref);
        }
    }

    return false;
}

}
