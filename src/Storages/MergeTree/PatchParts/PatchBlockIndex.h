#pragma once

#include <vector>
#include <memory>

#include <Common/PODArray.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

namespace FastPForLib { class IntegerCODEC; }

namespace DB
{

class IDataPartStorage;

/// Accumulates (_block_number, _block_offset, row_index) during part writing,
/// sorts by (_block_number, _block_offset), and writes the two index files
/// using delta + PFOR compression.
class PatchBlockIndexWriter
{
public:
    void addRows(
        const PaddedPODArray<UInt64> & block_numbers,
        const PaddedPODArray<UInt64> & block_offsets,
        UInt32 base_row_index,
        size_t num_rows);

    void finalize(IDataPartStorage & storage, MergeTreeDataPartChecksums & checksums);

    bool empty() const { return rows.empty(); }

private:
    struct Row
    {
        UInt64 block_number;
        UInt64 block_offset;
        UInt32 row_index;
    };

    std::vector<Row> rows;
};

/// Index entry loaded from .idx (one per unique _block_number).
struct PatchBlockIndexEntry
{
    UInt64 block_number;
    UInt32 num_offsets;
    UInt64 min_offset;
    UInt64 max_offset;
    UInt64 byte_offset; /// position in .bin
};

/// Reads the index files and provides cursor-based lookup by _block_number.
/// Replaces PatchHashMap for Join mode when the on-disk index is present.
///
/// At load time, reads .idx into memory and slurps the entire .bin into a
/// single contiguous buffer. Decompression happens lazily per-group via
/// GroupCursor, which holds raw pointers into the buffer (zero-copy).
class PatchBlockIndex
{
public:
    static constexpr auto BIN_FILE = "patch_block_index.bin";
    static constexpr auto IDX_FILE = "patch_block_index.idx";

    static constexpr UInt8 FORMAT_VERSION = 1;
    static constexpr size_t CHUNK_SIZE = 1024; /// values per PFOR chunk

    PatchBlockIndex();
    ~PatchBlockIndex();

    void load(const IDataPartStorage & storage);
    bool loaded() const { return is_loaded; }
    bool empty() const { return entries.empty(); }

    UInt64 minBlockNumber() const;
    UInt64 maxBlockNumber() const;

    /// Binary search in sorted entries. Returns nullptr if not found.
    const PatchBlockIndexEntry * findGroup(UInt64 block_number) const;

    /// Cursor for streaming through a block_number's offsets.
    /// Points into the contiguous .bin buffer — no copies.
    struct GroupCursor
    {
        /// Currently decompressed chunk
        PaddedPODArray<UInt64> offsets;
        PaddedPODArray<UInt32> row_indices;
        UInt32 count = 0;
        UInt32 pos = 0;

        /// Raw pointer into the contiguous .bin buffer for remaining chunks
        const char * chunk_ptr = nullptr;
        const char * chunk_end = nullptr;
        UInt64 last_offset = 0;
        UInt32 last_row_index = 0;
        UInt32 chunks_remaining = 0;

        bool decompressNextChunk(FastPForLib::IntegerCODEC & codec);
        bool skipNextChunk();
        bool advanceTo(UInt64 target_offset, FastPForLib::IntegerCODEC & codec);

        bool valid() const { return pos < count || chunks_remaining > 0; }
    };

    /// Create a cursor positioned at the start of a group.
    /// The cursor is a value type — no heap allocation.
    GroupCursor createCursor(const PatchBlockIndexEntry & entry);

    FastPForLib::IntegerCODEC & getCodec() { return *codec; }

private:
    std::vector<PatchBlockIndexEntry> entries; /// sorted by block_number

    /// Entire .bin file contents in a single contiguous buffer.
    String bin_data;

    std::unique_ptr<FastPForLib::IntegerCODEC> codec;
    bool is_loaded = false;
};

}
