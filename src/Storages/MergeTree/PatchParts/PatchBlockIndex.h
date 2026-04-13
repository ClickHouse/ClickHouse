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
    /// Append rows from a block being written. base_row_index is the global
    /// row position in the output part (i.e. rows_count before this block).
    void addRows(
        const PaddedPODArray<UInt64> & block_numbers,
        const PaddedPODArray<UInt64> & block_offsets,
        UInt32 base_row_index,
        size_t num_rows);

    /// Sort, group, delta+PFOR compress, and write both files. Adds to checksums.
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
/// At construction time, loads .idx entirely and reads all compressed data
/// from .bin into memory. Decompression (PFOR decode + inverse delta)
/// happens lazily per-group via GroupCursor.
class PatchBlockIndex
{
public:
    static constexpr auto BIN_FILE = "patch_block_index.bin";
    static constexpr auto IDX_FILE = "patch_block_index.idx";

    static constexpr UInt8 FORMAT_VERSION = 1;
    static constexpr size_t CHUNK_SIZE = 1024; /// values per PFOR chunk

    PatchBlockIndex();
    ~PatchBlockIndex();

    /// Load .idx into memory and read all compressed data from .bin.
    /// After this call, the .bin file is no longer accessed.
    void load(const IDataPartStorage & storage);
    bool loaded() const { return is_loaded; }
    bool empty() const { return entries.empty(); }

    UInt64 minBlockNumber() const;
    UInt64 maxBlockNumber() const;

    /// Binary search in sorted entries. Returns nullptr if not found.
    const PatchBlockIndexEntry * findGroup(UInt64 block_number) const;

    /// Cursor for streaming through a block_number's offsets.
    /// Chunks are decompressed lazily: when the current chunk is exhausted,
    /// the next one is decoded on demand. Chunks can be skipped without
    /// decompression if the target offset exceeds the chunk's max_offset.
    struct GroupCursor
    {
        /// Currently decompressed chunk
        PaddedPODArray<UInt64> offsets;
        PaddedPODArray<UInt32> row_indices;
        UInt32 count = 0;
        UInt32 pos = 0;

        /// Pointer into in-memory compressed data for remaining chunks
        const char * chunk_ptr = nullptr;
        const char * chunk_end = nullptr;
        UInt64 last_offset = 0;
        UInt32 last_row_index = 0;
        UInt32 chunks_remaining = 0;

        /// Decompress next chunk. Returns false if no more chunks.
        bool decompressNextChunk(FastPForLib::IntegerCODEC & codec);

        /// Skip next chunk without decompressing. Reads its header
        /// and advances chunk_ptr past the compressed data.
        bool skipNextChunk();

        /// Skip chunks whose max_offset < target_offset, then decompress
        /// the first chunk that may contain target_offset.
        bool advanceTo(UInt64 target_offset, FastPForLib::IntegerCODEC & codec);

        bool valid() const { return pos < count || chunks_remaining > 0; }
    };

    /// Create a cursor for a block_number group. Decompresses the first chunk.
    std::shared_ptr<GroupCursor> createCursor(const PatchBlockIndexEntry & entry);

    FastPForLib::IntegerCODEC & getCodec() { return *codec; }

private:
    std::vector<PatchBlockIndexEntry> entries; /// sorted by block_number

    /// Raw compressed data read from .bin at load() time, one buffer per group.
    std::vector<String> compressed_groups;

    std::unique_ptr<FastPForLib::IntegerCODEC> codec;
    bool is_loaded = false;
};

}
