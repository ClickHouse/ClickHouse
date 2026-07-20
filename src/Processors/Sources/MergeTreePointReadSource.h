#pragma once

#include <Processors/ISource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MarkCache.h>
#include <Core/NamesAndTypes.h>
#include <Common/PODArray.h>

namespace DB
{

class CompressedReadBufferFromFile;
class IMergeTreeReader;

/// Point-reads a fixed-size, uncompressed (`CODEC(NONE)`) `Array` column for a set of exact row offsets, fetching each
/// row's single compressed block instead of decompressing whole granules. Used by lazy materialization for the
/// two-phase quantized-codes vector search rescore.
///
/// The lazy read may also require other (non point-readable) columns alongside the vector column; those are read with a
/// standard `MergeTreeReaderWide` for the same row offsets and merged into the output chunk, so the source produces the
/// full `lazy_header` in a single pass. Only the fixed-size vector column is point-read; the others pay the usual
/// granule read (unavoidable for variable-width data), but the heavy vector column - the point of the optimization -
/// avoids it.
///
/// Precondition for the vector column (established by the per-column `max_compress_block_size` alignment and re-checked
/// by `isEligible`): its element stream is one vector per compressed block, so block/row `r` is at
/// `r * (framing + row_size)`, `framing = checksum(16) + header(9) = 25`. Checksums are kept.
class MergeTreePointReadSource final : public ISource
{
public:
    MergeTreePointReadSource(
        SharedHeader header_,
        RangesInDataPart part_,
        PaddedPODArray<UInt64> row_offsets_,
        NameAndTypePair vector_column_,
        size_t dimensions_,
        NamesAndTypesList other_columns_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeReaderSettings reader_settings_,
        MarkCachePtr mark_cache_,
        size_t max_block_size_);

    ~MergeTreePointReadSource() override;

    String getName() const override { return "MergeTreePointReadSource"; }

    /// Returns true if `part`'s `column` is stored with a one-row-per-block layout so point reads are exact.
    /// Definitive and cheap: the element `.bin` size equals `rows_count * (framing + row_size)` iff every block
    /// holds exactly one vector.
    static bool isEligible(const RangesInDataPart & part, const NameAndTypePair & column, size_t dimensions);

protected:
    Chunk generate() override;

private:
    void initialize();
    /// Point-read `vector_column` for the current batch of offsets into `dst_column`.
    void readVectorColumn(size_t base, size_t batch, IColumn & dst_column);
    /// Read `other_columns` for the current batch of offsets (via the standard reader) into `dst_columns`.
    void readOtherColumns(size_t base, size_t batch, Columns & dst_columns);

    SharedHeader header;
    RangesInDataPart part;
    PaddedPODArray<UInt64> row_offsets;
    NameAndTypePair vector_column;
    size_t dimensions;
    NamesAndTypesList other_columns;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeReaderSettings reader_settings;
    MarkCachePtr mark_cache;
    size_t max_block_size;

    size_t element_size = 0; /// bytes per vector element
    size_t row_size = 0;     /// dimensions * element_size
    size_t block_stride = 0; /// framing + row_size

    bool initialized = false;
    size_t next_offset_index = 0;

    std::shared_ptr<IMergeTreeDataPartInfoForReader> part_info;
    std::unique_ptr<CompressedReadBufferFromFile> vector_buffer;
    std::unique_ptr<IMergeTreeReader> other_reader;
};

}
