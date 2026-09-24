#pragma once

#include <Processors/ISource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MarkCache.h>
#include <Core/NamesAndTypes.h>
#include <Common/PODArray.h>

#include <atomic>
#include <limits>

namespace DB
{

class MergeTreeReaderStreamSingleColumnWholePart;
class IMergeTreeReader;

/// Point-reads a fixed-size `CODEC(NONE)` `Array` column using exact row offsets, fetching row `r`'s single uncompressed
/// block at `r * (25 + row_size)`. Other lazy columns in the query are read by a standard `MergeTreeReaderWide` and merged in.
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

    /// True if `column` is stored one value per compressed block, so point reads are exact.
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

    /// Bytes fetched from disk by both readers, accumulated by their profile callbacks. The chunk size is not a
    /// substitute: rows skipped inside a granule are read and dropped, and a remote reader fetches whole ranges.
    /// Atomic because a prefetching read method runs the callback on a pool thread.
    std::atomic<size_t> read_bytes = 0;

    bool initialized = false;
    size_t next_offset_index = 0;

    /// Where the previous `readRows` left `other_reader`: its mark, and the first row not yet consumed.
    static constexpr size_t no_mark = std::numeric_limits<size_t>::max();
    size_t last_read_mark = no_mark;
    UInt64 next_unread_row = 0;

    std::shared_ptr<IMergeTreeDataPartInfoForReader> part_info;
    std::unique_ptr<MergeTreeReaderStreamSingleColumnWholePart> vector_stream;
    std::unique_ptr<IMergeTreeReader> other_reader;
};

}
