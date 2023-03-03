#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

class MergeTreeDataPartCompact;
using DataPartCompactPtr = std::shared_ptr<const MergeTreeDataPartCompact>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// Reader for compact parts
class MergeTreeReaderCompact : public IMergeTreeReader
{
public:
    MergeTreeReaderCompact(
        DataPartCompactPtr data_part_,
        NamesAndTypesList columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, size_t current_task_last_mark,
                    bool continue_reading, size_t max_rows_to_read, Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return false; }

private:
    bool isContinuousReading(size_t mark, size_t column_position);

    ReadBuffer * data_buffer;
    CompressedReadBufferBase * compressed_data_buffer;
    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;

    /// Positions of columns in part structure.
    using ColumnPositions = std::vector<ColumnPosition>;
    ColumnPositions column_positions;
    /// Should we read full column or only it's offsets
    std::vector<bool> read_only_offsets;

    /// For asynchronous reading from remote fs. Same meaning as in MergeTreeReaderStream.
    std::optional<size_t> last_right_offset;

    size_t next_mark = 0;
    std::optional<std::pair<size_t, size_t>> last_read_granule;

    void seekToMark(size_t row_index, size_t column_index);

    void readData(const NameAndTypePair & name_and_type, ColumnPtr & column, size_t from_mark,
        size_t current_task_last_mark, size_t column_position, size_t rows_to_read, bool only_offsets);

    /// Returns maximal value of granule size in compressed file from @mark_ranges.
    /// This value is used as size of read buffer.
    static size_t getReadBufferSize(
        const DataPartPtr & part,
        MergeTreeMarksLoader & marks_loader,
        const ColumnPositions & column_positions,
        const MarkRanges & mark_ranges);

    /// For asynchronous reading from remote fs.
    void adjustUpperBound(size_t last_mark);
};

}
