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
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const StorageSnapshotPtr & storage_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ThreadPool * load_marks_threadpool_,
        ValueSizeMap avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, size_t current_task_last_mark,
                    bool continue_reading, size_t max_rows_to_read, Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return false; }

    void prefetchBeginOfRange(Priority priority) override;

private:
    bool isContinuousReading(size_t mark, size_t column_position);
    void fillColumnPositions();
    void initialize();

    ReadBuffer * data_buffer;
    CompressedReadBufferBase * compressed_data_buffer;
    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;

    /// Storage columns with collected separate arrays of Nested to columns of Nested type.
    /// They maybe be needed for finding offsets of missed Nested columns in parts.
    /// They are rarely used and are heavy to initialized, so we create them
    /// only on demand and cache in this field.
    std::optional<ColumnsDescription> storage_columns_with_collected_nested;

    /// Positions of columns in part structure.
    using ColumnPositions = std::vector<std::optional<size_t>>;
    ColumnPositions column_positions;

    /// Should we read full column or only it's offsets.
    /// Element of the vector is the level of the alternative stream.
    std::vector<ColumnNameLevel> columns_for_offsets;

    /// For asynchronous reading from remote fs. Same meaning as in MergeTreeReaderStream.
    std::optional<size_t> last_right_offset;

    size_t next_mark = 0;
    std::optional<std::pair<size_t, size_t>> last_read_granule;

    void seekToMark(size_t row_index, size_t column_index);

    void readData(const NameAndTypePair & name_and_type, ColumnPtr & column, size_t from_mark,
        size_t current_task_last_mark, size_t column_position,
        size_t rows_to_read, ColumnNameLevel name_level_for_offsets);

    /// Returns maximal value of granule size in compressed file from @mark_ranges.
    /// This value is used as size of read buffer.
    static size_t getReadBufferSize(
        const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
        MergeTreeMarksLoader & marks_loader,
        const ColumnPositions & column_positions,
        const MarkRanges & mark_ranges);

    /// For asynchronous reading from remote fs.
    void adjustUpperBound(size_t last_mark);

    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;
    bool initialized = false;
};

}
