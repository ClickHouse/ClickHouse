#pragma once
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

namespace DB
{

/// Reader for compact parts that uses a separate buffer for every column.
/// Inside a stripe of a compact part the data of a column is contiguous (see the `MergeTree` setting
/// `compact_parts_max_granules_to_buffer`), so every buffer reads its column sequentially, the columns
/// can be prefetched in parallel, and a query that reads a subset of columns does not read the data of
/// the other columns.
/// If the part is written without stripes (all columns of a granule are adjacent), all columns share one
/// buffer, because separate buffers would read the whole file for every column. All columns also share one
/// buffer when only a few granules are read (see the setting `merge_tree_compact_parts_min_granules_to_multibuffer_read`).
/// The choice is made for the mark ranges of the current read task and is made again when a reused reader
/// gets the ranges of another task.
class MergeTreeReaderCompactMultipleBuffers : public MergeTreeReaderCompact
{
public:
    template <typename... Args>
    explicit MergeTreeReaderCompactMultipleBuffers(Args &&... args)
        : MergeTreeReaderCompact{std::forward<Args>(args)...}
    {
        fillColumnPositions();
        fillReadOrder();
    }

    /// Returns the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, size_t current_range_last_mark,
                    bool continue_reading, size_t max_rows_to_read,
                    MutableColumns & res_columns, Columns & shared_columns) override;

    void prefetchBeginOfRange(Priority priority) override;

    void updateReadRequestMap(MarkRangesPtr request_map) override;

    void updateAllMarkRanges(const MarkRanges & ranges) override;

private:
    MergeTreeReaderStream & getStream(const NameAndTypePair & column) override;
    void readGranule(size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask) override;
    void init();
    /// Whether to read all columns with one buffer for the current `all_mark_ranges`.
    bool shouldUseSharedStream();
    void updateStreamsReadRequestMap();
    void fillReadOrder();
    /// Position of the first substream of a granule that the reading of `columns_to_read[column_idx]` needs,
    /// if it can be established. For a subcolumn it is the first of its own substreams, not of the whole column.
    std::optional<size_t> getFirstPositionToRead(size_t column_idx) const;

    struct Stream
    {
        std::unique_ptr<MergeTreeReaderStream> stream;
        /// Position of the first mark of a granule that the stream reads, where the prefetch starts.
        /// It is the first required substream of the columns to read, which for subcolumns can be
        /// after the first substream of the column. Empty if it cannot be established.
        std::optional<size_t> first_position;
    };

    /// Indexes in `columns_to_read` in the order of the columns in the data file.
    /// A granule is read in this order, since a backward seek out of the shared buffer is a new request
    /// that reads the data after it again (i.e. mutation reads columns in the order of `NameSet`).
    std::vector<size_t> read_order;

    bool initialized = false;
    /// Streams by the position of the column in the part. Subcolumns share the stream of their column.
    std::map<size_t, Stream> streams_by_position;
    /// The only stream when the part is written without stripes.
    std::optional<Stream> shared_stream;
    /// Stream for every column of `columns_to_read` (nullptr for columns missing in the part).
    std::vector<MergeTreeReaderStream *> streams;
    std::unordered_map<String, MergeTreeReaderStream *> streams_by_name;
};

}
