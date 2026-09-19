#pragma once
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

namespace DB
{

/// Reader for compact parts that uses a separate buffer for every column.
/// Inside a stripe of a compact part the data of a column is contiguous (see the `MergeTree` setting
/// `compact_parts_max_granules_to_buffer`), so every buffer reads its column sequentially, the columns
/// can be prefetched in parallel, and a query that reads a subset of columns does not read the data of
/// the other columns. It's suitable for compact parts with large stripes.
/// If the part is written without stripes (all columns of a granule are adjacent), all columns share one
/// buffer, because separate buffers would read the whole file for every column.
class MergeTreeReaderCompactMultipleBuffers : public MergeTreeReaderCompact
{
public:
    template <typename... Args>
    explicit MergeTreeReaderCompactMultipleBuffers(Args &&... args)
        : MergeTreeReaderCompact{std::forward<Args>(args)...}
    {
        fillColumnPositions();
    }

    /// Returns the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark,
                    bool continue_reading, size_t max_rows_to_read,
                    MutableColumns & res_columns) override;

    void prefetchBeginOfRange(Priority priority) override;

private:
    MergeTreeReaderStream & getStream(const NameAndTypePair & column) override;
    void init();

    struct Stream
    {
        std::unique_ptr<MergeTreeReaderStream> stream;
        /// Position of the first mark of the stream's column in a granule, where reading of a granule starts.
        size_t first_position = 0;
    };

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
