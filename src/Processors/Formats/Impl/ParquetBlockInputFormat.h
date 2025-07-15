#pragma once
#include "config.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatParserGroup.h>

#include <queue>

namespace parquet
{
class ParquetFileReader;
class FileMetaData;
}
namespace parquet::arrow { class FileReader; }
namespace arrow { class Buffer; class RecordBatchReader;}
namespace arrow::io { class RandomAccessFile; }

namespace DB
{

class ArrowColumnToCHColumn;
class ParquetRecordReader;

// Parquet files contain a metadata block with the following information:
//  * list of columns,
//  * list of "row groups",
//  * for each column in each row group:
//     - byte range for the data,
//     - min, max, count,
//     - (note that we *don't* have a reliable estimate of the decompressed+decoded size; the
//       metadata has decompressed size, but decoded size is sometimes much bigger because of
//       dictionary encoding)
//
// This information could be used for:
//  (1) Precise reads - only reading the byte ranges we need, instead of filling the whole
//      arbitrarily-sized buffer inside ReadBuffer. We know in advance exactly what ranges we'll
//      need to read.
//  (2) Skipping row groups based on WHERE conditions.
//  (3) Skipping decoding of individual pages based on PREWHERE.
//  (4) Projections. I.e. for queries that only request min/max/count, we can report the
//      min/max/count from metadata. This can be done per row group. I.e. for row groups that
//      fully pass the WHERE conditions we'll use min/max/count from metadata, for row groups that
//      only partially overlap with the WHERE conditions we'll read data.
//  (4a) Before projections are implemented, we should at least be able to do `SELECT count(*)`
//       without reading data.
//
// For (1), we need the IInputFormat to be in control of reading, with its own implementation of
// parallel reading+decoding, instead of using ParallelReadBuffer and ParallelParsingInputFormat.
// That's what RandomAccessInputCreator in FormatFactory is about.

class ParquetBlockInputFormat : public IInputFormat
{
public:
    ParquetBlockInputFormat(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & format_settings_,
        FormatParserGroupPtr parser_group_,
        size_t min_bytes_for_seek_);

    ~ParquetBlockInputFormat() override;

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return previous_approx_bytes_read_for_chunk; }

private:
    Chunk read() override;

    void onCancel() noexcept override
    {
        is_stopped = 1;
    }

    void initializeIfNeeded();
    void initializeRowGroupBatchReader(size_t row_group_batch_idx);

    void decodeOneChunk(size_t row_group_batch_idx, std::unique_lock<std::mutex> & lock);

    void scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_batch_touched = std::nullopt);
    void scheduleRowGroup(size_t row_group_batch_idx);

    void threadFunction(size_t row_group_batch_idx);

    // Data layout in the file:
    //
    // row group 0
    //   column 0
    //     page 0, page 1, ...
    //   column 1
    //     page 0, page 1, ...
    //   ...
    // row group 1
    //   column 0
    //     ...
    //   ...
    // ...
    //
    // All columns in one row group have the same number of rows.
    // (Not necessarily the same number of *values* if there are arrays or nulls.)
    // Pages have arbitrary sizes and numbers of rows, independent from each other, even if in the
    // same column or row group.
    //
    // We can think of this as having lots of data streams, one for each column x row group.
    // The main job of this class is to schedule read operations for these streams across threads.
    // Also: reassembling the results into chunks, creating/destroying these streams, prefetching.
    //
    // Some considerations:
    //  * Row group size is typically hundreds of MB (compressed). Apache recommends 0.5 - 1 GB.
    //  * Compression ratio can be pretty extreme, especially with dictionary compression.
    //    We can afford to keep a compressed row group in memory, but not uncompressed.
    //  * For each pair <row group idx, column idx>, the data lives in one contiguous range in the
    //    file. We know all these ranges in advance, from metadata.
    //  * The byte range for a column in a row group is often very short, like a few KB.
    //    So we need to:
    //     - Avoid unnecessary readahead, e.g. don't read 1 MB when we only need 1 KB.
    //     - Coalesce nearby ranges into longer reads when needed. E.g. if we need to read 5 ranges,
    //       1 KB each, with 1 KB gaps between them, it's better to do 1 x 9 KB read instead of
    //       5 x 1 KB reads.
    //     - Have lots of parallelism for reading (not necessarily for parsing). E.g. if we're
    //       reading one small column, it may translate to hundreds of tiny reads with long gaps
    //       between them. If the data comes from an HTTP server, that's hundreds of tiny HTTP GET
    //       requests. To get good performance, we have to do tens or hundreds of them in parallel.
    //       So we should probably have separate parallelism control for IO vs parsing (since we
    //       don't want hundreds of worker threads oversubscribing the CPU cores).
    //
    // (Some motivating example access patterns:
    //   - 'SELECT small_column'. Bottlenecked on number of seeks. Need to do lots of file/network
    //     reads in parallel, for lots of row groups.
    //   - 'SELECT *' when row group size is big and there are many columns. Read the whole file.
    //     Need some moderate parallelism for IO and for parsing. Ideally read+parse columns of
    //     one row group in parallel to avoid having multiple row groups in memory at once.
    //   - 'SELECT big_column'. Have to read+parse multiple row groups in parallel.
    //   - 'SELECT big_column, many small columns'. This is a mix of the previous two scenarios.
    //     We have many columns, but still need to read+parse multiple row groups in parallel.)

    // With all that in mind, here's what we do.
    //
    // We treat each row group as a sequential single-threaded stream of blocks.
    //
    // We have a sliding window of active row groups. When a row group becomes active, we start
    // reading its data (using RAM). Row group becomes inactive when we finish reading and
    // delivering all its blocks and free the RAM. Size of the window is max_decoding_threads.
    //
    // Decoded blocks are placed in `pending_chunks` queue, then picked up by read().
    // If row group decoding runs too far ahead of delivery (by `max_pending_chunks_per_row_group`
    // chunks), we pause the stream for the row group, to avoid using too much memory when decoded
    // chunks are much bigger than the compressed data.
    //
    // Also:
    //  * If preserve_order = true, we deliver chunks strictly in order of increasing row group.
    //    Decoding may still proceed in later row groups.
    //  * If max_decoding_threads <= 1, we run all tasks inline in read(), without thread pool.

    // Potential improvements:
    //  * Plan all read ranges ahead of time, for the whole file, and do prefetching for them
    //    in background. Using max_download_threads, which can be made much greater than
    //    max_decoding_threads by default.
    //  * Can parse different columns within the same row group in parallel. This would let us have
    //    fewer row groups in memory at once, reducing memory usage when selecting many columns.
    //    Should probably do more than one column per task because columns are often very small.
    //    Maybe split each row group into, say, max_decoding_threads * 2 equal-sized column bunches?
    //  * Sliding window could take into account the (predicted) memory usage of row groups.
    //    If row groups are big and many columns are selected, we may use lots of memory when
    //    reading max_decoding_threads row groups at once. Can adjust the sliding window size based
    //    on row groups' data sizes from metadata.
    //  * The max_pending_chunks_per_row_group limit could be based on actual memory usage too.
    //    Useful for preserve_order.

    class RowGroupPrefetchIterator;

    struct RowGroupBatchState
    {
        // Transitions:
        //
        // NotStarted -> Running -> Complete
        //                  Ʌ
        //                  V
        //               Paused
        //
        // If max_decoding_threads <= 1: NotStarted -> Complete.
        enum class Status : uint8_t
        {
            NotStarted,
            Running,
            // Paused decoding because too many chunks are pending.
            Paused,
            // Decoded everything.
            Done,
        };

        Status status = Status::NotStarted;

        // Window of chunks that were decoded but not returned from read():
        //
        // (delivered)            next_chunk_idx
        //   v   v                       v
        // +---+---+---+---+---+---+---+---+---+---+
        // | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |    <-- all chunks
        // +---+---+---+---+---+---+---+---+---+---+
        //           ^   ^   ^   ^   ^
        //           num_pending_chunks
        //           (in pending_chunks)
        //  (at most max_pending_chunks_per_row_group)

        size_t next_chunk_idx = 0;
        size_t num_pending_chunks = 0;

        size_t total_rows = 0;
        size_t total_bytes_compressed = 0;
        std::vector<size_t> row_group_sizes;

        size_t adaptive_chunk_size = 0;

        std::vector<int> row_groups_idxs;

        // These are only used by the decoding thread, so don't require locking the mutex.
        // If use_native_reader, only native_record_reader is used;
        // otherwise, only native_record_reader is not used.
        std::shared_ptr<ParquetRecordReader> native_record_reader;
        std::unique_ptr<parquet::arrow::FileReader> file_reader;
        std::unique_ptr<RowGroupPrefetchIterator> prefetch_iterator;
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader;
        std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    };

    // Chunk ready to be delivered by read().
    struct PendingChunk
    {
        explicit PendingChunk(size_t num_columns) : block_missing_values(num_columns) {}

        Chunk chunk;
        BlockMissingValues block_missing_values;
        size_t chunk_idx; // within row group
        size_t row_group_batch_idx;
        size_t approx_original_chunk_size;

        // For priority_queue.
        // In ordered mode we deliver strictly in order of increasing row group idx,
        // in unordered mode we prefer to interleave chunks from different row groups.
        struct Compare
        {
            bool row_group_first = false;

            bool operator()(const PendingChunk & a, const PendingChunk & b) const
            {
                auto tuplificate = [this](const PendingChunk & c)
                { return row_group_first ? std::tie(c.row_group_batch_idx, c.chunk_idx)
                                         : std::tie(c.chunk_idx, c.row_group_batch_idx); };
                return tuplificate(a) > tuplificate(b);
            }
        };
    };

    // The trigger for row group prefetching improves the overall parsing response time
    // by hiding the IO overhead of the next row group in the processing time of the previous row group.
    // +-------------------------------------------------------------------------------------------------------------------+
    // |io       +-----------+     +-----------+     +-----------+      +-----------+      +-----------+                   |
    // |         |fetch rg 0 |---->|fetch rg 1 |---->|fetch rg 2 |----->|fetch rg 3 |----->|fetch rg 4 |                   |
    // |         +-----------+     +-----------+     +-----------+      +-----------+      +-----------+                   |
    // +-------------------------------------------------------------------------------------------------------------------+
    // +-------------------------------------------------------------------------------------------------------------------+
    // |compute                    +-----------+     +-----------+     +-----------+      +-----------+      +-----------+ |
    // |                           |parse rg 0 |---->|parse rg 1 |---->|parse rg 2 |----->|parse rg 3 |----->|parse rg 4 | |
    // |                           +-----------+     +-----------+     +-----------+      +-----------+      +-----------+ |
    // +-------------------------------------------------------------------------------------------------------------------+

    class RowGroupPrefetchIterator
    {
    public:
        RowGroupPrefetchIterator(
            parquet::ParquetFileReader* file_reader_, RowGroupBatchState & row_group_batch_, const std::vector<int> & column_indices_, size_t min_bytes_for_seek_)
            : file_reader(file_reader_), row_group_batch(row_group_batch_), column_indices(column_indices_), min_bytes_for_seek(min_bytes_for_seek_)
        {
            prefetchNextRowGroups();
        }
        std::shared_ptr<arrow::RecordBatchReader> nextRowGroupReader();
    private:
        void prefetchNextRowGroups();
        size_t next_row_group_idx= 0;
        std::vector<int> prefetched_row_groups;
        parquet::ParquetFileReader * file_reader;
        RowGroupBatchState& row_group_batch;
        const std::vector<int>& column_indices;
        const size_t min_bytes_for_seek;
    };

    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_row_groups;
    FormatParserGroupPtr parser_group;
    size_t min_bytes_for_seek;
    const size_t max_pending_chunks_per_row_group_batch = 2;

    /// RandomAccessFile is thread safe, so we share it among threads.
    /// FileReader is not, so each thread creates its own.
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::shared_ptr<parquet::FileMetaData> metadata;
    /// Indices of columns to read from Parquet file.
    std::vector<int> column_indices;

    // Window of active row groups:
    //
    // row_groups_completed   row_groups_started
    //          v                   v
    //  +---+---+---+---+---+---+---+---+---+---+
    //  | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |   <-- all row groups
    //  +---+---+---+---+---+---+---+---+---+---+
    //    ^   ^                       ^   ^   ^
    //    Done                        NotStarted

    std::mutex mutex;
    // Wakes up the read() call, if any.
    std::condition_variable condvar;

    std::vector<RowGroupBatchState> row_group_batches;
    std::priority_queue<PendingChunk, std::vector<PendingChunk>, PendingChunk::Compare> pending_chunks;
    size_t row_group_batches_completed = 0;

    // These are only used when max_decoding_threads > 1.
    size_t row_group_batches_started = 0;
    bool use_thread_pool = false;
    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();
    std::shared_ptr<ThreadPool> io_pool;

    BlockMissingValues previous_block_missing_values;
    size_t previous_approx_bytes_read_for_chunk = 0;

    std::exception_ptr background_exception = nullptr;
    std::atomic<int> is_stopped{0};
    bool is_initialized = false;
};

class ParquetSchemaReader : public ISchemaReader
{
public:
    ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;
    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    const FormatSettings format_settings;
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::shared_ptr<parquet::FileMetaData> metadata;
};

}

#endif
