#pragma once
#include "config.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace parquet { class FileMetaData; }
namespace parquet::arrow { class FileReader; }
namespace arrow { class Buffer; class RecordBatchReader;}
namespace arrow::io { class RandomAccessFile; }

namespace DB
{

class ArrowColumnToCHColumn;
class SeekableReadBufferFactory;

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
// That's what MultistreamInputCreator in FormatFactory is about.

class ParquetBlockInputFormat : public IInputFormat
{
public:
    ParquetBlockInputFormat(
        // exactly one of these two is nullptr
        ReadBuffer * buf,
        std::unique_ptr<SeekableReadBufferFactory> buf_factory,
        const Block & header,
        const FormatSettings & format_settings,
        size_t max_decoding_threads,
        size_t min_bytes_for_seek);
    ~ParquetBlockInputFormat() override;

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

    const BlockMissingValues & getMissingValues() const override;

private:
    Chunk generate() override;

    void onCancel() override
    {
        is_stopped = 1;
    }

    void initializeIfNeeded();

    void prepareRowGroupReader(size_t row_group_idx);

    // The lock must be locked when calling and when returning.
    bool decodeOneChunk(size_t row_group_idx, std::unique_lock<std::mutex> & lock);

    void scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_touched = std::nullopt);
    void scheduleRowGroup(size_t row_group_idx);

    void threadFunction(size_t row_group_idx);

    struct RowGroupState {
        bool running = false;
        bool done = false; // all chunks were decoded

        size_t chunks_decoded = 0; // added to_deliver
        size_t chunks_delivered = 0; // removed from to_deliver

        // These are only used by the decoding thread, so don't require locking the mutex.
        std::unique_ptr<parquet::arrow::FileReader> file_reader;
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader;
        std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    };

    struct ChunkToDeliver {
        Chunk chunk;
        BlockMissingValues block_missing_values;
        size_t chunk_idx; // within row group
        size_t row_group_idx;

        // For priority_queue.
        // In ordered mode we deliver strictly in order of increasing row group idx,
        // in unordered mode we prefer to interleave chunks from different row groups.
        struct Compare {
            bool row_group_first = false;

            bool operator()(const ChunkToDeliver & a, const ChunkToDeliver & b) const {
                auto tuplificate = [this](const ChunkToDeliver & c)
                { return row_group_first ? std::tie(c.row_group_idx, c.chunk_idx)
                                         : std::tie(c.chunk_idx, c.row_group_idx); };
                return tuplificate(a) > tuplificate(b);
            }
        };
    };

    std::unique_ptr<SeekableReadBufferFactory> buf_factory;
    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_row_groups;
    size_t max_decoding_threads;
    size_t min_bytes_for_seek;
    const size_t max_pending_chunks_per_row_group = 2;

    // RandomAccessFile is thread safe, so we share it among threads.
    // FileReader is not, so each thread creates its own.
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::shared_ptr<parquet::FileMetaData> metadata;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;

    std::mutex mutex;
    std::condition_variable condvar; // notified after adding to to_deliver
    std::vector<RowGroupState> row_groups;
    std::priority_queue<ChunkToDeliver, std::vector<ChunkToDeliver>, ChunkToDeliver::Compare> to_deliver;
    size_t row_groups_retired = 0;
    BlockMissingValues previous_block_missing_values;

    // These are only used when max_decoding_threads > 1.
    size_t row_groups_started = 0;
    std::optional<ThreadPool> pool;

    std::exception_ptr background_exception = nullptr;
    std::atomic<int> is_stopped{0};
    bool is_initialized = false;
};

class ParquetSchemaReader : public ISchemaReader
{
public:
    ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

}

#endif
