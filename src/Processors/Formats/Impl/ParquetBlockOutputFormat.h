#pragma once
#include "config.h"

#if USE_PARQUET

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/Parquet/Write.h>
#include <Formats/FormatSettings.h>
#include <Common/ThreadPool.h>

namespace arrow
{
class Array;
class DataType;
}

namespace parquet
{
namespace arrow
{
    class FileWriter;
}
}

namespace DB
{

class CHColumnToArrowColumn;

class ParquetBlockOutputFormat : public IOutputFormat
{
public:
    ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);
    ~ParquetBlockOutputFormat() override;

    String getName() const override { return "ParquetBlockOutputFormat"; }

    String getContentType() const override { return "application/octet-stream"; }

private:
    struct MemoryToken
    {
        ParquetBlockOutputFormat * parent;
        size_t bytes = 0;

        explicit MemoryToken(ParquetBlockOutputFormat * p, size_t b = 0) : parent(p)
        {
            set(b);
        }

        MemoryToken(MemoryToken && t) /// NOLINT
          : parent(std::exchange(t.parent, nullptr)), bytes(std::exchange(t.bytes, 0)) {}

        MemoryToken & operator=(MemoryToken && t) /// NOLINT
        {
            parent = std::exchange(t.parent, nullptr);
            bytes = std::exchange(t.bytes, 0);
            return *this;
        }

        ~MemoryToken()
        {
            set(0);
        }

        void set(size_t new_size)
        {
            if (new_size == bytes)
                return;
            parent->bytes_in_flight += new_size - bytes; // overflow is fine
            bytes = new_size;
        }
    };

    struct ColumnChunk
    {
        Parquet::ColumnChunkWriteState state;
        PODArray<char> serialized;

        MemoryToken mem;

        explicit ColumnChunk(ParquetBlockOutputFormat * p) : mem(p) {}
    };

    struct RowGroupState
    {
        size_t tasks_in_flight = 0;
        std::vector<std::vector<ColumnChunk>> column_chunks;
        size_t num_rows = 0;
    };

    struct Task
    {
        RowGroupState * row_group;
        size_t column_idx;
        size_t subcolumn_idx = 0;

        MemoryToken mem;

        /// If not null, we need to call prepareColumnForWrite().
        /// Otherwise we need to call writeColumnChunkBody().
        DataTypePtr column_type;
        std::string column_name;
        std::vector<ColumnPtr> column_pieces;

        Parquet::ColumnChunkWriteState state;

        Task(RowGroupState * rg, size_t ci, ParquetBlockOutputFormat * p)
            : row_group(rg), column_idx(ci), mem(p) {}
    };

    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;
    void onCancel() noexcept override;

    void writeRowGroup(std::vector<Chunk> chunks);
    void writeUsingArrow(std::vector<Chunk> chunks);
    void writeRowGroupInOneThread(Chunk chunk);
    void writeRowGroupInParallel(std::vector<Chunk> chunks);

    void threadFunction();
    void startMoreThreadsIfNeeded(const std::unique_lock<std::mutex> & lock);

    /// Called in single-threaded fashion. Writes to the file.
    void reapCompletedRowGroups(std::unique_lock<std::mutex> & lock);

    const FormatSettings format_settings;

    /// Chunks to squash together to form a row group.
    std::vector<Chunk> staging_chunks;
    size_t staging_rows = 0;
    size_t staging_bytes = 0;

    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;

    Parquet::WriteOptions options;
    Parquet::SchemaElements schema;
    std::vector<parquet::format::RowGroup> row_groups_complete;
    std::vector<std::vector<parquet::format::ColumnIndex>> column_indexes;
    std::vector<std::vector<parquet::format::OffsetIndex>> offset_indexes;
    size_t base_offset = 0;


    std::mutex mutex;
    std::condition_variable condvar; // wakes up consume()
    std::unique_ptr<ThreadPool> pool;

    std::atomic_bool is_stopped{false};
    std::exception_ptr background_exception = nullptr;

    /// Invariant: if there's at least one task then there's at least one thread.
    size_t threads_running = 0;
    std::atomic<size_t> bytes_in_flight{0};

    std::deque<Task> task_queue;
    std::deque<RowGroupState> row_groups;
};

}

#endif
