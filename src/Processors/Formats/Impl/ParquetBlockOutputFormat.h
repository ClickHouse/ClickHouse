#pragma once
#include "config.h"

#if USE_PARQUET

#include <Core/NamesAndTypes.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/Parquet/Write.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFilterInfo.h>
#include <Common/ThreadPool.h>

namespace DB
{

class ParquetBlockOutputFormat final : public IOutputFormat
{
public:
    ParquetBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, FormatFilterInfoPtr format_filter_info_);
    ~ParquetBlockOutputFormat() override;

    String getName() const override { return "ParquetBlockOutputFormat"; }

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
        Columns column_pieces;

        Parquet::ColumnChunkWriteState state;

        Task(RowGroupState * rg, size_t ci, ParquetBlockOutputFormat * p)
            : row_group(rg), column_idx(ci), mem(p) {}
    };

    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;
    void onCancel() noexcept override;

    void writeRowGroup(std::vector<Chunk> chunks);
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

    Parquet::WriteOptions options;
    /// Filled in by the ctor and read-only afterwards, so the encoder threads can share it.
    Parquet::IcebergOptionality iceberg_optionality;
    Parquet::SchemaElements schema;
    Parquet::FileWriteState file_state;
    size_t base_offset = 0; // initial out.count(), just for assert

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
    FormatFilterInfoPtr format_filter_info;

    /// Parsed from output_format_parquet_column_field_ids setting.
    std::optional<std::unordered_map<String, Int64>> column_field_ids;
};

/// Validates the user-facing Parquet `field_id` settings without writing anything, throwing
/// `BAD_ARGUMENTS` on a `column_field_ids` map the writer would reject: a value that does not
/// parse as an `Int32`, a negative id, an id in the range Iceberg reserves for metadata fields,
/// or a duplicate path or id. When `physical_columns` is non-empty the header-dependent checks
/// run too: entries referencing unknown columns, a map that does not cover the schema while
/// auto-assign is disabled, and a schema whose flattened dotted paths are ambiguous. An empty
/// `physical_columns` means the schema is not known (a table definition relying on schema
/// inference); only the header-independent checks run then.
///
/// Used at table definition time by engines that freeze the format settings from the
/// `CREATE TABLE ... SETTINGS` clause, so an invalid definition is rejected up front instead of
/// producing a table whose every `INSERT` fails.
void validateParquetColumnFieldIds(
    const NamesAndTypesList & physical_columns,
    const std::vector<std::pair<String, String>> & overrides,
    bool auto_assign,
    bool write_geometadata);

}

#endif
