#pragma once
#include <Processors/Formats/Impl/Parquet/SelectiveColumnReader.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Common/threadPoolCallbackRunner.h>

namespace parquet
{
class RowGroupPageIndexReader;
}

namespace DB
{
class ParquetReader;
struct FilterDescription;

struct SelectResult
{
    std::optional<RowSet> set;
    std::unordered_map<String, ColumnPtr> intermediate_columns;
    IColumn::Filter intermediate_filter;
    size_t valid_count = 0;
    bool skip_all = false;
};

class SelectConditions
{
public:
    SelectConditions(
        std::unordered_map<String, SelectiveColumnReaderPtr> & readers_,
        std::vector<String> & fast_filter_columns_,
        std::vector<std::shared_ptr<ExpressionFilter>>& expression_filters,
        const Block & header_);
    SelectResult selectRows(size_t rows);
private:
    bool has_filter = false;
    const std::unordered_map<String, SelectiveColumnReaderPtr> & readers;
    const std::vector<String> & fast_filter_columns;
    const std::vector<std::shared_ptr<ExpressionFilter>>& expression_filters;
    const Block & header;
};


struct ColumnChunkData
{
    uint8_t * data;
    size_t size;
};

struct ReadRangeBuffer
{
    arrow::io::ReadRange range;
    PaddedPODArray<UInt8> buffer;
};

class RowGroupPrefetch;
using RowGroupPrefetchPtr = std::shared_ptr<RowGroupPrefetch>;

class RowGroupPrefetch
{
public:
    RowGroupPrefetch(SeekableReadBuffer & file_, std::mutex & mutex, const parquet::ArrowReaderProperties& arrow_properties_, ThreadPool & io_pool);
    void prefetchRange(const arrow::io::ReadRange& range);
    void startPrefetch();
    ColumnChunkData readRange(const arrow::io::ReadRange& range);
    bool isEmpty() const { return ranges.empty(); }
    size_t totalSize() const;
    bool merge(const RowGroupPrefetchPtr other);
private:
    struct TaskEntry
    {
        arrow::io::ReadRange range;
        std::future<ColumnChunkData> task;
    };

    ThreadPoolCallbackRunnerUnsafe<ColumnChunkData> callback_runner;
    SeekableReadBuffer& file;
    std::mutex& file_mutex;
    std::vector<arrow::io::ReadRange> ranges;
    std::vector<ReadRangeBuffer> read_range_buffers;
    std::vector<TaskEntry> tasks;
    parquet::ArrowReaderProperties arrow_properties;
    bool fetched = false;
};


struct RowGroupContext
{
    ParquetReader * parquet_reader;
    parquet::RowGroupMetaData * row_group_meta;
    RowGroupPrefetchPtr prefetch_conditions;
    RowGroupPrefetchPtr prefetch;
    std::shared_ptr<parquet::RowGroupPageIndexReader> row_group_index_reader;
};

arrow::io::ReadRange getColumnRange(const parquet::ColumnChunkMetaData & column_metadata);

class RowGroupChunkReader
{
public:
    struct ReadMetrics
    {
        size_t output_rows = 0;
        size_t filtered_rows = 0;
        size_t skipped_rows = 0;
    };
    RowGroupChunkReader(
        ParquetReader * parquetReader,
        size_t row_group_idx,
        RowGroupPrefetchPtr prefetch_conditions,
        RowGroupPrefetchPtr prefetch,
        std::unordered_map<String, ColumnFilterPtr> filters);
    ~RowGroupChunkReader()
    {
    }
    Chunk readChunk(size_t rows);
    bool hasMoreRows() const { return remain_rows > 0; }
    void printMetrics(std::ostream & out) const
    {
        out << fmt::format("metrics.output_rows: {} \n metrics.filtered_rows: {} \n metrics.skipped_rows: {} \n", metrics.output_rows, metrics.filtered_rows, metrics.skipped_rows);
    }
private:
    ParquetReader * parquet_reader;
    std::unique_ptr<parquet::RowGroupMetaData> row_group_meta;
    std::vector<String> filter_columns;
    RowGroupPrefetchPtr prefetch_conditions;
    RowGroupPrefetchPtr prefetch;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
    DataTypes reader_data_types;
    std::vector<PaddedPODArray<UInt8>> column_buffers;
    size_t remain_rows = 0;
    ReadMetrics metrics;
    std::unique_ptr<SelectConditions> selectConditions;
    RowGroupContext context;
    std::unique_ptr<ColumnReaderBuilder> builder;
    std::shared_ptr<parquet::RowGroupPageIndexReader> row_group_index_reader;
};
}
