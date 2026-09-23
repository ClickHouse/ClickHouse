#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>

#if USE_GPU

#include <Columns/IColumn.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/Block.h>
#include <GPU/GPUAccumulator.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/ColumnSize.h>
#include <Storages/MergeTree/MergeTreeCompressedBlockReader.h>
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

using ColumnToReduce = ReadFromGPUCompressedColumns::ColumnToReduce;
using DeviceFilter = ReadFromGPUCompressedColumns::DeviceFilter;

struct SharedState
{
    std::vector<NameAndTypePair> keys;
    std::vector<ColumnToReduce> columns;
    std::optional<DeviceFilter> filter;
    DataPartsVector parts;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    ReadSettings read_settings;
    size_t batch_bytes;
    size_t num_readers = 1;
    double device_decompression_max_ratio = 1;

    std::atomic<size_t> next_part{0};
};

using SharedStatePtr = std::shared_ptr<SharedState>;

/// Whether a column of a part is compressed well enough for the device to expand it. Expanding
/// costs the device about as long per byte as the link takes to carry one, and keeps it from
/// grouping meanwhile, so a column that compression barely shrinks is cheaper sent whole.
bool expandsOnDevice(const IMergeTreeDataPart & part, const String & column_name, double max_ratio)
{
    if (max_ratio >= 1)
        return true;

    const ColumnSize size = part.getColumnSize(column_name);
    if (size.data_uncompressed == 0)
        return true;
    return static_cast<double>(size.data_compressed) <= max_ratio * static_cast<double>(size.data_uncompressed);
}

GPU::GPUCodec codecOrThrow(UInt8 method, const String & column_name, const IMergeTreeDataPart & part)
{
    const auto codec = GPU::codecOf(method);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of part {} is compressed with method {:#x}, which the device cannot expand",
            column_name,
            part.name,
            static_cast<UInt16>(method));
    return *codec;
}

constexpr size_t raw_piece_bytes = 4UL << 20;

const std::vector<NameAndTypePair> & filterColumnsOf(const SharedState & state)
{
    static const std::vector<NameAndTypePair> none;
    return state.filter ? state.filter->columns : none;
}

DataTypes typesOf(const std::vector<NameAndTypePair> & columns)
{
    DataTypes types;
    types.reserve(columns.size());
    for (const auto & column : columns)
        types.push_back(column.type);
    return types;
}

/// Two readers fill the link when they only copy compressed blocks into pinned memory; a reader
/// that also expands a column is bound by the CPU, and a quarter of the cores keep up with the
/// link, where more of them hold more buffers on both sides of it than they gain.
size_t automaticReaders(const SharedState & state)
{
    const size_t expanding_readers = std::max<size_t>(2, getNumberOfCPUCoresToUse() / 4);
    for (const auto & part : state.parts)
    {
        for (const auto & key : state.keys)
        {
            if (!expandsOnDevice(*part, key.name, state.device_decompression_max_ratio))
                return expanding_readers;
        }
        for (const auto & column : state.columns)
        {
            if (!expandsOnDevice(*part, column.column.name, state.device_decompression_max_ratio))
                return expanding_readers;
        }
        for (const auto & column : filterColumnsOf(state))
        {
            if (!expandsOnDevice(*part, column.name, state.device_decompression_max_ratio))
                return expanding_readers;
        }
    }
    return 2;
}

/// The values of a column of a wide part, expanded on the host a piece at a time.
class RawColumnReader
{
public:
    RawColumnReader(const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings)
        : in(MergeTreeCompressedBlockReader::openColumnFile(part, column, read_settings))
    {
    }

    /// Expands up to `room.size()` bytes into `room` and answers how many, none once the column is
    /// read out.
    size_t readInto(std::span<char> room) { return in.readBig(room.data(), room.size()); }

private:
    CompressedReadBufferFromFile in;
};

class GPUCompressedColumnsSource : public ISource
{
public:
    GPUCompressedColumnsSource(SharedHeader header_, SharedStatePtr state_, QueryStatusPtr query_status_)
        : ISource(std::move(header_))
        , state(std::move(state_))
        , query_status(std::move(query_status_))
        , accumulators(state->columns.size())
    {
    }

    String getName() const override { return "GPUCompressedColumns"; }

protected:
    Chunk generate() override
    {
        const size_t part_idx = state->next_part.fetch_add(1);
        if (part_idx >= state->parts.size())
            return {};

        checkNotCancelled();

        const DataPartPtr & part = state->parts[part_idx];

        MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();
        if (result_columns.size() != state->columns.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A row of {} per-part results does not fit an output header of {} columns",
                state->columns.size(),
                result_columns.size());

        for (size_t i = 0; i < state->columns.size(); ++i)
            result_columns[i]->insert(reduceColumn(*part, i));

        return Chunk(std::move(result_columns), 1);
    }

private:
    /// The accumulator of a column, and with it the staging and device buffers, outlives the part:
    /// pinning host memory is a call into the driver, and a query over many parts must not make it
    /// once per part.
    /// The accumulator of a column takes either compressed blocks of one codec or plain values.
    struct ColumnAccumulator
    {
        std::optional<GPU::GPUCodec> codec;
        GPU::GPUAccumulator accumulator;
    };

    GPU::GPUAccumulator & accumulatorFor(size_t column_idx, std::optional<GPU::GPUCodec> codec)
    {
        const ColumnToReduce & column = state->columns[column_idx];

        std::optional<ColumnAccumulator> & slot = accumulators[column_idx];
        if (!slot || slot->codec != codec)
            slot.emplace(codec, GPU::GPUAccumulator(*column.column.type, *column.result_type, column.aggregation, state->batch_bytes, codec));

        return slot->accumulator;
    }

    Field reduceColumn(const IMergeTreeDataPart & part, size_t column_idx)
    {
        const ColumnToReduce & column = state->columns[column_idx];

        GPU::GPUAccumulator * accumulator = nullptr;
        size_t bytes_read = 0;

        if (expandsOnDevice(part, column.column.name, state->device_decompression_max_ratio))
        {
            MergeTreeCompressedBlockReader reader(part, column.column, state->read_settings);

            while (const auto block = reader.next())
            {
                if (!accumulator)
                    accumulator = &accumulatorFor(column_idx, codecOrThrow(*reader.methodByte(), column.column.name, part));

                accumulator->addBlock(std::string_view(block->payload, block->compressed_bytes), block->decompressed_bytes);
                bytes_read += block->decompressed_bytes;

                checkNotCancelled();
            }
        }
        else
        {
            accumulator = &accumulatorFor(column_idx, std::nullopt);
            RawColumnReader reader(part, column.column, state->read_settings);

            while (true)
            {
                const size_t read = reader.readInto(accumulator->reserveRaw(raw_piece_bytes));
                accumulator->commitRaw(read);
                if (read == 0)
                    break;
                bytes_read += read;

                checkNotCancelled();
            }
        }

        const size_t element_size = column.column.type->getSizeOfValueInMemory();
        if (bytes_read != part.rows_count * element_size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of part {} holds {} bytes, where the part has {} rows of {} bytes",
                column.column.name,
                part.name,
                bytes_read,
                part.rows_count,
                element_size);

        if (!accumulator)
            return column.result_type->getDefault();

        return accumulator->finalize();
    }

    void checkNotCancelled() const
    {
        if (isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        if (query_status)
            query_status->checkTimeLimit();
    }

    SharedStatePtr state;
    QueryStatusPtr query_status;
    std::vector<std::optional<ColumnAccumulator>> accumulators;
};

/// One `GROUP BY` on the device over every part, fed the parts' compressed blocks by several
/// reading threads, each a part at a time.
class GPUCompressedGroupBySource : public ISource
{
public:
    GPUCompressedGroupBySource(SharedHeader header_, SharedStatePtr state_, QueryStatusPtr query_status_)
        : ISource(std::move(header_))
        , state(std::move(state_))
        , query_status(std::move(query_status_))
        , num_readers(std::max<size_t>(1, std::min(state->num_readers, state->parts.size())))
        , accumulator(
              typesOf(state->keys),
              argumentTypesOf(state->columns),
              resultTypesOf(state->columns),
              aggregationsOf(state->columns),
              state->batch_bytes,
              /*compressed=*/true,
              num_readers,
              typesOf(filterColumnsOf(*state)),
              state->filter ? std::optional(state->filter->program) : std::nullopt)
    {
    }

    String getName() const override { return "GPUCompressedGroupBy"; }

protected:
    Chunk generate() override
    {
        if (generated)
            return {};

        generated = true;

        readParts();

        const size_t num_groups = accumulator.finalize();
        if (num_groups == 0)
            return {};

        const Block & header = getPort().getHeader();

        MutableColumns key_columns;
        key_columns.reserve(state->keys.size());
        for (const auto & key : state->keys)
            key_columns.push_back(key.type->createColumn());

        MutableColumns value_columns;
        value_columns.reserve(state->columns.size());
        for (const auto & column : state->columns)
            value_columns.push_back(column.column.type->createColumn());

        accumulator.copyGroupsTo(key_columns, value_columns);

        Columns result_columns;
        result_columns.reserve(header.columns());
        for (const auto & header_column : header)
        {
            bool found = false;
            for (size_t i = 0; i < state->keys.size() && !found; ++i)
            {
                if (state->keys[i].name == header_column.name)
                {
                    result_columns.push_back(std::move(key_columns[i]));
                    found = true;
                }
            }
            for (size_t i = 0; i < state->columns.size() && !found; ++i)
            {
                if (state->columns[i].column.name == header_column.name)
                {
                    result_columns.push_back(std::move(value_columns[i]));
                    found = true;
                }
            }
            if (!found)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Column {} of the output header is neither a key nor an aggregated column", header_column.name);
        }

        return Chunk(std::move(result_columns), num_groups);
    }

private:
    static DataTypes argumentTypesOf(const std::vector<ColumnToReduce> & columns)
    {
        DataTypes types;
        types.reserve(columns.size());
        for (const auto & column : columns)
            types.push_back(column.column.type);
        return types;
    }

    static DataTypes resultTypesOf(const std::vector<ColumnToReduce> & columns)
    {
        DataTypes types;
        types.reserve(columns.size());
        for (const auto & column : columns)
            types.push_back(column.result_type);
        return types;
    }

    static std::vector<GPU::GPUAggregationKind> aggregationsOf(const std::vector<ColumnToReduce> & columns)
    {
        std::vector<GPU::GPUAggregationKind> aggregations;
        aggregations.reserve(columns.size());
        for (const auto & column : columns)
            aggregations.push_back(column.aggregation);
        return aggregations;
    }

    /// Reads a column of a part as compressed blocks for the device to expand, or as values
    /// expanded on the host.
    struct ColumnReader
    {
        ColumnReader(const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings, bool on_device)
            : element_size(column.type->getSizeOfValueInMemory())
        {
            if (on_device)
                blocks.emplace(part, column, read_settings);
            else
                raw.emplace(part, column, read_settings);
        }

        std::optional<MergeTreeCompressedBlockReader> blocks;
        std::optional<RawColumnReader> raw;
        size_t element_size;
        size_t bytes_read = 0;
        bool done = false;

        size_t rowsRead() const { return bytes_read / element_size; }
    };

    /// Reads the parts on `num_readers` threads, each taking the next part not yet taken. The
    /// first failure stops the others, and is what is thrown here.
    void readParts()
    {
        if (state->parts.empty())
            return;

        std::vector<ThreadFromGlobalPool> threads;
        threads.reserve(num_readers);

        for (size_t reader_index = 0; reader_index < num_readers; ++reader_index)
        {
            threads.emplace_back([this, reader_index, thread_group = CurrentThread::getGroup()]
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_READER);
                try
                {
                    while (!reader_failed.load(std::memory_order_acquire))
                    {
                        const size_t part_idx = state->next_part.fetch_add(1);
                        if (part_idx >= state->parts.size())
                            break;
                        readPart(reader_index, *state->parts[part_idx]);
                    }
                }
                catch (...)
                {
                    std::lock_guard lock(reader_error_mutex);
                    if (!reader_error)
                        reader_error = std::current_exception();
                    reader_failed.store(true, std::memory_order_release);
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        if (reader_error)
            std::rethrow_exception(reader_error);
    }

    /// Reads the part's columns a block at a time, always the column that is furthest behind in
    /// rows, so that the columns arrive on the device within a block of each other and what waits
    /// there for the others is at most a block per column. The columns go to the accumulator in
    /// its order: the keys, the values, then the columns of the filter.
    void readPart(size_t reader_index, const IMergeTreeDataPart & part)
    {
        const std::vector<NameAndTypePair> & filter_columns = filterColumnsOf(*state);

        std::vector<std::unique_ptr<ColumnReader>> readers;
        readers.reserve(state->keys.size() + state->columns.size() + filter_columns.size());

        const double max_ratio = state->device_decompression_max_ratio;
        for (const auto & key : state->keys)
            readers.push_back(std::make_unique<ColumnReader>(part, key, state->read_settings, expandsOnDevice(part, key.name, max_ratio)));
        for (const auto & column : state->columns)
            readers.push_back(std::make_unique<ColumnReader>(
                part, column.column, state->read_settings, expandsOnDevice(part, column.column.name, max_ratio)));
        for (const auto & column : filter_columns)
            readers.push_back(
                std::make_unique<ColumnReader>(part, column, state->read_settings, expandsOnDevice(part, column.name, max_ratio)));

        while (true)
        {
            size_t behind = readers.size();
            for (size_t i = 0; i < readers.size(); ++i)
            {
                if (!readers[i]->done && (behind == readers.size() || readers[i]->rowsRead() < readers[behind]->rowsRead()))
                    behind = i;
            }
            if (behind == readers.size())
                break;

            ColumnReader & column = *readers[behind];
            if (column.raw)
            {
                const size_t read = column.raw->readInto(accumulator.reserveRawBytes(reader_index, behind, raw_piece_bytes));
                accumulator.commitRawBytes(reader_index, behind, read);
                if (read == 0)
                {
                    column.done = true;
                    continue;
                }

                column.bytes_read += read;
            }
            else
            {
                const auto block = column.blocks->next();
                if (!block)
                {
                    column.done = true;
                    continue;
                }

                const GPU::GPUCodec codec = codecOrThrow(*column.blocks->methodByte(), std::to_string(behind), part);
                accumulator.addCompressedBlock(
                    reader_index, behind, codec, std::string_view(block->payload, block->compressed_bytes), block->decompressed_bytes);
                column.bytes_read += block->decompressed_bytes;
            }

            checkNotCancelled();
        }

        for (size_t i = 0; i < readers.size(); ++i)
        {
            if (readers[i]->bytes_read != part.rows_count * readers[i]->element_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} of part {} holds {} bytes, where the part has {} rows of {} bytes",
                    i,
                    part.name,
                    readers[i]->bytes_read,
                    part.rows_count,
                    readers[i]->element_size);
        }

        accumulator.finishPart(reader_index, part.rows_count);
    }

    void checkNotCancelled() const
    {
        if (isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        if (reader_failed.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Another reading thread of the GPU aggregation failed");
        if (query_status)
            query_status->checkTimeLimit();
    }

    SharedStatePtr state;
    QueryStatusPtr query_status;
    const size_t num_readers;
    GPU::GroupByGPUAccumulator accumulator;
    bool generated = false;

    std::mutex reader_error_mutex;
    std::exception_ptr reader_error;
    std::atomic<bool> reader_failed{false};
};

}

ReadFromGPUCompressedColumns::ReadFromGPUCompressedColumns(
    SharedHeader output_header_,
    std::vector<NameAndTypePair> keys_,
    std::vector<ColumnToReduce> columns_,
    std::optional<DeviceFilter> filter_,
    DataPartsVector parts_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t batch_bytes_,
    size_t num_streams_,
    size_t num_readers_,
    double device_decompression_max_ratio_)
    : ISourceStep(std::move(output_header_))
    , keys(std::move(keys_))
    , columns(std::move(columns_))
    , filter(std::move(filter_))
    , parts(std::move(parts_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , batch_bytes(batch_bytes_)
    , num_streams(num_streams_)
    , num_readers(num_readers_)
    , device_decompression_max_ratio(device_decompression_max_ratio_)
{
    if (filter && keys.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A keyless GPU aggregation over compressed columns with a `WHERE` for the device");
}

void ReadFromGPUCompressedColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<SharedState>();
    state->keys = keys;
    state->columns = columns;
    state->filter = filter;
    state->parts = std::move(parts);
    state->storage_snapshot = storage_snapshot;
    state->context = context;
    state->read_settings = context->getReadSettings();
    state->device_decompression_max_ratio = device_decompression_max_ratio;
    state->batch_bytes = batch_bytes;
    state->num_readers = num_readers != 0 ? num_readers : automaticReaders(*state);

    Pipes pipes;

    if (!keys.empty())
    {
        pipes.emplace_back(std::make_shared<GPUCompressedGroupBySource>(getOutputHeader(), state, settings.process_list_element));
    }
    else
    {
        const size_t streams = std::max<size_t>(1, std::min(num_streams, state->parts.size()));
        pipes.reserve(streams);
        for (size_t i = 0; i < streams; ++i)
            pipes.emplace_back(std::make_shared<GPUCompressedColumnsSource>(getOutputHeader(), state, settings.process_list_element));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromGPUCompressedColumns::describeActions(FormatSettings & format_settings) const
{
    format_settings.out << format_settings.detail_prefix << "Parts: " << parts.size() << "\n";
    if (!keys.empty())
    {
        format_settings.out << format_settings.detail_prefix << "Keys: ";
        for (size_t i = 0; i < keys.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << keys[i].name;
        format_settings.out << "\n";
    }
    format_settings.out << format_settings.detail_prefix << "Columns: ";
    for (size_t i = 0; i < columns.size(); ++i)
        format_settings.out << (i == 0 ? "" : ", ") << columns[i].column.name;
    format_settings.out << "\n";

    if (filter)
    {
        format_settings.out << format_settings.detail_prefix << "Filter on the device: " << filter->description << "\n";
        format_settings.out << format_settings.detail_prefix << "Filter columns: ";
        for (size_t i = 0; i < filter->columns.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << filter->columns[i].name;
        format_settings.out << "\n";
    }
}

void ReadFromGPUCompressedColumns::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Parts", parts.size());

    if (!keys.empty())
    {
        auto keys_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & key : keys)
            keys_array->add(key.name);
        map.add("Keys", std::move(keys_array));
    }

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column.column.name);
    map.add("Columns", std::move(columns_array));

    if (filter)
    {
        map.add("Filter on the device", filter->description);

        auto filter_columns_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & column : filter->columns)
            filter_columns_array->add(column.name);
        map.add("Filter columns", std::move(filter_columns_array));
    }
}

}

#endif
