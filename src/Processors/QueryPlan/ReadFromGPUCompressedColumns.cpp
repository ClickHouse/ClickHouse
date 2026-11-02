#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>

#if USE_GPU

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <GPU/GPUAccumulator.h>
#include <GPU/GPUColumns.h>
#include <GPU/GPUTypeMapping.h>
#include <Storages/ColumnsDescription.h>
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
#include <Storages/MergeTree/MergeTreeCompressedColumnsReader.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>

#include <algorithm>
#include <atomic>
#include <deque>
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
using AggregateToCompute = ReadFromGPUCompressedColumns::AggregateToCompute;
using DerivedColumn = ReadFromGPUCompressedColumns::DerivedColumn;
using PartToRead = ReadFromGPUCompressedColumns::PartToRead;

struct SharedState
{
    std::vector<NameAndTypePair> keys;
    std::vector<ColumnToReduce> columns;
    std::vector<DerivedColumn> derived;
    std::vector<AggregateToCompute> aggregates;
    std::optional<Columns> result_over_no_rows;
    std::optional<DeviceFilter> filter;
    std::optional<ReadFromGPUCompressedColumns::GroupFilter> group_filter;
    std::vector<ReadFromGPUCompressedColumns::PartToRead> parts;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    MergeTreeReaderSettings reader_settings = MergeTreeReaderSettings::createFromSettings();
    size_t batch_bytes;
    size_t num_readers = 1;
    GPUDecompressionMode decompression = GPUDecompressionMode::DEVICE;
    bool collect = false;
    /// Of each column of the filter: the key or the value column of the same name and type, among the keys and then
    /// the values, which is read once for both.
    std::vector<std::optional<uint32_t>> filter_aliases;

    std::atomic<size_t> next_part{0};
};

using SharedStatePtr = std::shared_ptr<SharedState>;

/// Whether a column of a part is compressed well enough for the device to expand it. Expanding
/// costs the device about as long per byte as the link takes to carry one, and keeps it from
/// grouping meanwhile, so a column that compression barely shrinks is cheaper sent whole.
bool expandsOnDevice(GPUDecompressionMode mode)
{
    return mode == GPUDecompressionMode::DEVICE;
}

GPU::GPUCodec codecOrThrow(UInt8 method, std::string_view payload, const String & column_name, const IMergeTreeDataPart & part)
{
    const auto codec = GPU::deviceCodecOf(method, payload);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of part {} is compressed with method {:#x}, which the device cannot expand",
            column_name,
            part.name,
            static_cast<UInt16>(method));
    return *codec;
}

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

/// Whether every column the read takes is of a fixed width, so that a part can be read in ranges of its marks.
bool readsFixedWidthOnly(const SharedState & state)
{
    const auto fixed = [](const NameAndTypePair & column) { return !isString(*column.type); };
    return std::ranges::all_of(state.keys, fixed) && std::ranges::all_of(state.columns, [&](const auto & column) { return fixed(column.column); })
        && std::ranges::all_of(filterColumnsOf(state), fixed);
}

/// A piece smaller than this is not split: a reader gathers about a batch of rows before the device groups them.
constexpr size_t min_rows_to_split = 1UL << 20;

/// Splits the parts into at least `min_pieces` pieces of ranges of marks, the largest in two each time, so that as many
/// readers as there are have rows to read however few parts the table has: a table of one part after a merge was read
/// by one reader. A piece of several ranges is split between its ranges, one of one range at its middle mark.
void splitPartsAmongReaders(std::vector<PartToRead> & parts, size_t min_pieces)
{
    if (parts.size() >= min_pieces)
        return;

    struct Piece
    {
        DataPartPtr part;
        MarkRanges ranges;
        size_t rows = 0;
    };

    std::vector<Piece> pieces;
    pieces.reserve(min_pieces);
    for (auto & part_to_read : parts)
    {
        const auto & granularity = *part_to_read.part->index_granularity;
        MarkRanges ranges = part_to_read.ranges.empty() ? MarkRanges{MarkRange(0, granularity.getMarksCountWithoutFinal())} : part_to_read.ranges;
        const size_t rows = granularity.getRowsCountInRanges(ranges);
        pieces.push_back({.part = part_to_read.part, .ranges = std::move(ranges), .rows = rows});
    }

    while (pieces.size() < min_pieces)
    {
        auto largest = std::ranges::max_element(pieces, {}, &Piece::rows);
        if (largest->rows < 2 * min_rows_to_split)
            break;

        const auto & granularity = *largest->part->index_granularity;
        Piece second{.part = largest->part, .ranges = {}, .rows = 0};
        if (largest->ranges.size() > 1)
        {
            /// From the range at which the rows so far pass half.
            size_t rows_before = 0;
            size_t split_at = 0;
            while (split_at + 1 < largest->ranges.size() && rows_before + granularity.getRowsCountInRange(largest->ranges[split_at]) <= largest->rows / 2)
                rows_before += granularity.getRowsCountInRange(largest->ranges[split_at++]);
            if (split_at == 0)
                split_at = 1;
            second.ranges.assign(largest->ranges.begin() + split_at, largest->ranges.end());
            largest->ranges.resize(split_at);
        }
        else
        {
            MarkRange & range = largest->ranges.front();
            if (range.end - range.begin < 2)
                break;
            const size_t middle = (range.begin + range.end) / 2;
            second.ranges.push_back(MarkRange(middle, range.end));
            range.end = middle;
        }
        second.rows = granularity.getRowsCountInRanges(second.ranges);
        largest->rows -= second.rows;
        pieces.push_back(std::move(second));
    }

    parts.clear();
    parts.reserve(pieces.size());
    for (auto & piece : pieces)
        parts.push_back({.part = std::move(piece.part), .ranges = std::move(piece.ranges)});
}

/// Two readers fill the link when they only copy compressed blocks into pinned memory; a reader
/// that also expands the columns is bound by the CPU, and a quarter of the cores keep up with the
/// link, where more of them hold more buffers on both sides of it than they gain.
size_t automaticReaders(const SharedState & state)
{
    if (expandsOnDevice(state.decompression))
        return 2;
    return std::max<size_t>(2, getNumberOfCPUCoresToUse() / 4);
}

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
        , output_queue(state->collect ? std::make_unique<ConcurrentBoundedQueue<Chunk>>(collect_queue_chunks) : nullptr)
        , accumulator(
              typesOf(state->keys),
              argumentTypesOf(state->columns),
              groupByAggregatesOf(state->columns, state->aggregates),
              state->batch_bytes,
              /*compressed=*/true,
              num_readers,
              typesOf(filterColumnsOf(*state)),
              state->filter ? std::optional(state->filter->program) : std::nullopt,
              derivedValuesOf(state->derived),
              state->collect ? std::function<void(MutableColumns)>([this](MutableColumns columns) { pushCollected(std::move(columns)); })
                             : std::function<void(MutableColumns)>{},
              state->filter_aliases)
    {
    }

    ~GPUCompressedGroupBySource() override
    {
        if (output_queue)
            output_queue->clearAndFinish();
        if (reading_thread.joinable())
            reading_thread.join();
    }

    String getName() const override { return "GPUCompressedGroupBy"; }

protected:
    void onCancel() noexcept override
    {
        if (output_queue)
            output_queue->clearAndFinish();
    }

    Chunk generate() override
    {
        if (state->collect)
            return generateCollected();

        if (generated)
            return {};

        generated = true;

        readParts();

        size_t num_groups = accumulator.finalize();

        /// No row came to the device, and an aggregation without keys emits its results over none, which the `HAVING`
        /// above evaluates.
        if (num_groups == 0 && state->result_over_no_rows)
            return Chunk(*state->result_over_no_rows, 1);

        if (num_groups != 0 && state->group_filter)
            num_groups = accumulator.filterGroups(state->group_filter->program, state->group_filter->columns);
        if (num_groups == 0)
            return {};

        /// The header is the aggregation's: the keys, then the result of each aggregate.
        const Block & header = getPort().getHeader();
        if (header.columns() != state->keys.size() + state->aggregates.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A GPU aggregation of {} keys and {} aggregates emits {} columns",
                state->keys.size(),
                state->aggregates.size(),
                header.columns());

        MutableColumns key_columns;
        key_columns.reserve(state->keys.size());
        for (size_t i = 0; i < state->keys.size(); ++i)
            key_columns.push_back(header.getByPosition(i).type->createColumn());

        MutableColumns aggregate_columns;
        aggregate_columns.reserve(state->aggregates.size());
        for (size_t i = 0; i < state->aggregates.size(); ++i)
            aggregate_columns.push_back(header.getByPosition(state->keys.size() + i).type->createColumn());

        accumulator.copyGroupsTo(key_columns, aggregate_columns);

        Columns result_columns;
        result_columns.reserve(header.columns());
        for (auto & column : key_columns)
            result_columns.push_back(std::move(column));
        for (auto & column : aggregate_columns)
            result_columns.push_back(std::move(column));

        return Chunk(std::move(result_columns), num_groups);
    }

private:
    /// How many chunks of collected rows wait for the steps above before the grouping thread waits for them.
    static constexpr size_t collect_queue_chunks = 4;

    /// The rows the device collected, as the chunks the grouping thread hands over while the parts are read on a thread
    /// of this source's own, and the rest once the read ends.
    Chunk generateCollected()
    {
        if (!reading_started)
        {
            reading_started = true;
            reading_thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup()]
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_READER);
                try
                {
                    readParts();
                    if (accumulator.finalize() != 0)
                    {
                        MutableColumns no_keys;
                        MutableColumns columns;
                        columns.reserve(state->columns.size());
                        for (const auto & column : state->columns)
                            columns.push_back(column.column.type->createColumn());
                        accumulator.copyGroupsTo(no_keys, columns);
                        pushCollected(std::move(columns));
                    }
                }
                catch (...)
                {
                    reading_error = std::current_exception();
                }
                output_queue->finish();
            });
        }

        Chunk chunk;
        if (output_queue->pop(chunk))
            return chunk;

        if (reading_thread.joinable())
            reading_thread.join();
        if (reading_error)
            std::rethrow_exception(reading_error);
        return {};
    }

    /// A column of each of `state->columns`, into the read's header, to the steps above. Waits for room.
    void pushCollected(MutableColumns columns)
    {
        const Block & header = getPort().getHeader();
        if (header.columns() != state->columns.size() || columns.size() != state->columns.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A GPU read of {} columns emits {} columns, {} of them collected",
                state->columns.size(),
                header.columns(),
                columns.size());

        const size_t num_rows = columns.front()->size();
        Columns result_columns(header.columns());
        for (size_t i = 0; i < state->columns.size(); ++i)
            result_columns[header.getPositionByName(state->columns[i].column.name)] = std::move(columns[i]);

        if (!output_queue->push(Chunk(std::move(result_columns), num_rows)))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "The GPU read's rows are no longer taken");
    }

    static DataTypes argumentTypesOf(const std::vector<ColumnToReduce> & columns)
    {
        DataTypes types;
        types.reserve(columns.size());
        for (const auto & column : columns)
            types.push_back(column.column.type);
        return types;
    }

    static std::vector<GPU::GPUGroupByAggregate>
    groupByAggregatesOf(const std::vector<ColumnToReduce> & columns, const std::vector<AggregateToCompute> & aggregates)
    {
        std::vector<GPU::GPUGroupByAggregate> result;
        result.reserve(aggregates.size());
        static const DataTypePtr derived_type = std::make_shared<DataTypeFloat64>();
        for (const auto & aggregate : aggregates)
        {
            const IDataType * argument_type = nullptr;
            uint32_t column = GPU::GPUGroupByAggregate::no_column;
            if (aggregate.column)
            {
                argument_type = columns.at(*aggregate.column).column.type.get();
                column = static_cast<uint32_t>(*aggregate.column);
            }
            else if (aggregate.derived)
            {
                argument_type = derived_type.get();
                column = static_cast<uint32_t>(columns.size() + *aggregate.derived);
            }
            result.push_back({
                .aggregation = aggregate.aggregation,
                .column = column,
                .result_type = GPU::groupByResultTypeOrThrow(argument_type, *aggregate.result_type, aggregate.aggregation),
            });
        }
        return result;
    }

    static std::vector<GPU::GPUDerivedValue> derivedValuesOf(const std::vector<DerivedColumn> & derived)
    {
        std::vector<GPU::GPUDerivedValue> result;
        result.reserve(derived.size());
        for (const auto & column : derived)
        {
            if (column.columns.size() > GPU::max_filter_columns)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A value the GPU derives from {} columns", column.columns.size());
            GPU::GPUDerivedValue value;
            value.program = column.program;
            for (size_t i = 0; i < column.columns.size(); ++i)
                value.columns[i] = static_cast<uint32_t>(column.columns[i]);
            value.num_columns = static_cast<uint32_t>(column.columns.size());
            result.push_back(value);
        }
        return result;
    }

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
                        readPart(reader_index, state->parts[part_idx]);
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

    /// Hands what a reader reads of the columns of a part to the accumulator, as its columns.
    class AccumulatorFeed final : public MergeTreeCompressedColumnsReader::Consumer
    {
    public:
        using Stream = MergeTreeCompressedColumnsReader::Stream;

        AccumulatorFeed(GPUCompressedGroupBySource & source_, size_t reader_index_, const IMergeTreeDataPart & part_)
            : source(source_)
            , reader_index(reader_index_)
            , part(part_)
        {
        }

        /// Of each column read: its name, and the accumulator's column of its values and of its sizes.
        std::vector<String> names;
        std::vector<size_t> column_indices;
        std::vector<size_t> offsets_column_indices;

        void addCompressedBlock(
            size_t column,
            Stream stream,
            const CompressedReadBufferBase::CompressedBlock & block,
            size_t skipped,
            size_t kept,
            size_t remaining_bytes) override
        {
            codecOrThrow(block.method, block.payload, names[column] + (stream == Stream::Sizes ? ".size" : ""), part);
            source.accumulator.addCompressedBlock(
                reader_index, indexOf(column, stream), block.method, block.payload, block.decompressed_bytes, remaining_bytes, skipped, kept);
        }

        std::span<char> reserveBytes(size_t column, Stream stream, size_t max_bytes, size_t remaining_bytes) override
        {
            return source.accumulator.reserveRawBytes(reader_index, indexOf(column, stream), max_bytes, remaining_bytes);
        }

        void commitBytes(size_t column, Stream stream, size_t bytes) override
        {
            source.accumulator.commitRawBytes(reader_index, indexOf(column, stream), bytes);
        }

        void checkCancelled() override { source.checkNotCancelled(); }

    private:
        size_t indexOf(size_t column, Stream stream) const
        {
            return stream == Stream::Sizes ? offsets_column_indices[column] : column_indices[column];
        }

        GPUCompressedGroupBySource & source;
        const size_t reader_index;
        const IMergeTreeDataPart & part;
    };

    /// Reads the part's columns, in the accumulator's order: the keys, the values, then the columns of the filter. The
    /// rows of all the ranges of a part go to the device as the rows of one part, so that it groups them as they come,
    /// however many ranges there are, rather than drain its work at the end of each.
    void readPart(size_t reader_index, const ReadFromGPUCompressedColumns::PartToRead & part_to_read)
    {
        const IMergeTreeDataPart & part = *part_to_read.part;
        const std::vector<NameAndTypePair> & filter_columns = filterColumnsOf(*state);

        AccumulatorFeed feed(*this, reader_index, part);
        std::vector<NameAndTypePair> columns;
        columns.reserve(state->keys.size() + state->columns.size() + filter_columns.size());

        size_t column_index = 0;
        const auto add_column = [&](const NameAndTypePair & column)
        {
            columns.push_back(column);
            feed.names.push_back(column.name);
            feed.column_indices.push_back(column_index);
            feed.offsets_column_indices.push_back(isString(*column.type) ? accumulator.offsetsColumnOf(column_index) : 0);
            ++column_index;
        };
        for (const auto & key : state->keys)
            add_column(key);
        for (const auto & column : state->columns)
            add_column(column.column);
        /// A column of the filter that is a key or a value as well is read as that column.
        for (size_t i = 0; i < filter_columns.size(); ++i)
        {
            if (state->filter_aliases[i])
                ++column_index;
            else
                add_column(filter_columns[i]);
        }

        MergeTreeCompressedColumnsReader reader(
            part, columns, state->reader_settings, expandsOnDevice(state->decompression), part_to_read.ranges);
        accumulator.finishPart(reader_index, reader.read(feed));
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
    /// Of a collecting read: the chunks handed over, which `generate` takes. Before `accumulator`, which pushes into it.
    std::unique_ptr<ConcurrentBoundedQueue<Chunk>> output_queue;
    GPU::GroupByGPUAccumulator accumulator;
    /// Only under `gpu_aggregation_decompression = 'auto'`.
    bool generated = false;

    std::mutex reader_error_mutex;
    std::exception_ptr reader_error;
    std::atomic<bool> reader_failed{false};

    /// Of a collecting read.
    bool reading_started = false;
    ThreadFromGlobalPool reading_thread;
    std::exception_ptr reading_error;
};

}

ReadFromGPUCompressedColumns::ReadFromGPUCompressedColumns(
    SharedHeader output_header_,
    std::vector<NameAndTypePair> keys_,
    std::vector<ColumnToReduce> columns_,
    std::vector<DerivedColumn> derived_,
    std::vector<AggregateToCompute> aggregates_,
    std::optional<Columns> result_over_no_rows_,
    std::optional<DeviceFilter> filter_,
    std::vector<PartToRead> parts_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t batch_bytes_,
    size_t num_readers_,
    GPUDecompressionMode decompression_,
    bool collect_)
    : ISourceStep(std::move(output_header_))
    , keys(std::move(keys_))
    , columns(std::move(columns_))
    , derived(std::move(derived_))
    , aggregates(std::move(aggregates_))
    , result_over_no_rows(std::move(result_over_no_rows_))
    , filter(std::move(filter_))
    , parts(std::move(parts_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , batch_bytes(batch_bytes_)
    , num_readers(num_readers_)
    , decompression(decompression_)
    , collect(collect_)
{
    if (collect && (!keys.empty() || !aggregates.empty() || !derived.empty() || columns.empty()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read that collects rows of {} columns with keys or aggregates", columns.size());

    if (!collect && aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over compressed columns of {} keys and no aggregates", keys.size());

    if (result_over_no_rows && (!keys.empty() || result_over_no_rows->size() != aggregates.size()))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation over compressed columns of {} keys and {} aggregates with {} results over no rows",
            keys.size(),
            aggregates.size(),
            result_over_no_rows->size());
}

void ReadFromGPUCompressedColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<SharedState>();
    state->keys = keys;
    state->columns = columns;
    state->derived = derived;
    state->aggregates = aggregates;
    state->result_over_no_rows = result_over_no_rows;
    state->filter = filter;
    state->group_filter = group_filter;
    state->parts = std::move(parts);
    state->storage_snapshot = storage_snapshot;
    state->context = context;
    state->reader_settings = MergeTreeReaderSettings::createFromContext(context);
    state->decompression = decompression;
    state->batch_bytes = batch_bytes;
    state->num_readers = num_readers != 0 ? num_readers : automaticReaders(*state);
    state->collect = collect;
    if (filter)
    {
        for (const NameAndTypePair & column : filter->columns)
        {
            std::optional<uint32_t> alias;
            for (size_t i = 0; i < keys.size() && !alias; ++i)
                if (keys[i].name == column.name && keys[i].type->equals(*column.type))
                    alias = static_cast<uint32_t>(i);
            for (size_t i = 0; i < columns.size() && !alias; ++i)
                if (columns[i].column.name == column.name && columns[i].column.type->equals(*column.type))
                    alias = static_cast<uint32_t>(keys.size() + i);
            state->filter_aliases.push_back(alias);
        }
    }

    /// A read of columns of a fixed width reads a large part in pieces, a reader each.
    if (readsFixedWidthOnly(*state))
        splitPartsAmongReaders(state->parts, 2 * state->num_readers);

    Pipe pipe(std::make_shared<GPUCompressedGroupBySource>(getOutputHeader(), state, settings.process_list_element));
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromGPUCompressedColumns::describeActions(FormatSettings & format_settings) const
{
    format_settings.out << format_settings.detail_prefix << "Parts: " << parts.size() << "\n";
    if (collect)
        format_settings.out << format_settings.detail_prefix << "Rows the filter keeps sent back\n";
    if (const size_t ranged = std::ranges::count_if(parts, [](const auto & part) { return !part.ranges.empty(); }))
        format_settings.out << format_settings.detail_prefix << "Parts read in ranges of marks: " << ranged << "\n";
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
    if (!derived.empty())
    {
        format_settings.out << format_settings.detail_prefix << "Computed on the device: ";
        for (size_t i = 0; i < derived.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << derived[i].description;
        format_settings.out << "\n";
    }
    if (!aggregates.empty())
    {
        format_settings.out << format_settings.detail_prefix << "Aggregates: ";
        for (size_t i = 0; i < aggregates.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << aggregates[i].description;
        format_settings.out << "\n";
    }
    if (group_filter)
        format_settings.out << format_settings.detail_prefix << "Filter of the groups on the device: " << group_filter->description << "\n";

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
    map.add("Collected", collect);
    map.add("Parts Read In Ranges", std::ranges::count_if(parts, [](const auto & part) { return !part.ranges.empty(); }));

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

    if (!derived.empty())
    {
        auto derived_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & column : derived)
            derived_array->add(column.description);
        map.add("Computed on the device", std::move(derived_array));
    }

    if (!aggregates.empty())
    {
        auto aggregates_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & aggregate : aggregates)
            aggregates_array->add(aggregate.description);
        map.add("Aggregates", std::move(aggregates_array));
    }

    if (group_filter)
        map.add("Filter of the groups on the device", group_filter->description);

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
