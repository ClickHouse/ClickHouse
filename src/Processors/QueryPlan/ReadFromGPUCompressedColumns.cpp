#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>

#if USE_GPU

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Compression/CompressedReadBufferFromFile.h>
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
#include <Storages/MergeTree/MergeTreeCompressedBlockReader.h>
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
    std::optional<DeviceFilter> filter;
    std::optional<ReadFromGPUCompressedColumns::GroupFilter> group_filter;
    std::vector<ReadFromGPUCompressedColumns::PartToRead> parts;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    ReadSettings read_settings;
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

ISerialization::SubstreamPath stringSizesPath()
{
    ISerialization::SubstreamPath path;
    path.push_back(ISerialization::Substream::StringSizes);
    return path;
}

/// The uncompressed bytes of a stream of a column of the part.
size_t streamBytesOf(const IMergeTreeDataPart & part, const NameAndTypePair & column, const ISerialization::SubstreamPath & substream_path)
{
    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, substream_path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has no file for a stream of column {}", part.name, column.name);
    return part.checksums.files.at(*stream_name + ".bin").uncompressed_size;
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

        /// Without keys, every row of each part is read.
        const DataPartPtr & part = state->parts[part_idx].part;
        chassert(state->parts[part_idx].ranges.empty());

        MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();
        chassert(result_columns.size() == state->columns.size());

        for (size_t i = 0; i < state->columns.size(); ++i)
            result_columns[i]->insert(reduceColumn(*part, i));

        return Chunk(std::move(result_columns), 1);
    }

private:
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
        if (GPU::columnTypeOrThrow(*column.column.type) == GPU::GPUElementType::String)
            return reduceVariableColumn(part, column_idx);

        GPU::GPUAccumulator * accumulator = nullptr;
        size_t bytes_read = 0;

        if (expandsOnDevice(state->decompression))
        {
            MergeTreeCompressedBlockReader reader(part, column.column, state->read_settings);

            while (const auto block = reader.next())
            {
                const std::string_view payload(block->payload, block->compressed_bytes);
                if (!accumulator)
                    accumulator = &accumulatorFor(column_idx, codecOrThrow(*reader.methodByte(), payload, column.column.name, part));

                accumulator->addBlock(*reader.methodByte(), payload, block->decompressed_bytes);
                bytes_read += block->decompressed_bytes;

                checkNotCancelled();
            }
        }
        else
        {
            accumulator = &accumulatorFor(column_idx, std::nullopt);
            CompressedReadBufferFromFile reader(MergeTreeCompressedBlockReader::openColumnFile(part, column.column, state->read_settings));

            while (true)
            {
                const std::span<char> room = accumulator->reserveRaw(raw_piece_bytes);
                const size_t read = reader.readBig(room.data(), room.size());
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

    /// A column of values of varying width - a `String` - is two streams: its bytes, and the sizes of its rows in the
    /// `.size` stream beside them. Expanded on the device, both go there as compressed blocks, the sizes a little ahead
    /// of the bytes they cover, so that the device holds little of either without the other. Expanded on the host, the
    /// rows are read into columns of strings a piece at a time.
    Field reduceVariableColumn(const IMergeTreeDataPart & part, size_t column_idx)
    {
        const ColumnToReduce & column = state->columns[column_idx];
        const size_t bytes_total = streamBytesOf(part, column.column, {});
        const size_t sizes_total = streamBytesOf(part, column.column, stringSizesPath());
        if (sizes_total != part.rows_count * sizeof(UInt64))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of part {} holds {} bytes of sizes, where the part has {} rows",
                column.column.name,
                part.name,
                sizes_total,
                part.rows_count);

        GPU::GPUAccumulator * accumulator = nullptr;
        size_t rows_read = 0;
        size_t bytes_read = 0;

        if (expandsOnDevice(state->decompression))
        {
            MergeTreeCompressedBlockReader bytes_reader(part, column.column, state->read_settings);
            MergeTreeCompressedBlockReader sizes_reader(part, column.column, state->read_settings, stringSizesPath());
            size_t sizes_read = 0;

            while (bytes_read < bytes_total || sizes_read < sizes_total)
            {
                /// The rows the bytes read cover, were every row as long as the average one.
                const size_t rows_of_bytes = bytes_total == 0
                    ? part.rows_count
                    : static_cast<size_t>(static_cast<unsigned __int128>(bytes_read) * part.rows_count / bytes_total);
                const bool reads_sizes = sizes_read < sizes_total && (bytes_read == bytes_total || sizes_read / sizeof(UInt64) <= rows_of_bytes);
                MergeTreeCompressedBlockReader & reader = reads_sizes ? sizes_reader : bytes_reader;

                const auto block = reader.next();
                if (!block)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "A stream of column {} of part {} ended before its {} bytes", column.column.name, part.name, bytes_total);

                const std::string_view payload(block->payload, block->compressed_bytes);
                const UInt8 method = *reader.methodByte();
                const GPU::GPUCodec codec = codecOrThrow(method, payload, column.column.name, part);
                if (!accumulator)
                    accumulator = &accumulatorFor(column_idx, codec);
                else if (accumulators[column_idx]->codec != codec)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "The streams of column {} of part {} are compressed by different codecs", column.column.name, part.name);

                if (reads_sizes)
                {
                    accumulator->addSizesBlock(method, payload, block->decompressed_bytes);
                    sizes_read += block->decompressed_bytes;
                }
                else
                {
                    accumulator->addBlock(method, payload, block->decompressed_bytes);
                    bytes_read += block->decompressed_bytes;
                }

                checkNotCancelled();
            }

            rows_read = sizes_read / sizeof(UInt64);
        }
        else
        {
            accumulator = &accumulatorFor(column_idx, std::nullopt);
            CompressedReadBufferFromFile bytes_reader(MergeTreeCompressedBlockReader::openColumnFile(part, column.column, state->read_settings));
            CompressedReadBufferFromFile sizes_reader(
                MergeTreeCompressedBlockReader::openColumnFile(part, column.column, state->read_settings, stringSizesPath()));

            while (rows_read < part.rows_count)
            {
                auto strings = ColumnString::create();
                auto & offsets = strings->getOffsets();
                offsets.resize(std::min(string_piece_rows, part.rows_count - rows_read));
                sizes_reader.readStrict(reinterpret_cast<char *>(offsets.data()), offsets.size() * sizeof(UInt64));

                UInt64 piece_bytes = 0;
                for (auto & offset : offsets)
                {
                    if (offset > bytes_total - bytes_read - piece_bytes)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "The sizes of column {} of part {} add up to more than its {} bytes", column.column.name, part.name, bytes_total);
                    piece_bytes += offset;
                    offset = piece_bytes;
                }

                auto & chars = strings->getChars();
                chars.resize(piece_bytes);
                bytes_reader.readStrict(reinterpret_cast<char *>(chars.data()), piece_bytes);

                accumulator->add(*strings);
                rows_read += offsets.size();
                bytes_read += piece_bytes;

                checkNotCancelled();
            }
        }

        if (rows_read != part.rows_count || bytes_read != bytes_total)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of part {} came to {} rows of {} bytes, where the part has {} rows of {} bytes",
                column.column.name,
                part.name,
                rows_read,
                bytes_read,
                part.rows_count,
                bytes_total);

        if (!accumulator)
            return column.result_type->getDefault();

        return accumulator->finalize();
    }

    /// How many rows of a column of values of varying width the host expands at a time.
    static constexpr size_t string_piece_rows = 64 * 1024;

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

    /// Reads a column of a part as compressed blocks for the device to expand, or as values
    /// expanded on the host, a piece at a time.
    ///
    /// A column of values of varying width - a `String`, the one kind a part is read as, a key or a
    /// value - is two streams: its bytes, in the column's own file, which are read like the values of
    /// any other column, and their sizes, in the `.size` stream, which go to the device as compressed
    /// blocks, a little ahead of the bytes, for it to turn into the offsets the bytes end at. How many
    /// of its rows are read is told by the bytes read, as the rows are as long as the average one.
    struct ColumnReader
    {
        /// Among the accumulator's columns: the keys, the values, then the columns of the filter.
        size_t column_index = 0;
        /// Of a column of varying width: the accumulator's column its sizes go to.
        size_t offsets_column_index = 0;

        ColumnReader(
            const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings, bool on_device, bool in_ranges)
            : is_variable(isString(*column.type))
            , element_size(is_variable ? 1 : column.type->getSizeOfValueInMemory())
            , rows_of_part(part.rows_count)
        {
            if (in_ranges)
            {
                if (is_variable)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of varying width is read in ranges of marks for the GPU", column.name);
                marks = MergeTreeCompressedBlockReader::loadMarks(part, column, read_settings);
                compressed_bytes_of_part = part.getColumnSize(column.name).data_compressed;
            }

            if (on_device)
            {
                blocks.emplace(part, column, read_settings);
                expected_bytes = part.getColumnSize(column.name).data_compressed;
            }
            else
            {
                raw = std::make_unique<CompressedReadBufferFromFile>(MergeTreeCompressedBlockReader::openColumnFile(part, column, read_settings));
                expected_bytes = is_variable ? part.getColumnSize(column.name).data_uncompressed : part.rows_count * element_size;
            }

            if (is_variable)
            {
                if (on_device)
                    sizes_blocks.emplace(part, column, read_settings, stringSizesPath());
                else
                    sizes_raw = std::make_unique<CompressedReadBufferFromFile>(
                        MergeTreeCompressedBlockReader::openColumnFile(part, column, read_settings, stringSizesPath()));
                chars_total = streamBytesOf(part, column, {});
                sizes_total = streamBytesOf(part, column, stringSizesPath());
            }
        }

        const bool is_variable;
        std::optional<MergeTreeCompressedBlockReader> blocks;
        std::unique_ptr<CompressedReadBufferFromFile> raw;
        size_t element_size;
        size_t bytes_read = 0;
        /// What the column stages for the device: its compressed file when expanded there, its
        /// values otherwise. Only sizes the staging buffers - see `addCompressedBlock`.
        size_t expected_bytes = 0;
        size_t staged_bytes = 0;
        bool done = false;

        /// Of a column of varying width: its sizes, as compressed blocks for the device or expanded on the host like
        /// the values of the column, the bytes of them read, and the bytes of both of its streams in the part.
        std::optional<MergeTreeCompressedBlockReader> sizes_blocks;
        std::unique_ptr<CompressedReadBufferFromFile> sizes_raw;
        size_t sizes_bytes_read = 0;
        size_t sizes_total = 0;
        size_t chars_total = 0;
        bool sizes_done = false;
        bool bytes_done = false;

        /// Of a read in ranges of marks: where each granule starts, and, of the range being read, the bytes of its rows
        /// and the bytes before them in the block it starts in, which are left out.
        MergeTreeMarksGetterPtr marks;
        std::optional<size_t> range_bytes;
        size_t bytes_to_skip = 0;
        size_t compressed_bytes_of_part = 0;
        const size_t rows_of_part;

        /// From the first block of the range, of the file of its compressed blocks, or from its first row, of the
        /// values the host expands.
        void startRange(const MarkRange & range, size_t num_rows)
        {
            const MarkInCompressedFile mark = marks->getMark(range.begin, 0);
            bytes_read = 0;
            staged_bytes = 0;
            done = false;
            range_bytes = num_rows * element_size;
            if (blocks)
            {
                blocks->seekToBlock(mark.offset_in_compressed_file);
                bytes_to_skip = mark.offset_in_decompressed_block;
                expected_bytes = rows_of_part == 0 ? 0 : compressed_bytes_of_part * num_rows / rows_of_part + 1;
            }
            else
            {
                raw->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
                expected_bytes = *range_bytes;
            }
        }

        /// Of a column of varying width, the rows the bytes read make were every row as long as the average one.
        size_t rowsRead() const
        {
            if (!is_variable)
                return bytes_read / element_size;
            if (chars_total == 0)
                return sizes_bytes_read / sizeof(UInt64);
            return static_cast<size_t>(static_cast<unsigned __int128>(bytes_read) * rows_of_part / chars_total);
        }

        size_t remainingBytes() const { return expected_bytes > staged_bytes ? expected_bytes - staged_bytes : 0; }

        /// A column of varying width reads the sizes of its rows ahead of their bytes: a block of sizes whenever those
        /// read cover no more rows than the bytes read make, and only sizes once the bytes have ended.
        bool readsSizesNext() const
        {
            return is_variable && !sizes_done && (bytes_done || sizes_bytes_read / sizeof(UInt64) <= rowsRead());
        }

        /// The file of the column's values, or of the bytes of a column of varying width, has ended.
        void finishBytes()
        {
            bytes_done = true;
            done = !is_variable || sizes_done;
        }
    };

    /// The sizes of a column of varying width, to the device, which turns them into offsets: a compressed block, or a
    /// piece of them expanded on the host, in whole sizes.
    void readSizes(size_t reader_index, const IMergeTreeDataPart & part, ColumnReader & column)
    {
        if (column.sizes_raw)
        {
            const size_t left = column.sizes_total > column.sizes_bytes_read ? column.sizes_total - column.sizes_bytes_read : 0;
            if (left == 0)
            {
                column.sizes_done = true;
                column.done = column.bytes_done;
                return;
            }
            const std::span<char> room = accumulator.reserveRawBytes(reader_index, column.offsets_column_index, raw_piece_bytes, left);
            const size_t wanted = std::min(room.size() / sizeof(UInt64) * sizeof(UInt64), left);
            if (wanted == 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No room for a size of column {} of part {}", column.column_index, part.name);
            size_t read = 0;
            while (read < wanted)
            {
                const size_t got = column.sizes_raw->readBig(room.data() + read, wanted - read);
                if (got == 0)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "The sizes of column {} of part {} ended after {} bytes of {}", column.column_index, part.name, column.sizes_bytes_read + read, column.sizes_total);
                read += got;
            }
            accumulator.commitRawBytes(reader_index, column.offsets_column_index, read);
            column.sizes_bytes_read += read;
            return;
        }

        const auto block = column.sizes_blocks->next();
        if (!block)
        {
            column.sizes_done = true;
            column.done = column.bytes_done;
            return;
        }

        const std::string_view payload(block->payload, block->compressed_bytes);
        codecOrThrow(*column.sizes_blocks->methodByte(), payload, std::to_string(column.column_index) + ".size", part);
        accumulator.addCompressedBlock(
            reader_index,
            column.offsets_column_index,
            *column.sizes_blocks->methodByte(),
            payload,
            block->decompressed_bytes,
            column.sizes_total > column.sizes_bytes_read ? column.sizes_total - column.sizes_bytes_read : 0);
        column.sizes_bytes_read += block->decompressed_bytes;
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

    /// Reads the part's columns a block at a time, always the column that is furthest behind in
    /// rows, so that the columns arrive on the device within a block of each other and what waits
    /// there for the others is at most a block per column. The columns go to the accumulator in
    /// its order: the keys, the values, then the columns of the filter. A part with ranges of marks
    /// is read a range at a time, each as a part of its own to the accumulator.
    void readPart(size_t reader_index, const ReadFromGPUCompressedColumns::PartToRead & part_to_read)
    {
        const IMergeTreeDataPart & part = *part_to_read.part;
        const bool in_ranges = !part_to_read.ranges.empty();
        const std::vector<NameAndTypePair> & filter_columns = filterColumnsOf(*state);

        std::vector<std::unique_ptr<ColumnReader>> readers;
        readers.reserve(state->keys.size() + state->columns.size() + filter_columns.size());

        const bool on_device = expandsOnDevice(state->decompression);

        size_t column_index = 0;
        const auto add_reader = [&](const NameAndTypePair & column)
        {
            readers.push_back(std::make_unique<ColumnReader>(part, column, state->read_settings, on_device, in_ranges));
            readers.back()->column_index = column_index;
            if (readers.back()->is_variable)
                readers.back()->offsets_column_index = accumulator.offsetsColumnOf(column_index);
            ++column_index;
        };
        for (const auto & key : state->keys)
            add_reader(key);
        for (const auto & column : state->columns)
            add_reader(column.column);
        /// A column of the filter that is a key or a value as well is read as that column.
        for (size_t i = 0; i < filter_columns.size(); ++i)
        {
            if (state->filter_aliases[i])
                ++column_index;
            else
                add_reader(filter_columns[i]);
        }

        /// The rows of all the ranges of a part go to the device as the rows of one part, so that it groups them as they
        /// come, however many ranges there are, rather than drain its work at the end of each.
        size_t num_rows_read = 0;
        if (!in_ranges)
        {
            readRows(reader_index, part, readers, part.rows_count);
            num_rows_read = part.rows_count;
        }
        else
        {
            for (const MarkRange & range : part_to_read.ranges)
            {
                const size_t num_rows = part.index_granularity->getRowsCountInRange(range);
                if (num_rows == 0)
                    continue;
                for (auto & reader : readers)
                    reader->startRange(range, num_rows);
                readRows(reader_index, part, readers, num_rows);
                num_rows_read += num_rows;
            }
        }
        accumulator.finishPart(reader_index, num_rows_read);

    }

    /// The `num_rows` rows of every column from where its reader is: every row of the part, or a range of it. The part is
    /// finished by the caller once all of its ranges are read.
    void readRows(size_t reader_index, const IMergeTreeDataPart & part, std::vector<std::unique_ptr<ColumnReader>> & readers, size_t num_rows)
    {
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
            if (column.readsSizesNext())
            {
                readSizes(reader_index, part, column);
            }
            else if (column.raw)
            {
                if (column.remainingBytes() == 0)
                {
                    /// A range ends where its rows do. Of a whole part, every value is staged, and the buffer holds
                    /// exactly them. Growing it only to learn that the file has ended would copy it; a byte past the
                    /// end is counted instead, so the check below reports it.
                    if (!column.range_bytes)
                    {
                        char past_end;
                        column.bytes_read += column.raw->readBig(&past_end, 1);
                    }
                    column.finishBytes();
                    continue;
                }

                const std::span<char> room = accumulator.reserveRawBytes(reader_index, column.column_index, raw_piece_bytes, column.remainingBytes());
                const size_t read = column.raw->readBig(
                    room.data(), column.range_bytes ? std::min(room.size(), column.remainingBytes()) : room.size());
                accumulator.commitRawBytes(reader_index, column.column_index, read);
                if (read == 0)
                {
                    column.finishBytes();
                    continue;
                }

                column.bytes_read += read;
                column.staged_bytes += read;
            }
            else
            {
                const auto block = column.blocks->next();
                if (!block)
                {
                    column.finishBytes();
                    continue;
                }

                /// Of a range, the rows before it in its first block and after it in its last are left out.
                size_t skipped = 0;
                size_t kept = block->decompressed_bytes;
                if (column.range_bytes)
                {
                    skipped = std::min(column.bytes_to_skip, block->decompressed_bytes);
                    column.bytes_to_skip -= skipped;
                    kept = std::min(block->decompressed_bytes - skipped, *column.range_bytes - column.bytes_read);
                }

                if (kept != 0)
                {
                    const std::string_view payload(block->payload, block->compressed_bytes);
                    codecOrThrow(*column.blocks->methodByte(), payload, std::to_string(column.column_index), part);
                    accumulator.addCompressedBlock(
                        reader_index,
                        column.column_index,
                        *column.blocks->methodByte(),
                        payload,
                        block->decompressed_bytes,
                        column.remainingBytes(),
                        skipped,
                        kept);
                    column.staged_bytes += block->compressed_bytes;
                }
                column.bytes_read += kept;
                if (column.range_bytes && column.bytes_read == *column.range_bytes)
                    column.finishBytes();
            }

            checkNotCancelled();
        }

        for (size_t i = 0; i < readers.size(); ++i)
        {
            if (readers[i]->is_variable)
            {
                if (readers[i]->sizes_bytes_read != num_rows * sizeof(UInt64) || readers[i]->bytes_read != readers[i]->chars_total)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} of varying width of part {} holds {} bytes of sizes and {} bytes of {}, where the part has {} rows",
                        i,
                        part.name,
                        readers[i]->sizes_bytes_read,
                        readers[i]->bytes_read,
                        readers[i]->chars_total,
                        num_rows);
                continue;
            }

            if (readers[i]->bytes_read != num_rows * readers[i]->element_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} of part {} holds {} bytes, where {} rows of {} bytes are read",
                    i,
                    part.name,
                    readers[i]->bytes_read,
                    num_rows,
                    readers[i]->element_size);
        }
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
    std::optional<DeviceFilter> filter_,
    std::vector<PartToRead> parts_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t batch_bytes_,
    size_t num_streams_,
    size_t num_readers_,
    GPUDecompressionMode decompression_,
    bool collect_)
    : ISourceStep(std::move(output_header_))
    , keys(std::move(keys_))
    , columns(std::move(columns_))
    , derived(std::move(derived_))
    , aggregates(std::move(aggregates_))
    , filter(std::move(filter_))
    , parts(std::move(parts_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , batch_bytes(batch_bytes_)
    , num_streams(num_streams_)
    , num_readers(num_readers_)
    , decompression(decompression_)
    , collect(collect_)
{
    if (collect && (!keys.empty() || !aggregates.empty() || !derived.empty() || columns.empty()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read that collects rows of {} columns with keys or aggregates", columns.size());

    if (filter && keys.empty() && !collect)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A keyless GPU aggregation over compressed columns with a `WHERE` for the device");

    if (keys.empty() != aggregates.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over compressed columns of {} keys and {} aggregates", keys.size(), aggregates.size());
}

void ReadFromGPUCompressedColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<SharedState>();
    state->keys = keys;
    state->columns = columns;
    state->derived = derived;
    state->aggregates = aggregates;
    state->filter = filter;
    state->group_filter = group_filter;
    state->parts = std::move(parts);
    state->storage_snapshot = storage_snapshot;
    state->context = context;
    state->read_settings = context->getReadSettings();
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

    /// A keyed or a collecting read, of columns of a fixed width, reads a large part in pieces, a reader each.
    if ((!keys.empty() || collect) && readsFixedWidthOnly(*state))
        splitPartsAmongReaders(state->parts, 2 * state->num_readers);

    Pipes pipes;

    if (!keys.empty() || collect)
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
