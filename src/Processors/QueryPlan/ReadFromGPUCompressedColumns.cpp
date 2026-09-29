#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>

#if USE_GPU

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/Block.h>
#include <GPU/GPUAccumulator.h>
#include <GPU/GPUColumns.h>
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
    GPUDecompressionMode decompression = GPUDecompressionMode::RATIO;
    double device_decompression_max_ratio = 1;

    std::atomic<size_t> next_part{0};
};

using SharedStatePtr = std::shared_ptr<SharedState>;

/// Whether a column of a part is compressed well enough for the device to expand it. Expanding
/// costs the device about as long per byte as the link takes to carry one, and keeps it from
/// grouping meanwhile, so a column that compression barely shrinks is cheaper sent whole.
bool expandsOnDevice(const IMergeTreeDataPart & part, const String & column_name, GPUDecompressionMode mode, double max_ratio)
{
    switch (mode)
    {
        case GPUDecompressionMode::DEVICE:
            return true;
        case GPUDecompressionMode::HOST:
            return false;
        case GPUDecompressionMode::RATIO:
        case GPUDecompressionMode::AUTO:
            break;
    }

    if (max_ratio >= 1)
        return true;

    const ColumnSize size = part.getColumnSize(column_name);
    if (size.data_uncompressed == 0)
        return true;
    return static_cast<double>(size.data_compressed) <= max_ratio * static_cast<double>(size.data_uncompressed);
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
            if (!expandsOnDevice(*part, key.name, state.decompression, state.device_decompression_max_ratio))
                return expanding_readers;
        }
        for (const auto & column : state.columns)
        {
            if (!expandsOnDevice(*part, column.column.name, state.decompression, state.device_decompression_max_ratio))
                return expanding_readers;
        }
        for (const auto & column : filterColumnsOf(state))
        {
            if (!expandsOnDevice(*part, column.name, state.decompression, state.device_decompression_max_ratio))
                return expanding_readers;
        }
    }
    return 2;
}

/** The threshold on a column's compression ratio that `gpu_aggregation_decompression = 'auto'`
  * reads the parts of a `GROUP BY` by. It starts where `gpu_aggregation_device_decompression_max_ratio`
  * is and moves after every part toward whichever side waited on the other since the last move:
  * when the device thread waited for work, the reading threads are behind and more columns are
  * left to the device to expand; when the reading threads waited for room in the device thread's
  * queue, the device is behind and more columns are expanded by the reading threads.
  */
class AdaptiveDecompressionRatio
{
public:
    AdaptiveDecompressionRatio(double initial, size_t num_readers_)
        : num_readers(num_readers_)
        , ratio(std::clamp(initial, 0.0, 1.0))
    {
    }

    double current() const
    {
        std::lock_guard lock(mutex);
        return ratio;
    }

    void update(const GPU::GroupByGPUAccumulator::Waits & waits)
    {
        std::lock_guard lock(mutex);

        /// Every reader waits when the device is behind, so their waits are compared per reader.
        const UInt64 readers_waited = (waits.readers_microseconds - last.readers_microseconds) / num_readers;
        const UInt64 device_waited = waits.device_microseconds - last.device_microseconds;
        last = waits;

        const double previous = ratio;
        if (device_waited >= std::max(min_wait_microseconds, 2 * readers_waited))
            ratio = std::min(1.0, ratio + step);
        else if (readers_waited >= std::max(min_wait_microseconds, 2 * device_waited))
            ratio = std::max(0.0, ratio - step);

        if (ratio != previous)
            LOG_TRACE(
                log,
                "The device waited {} us and each reader {} us since the last part, so columns compressed to at most {} of their size are "
                "expanded on the device from now on",
                device_waited,
                readers_waited,
                ratio);
    }

private:
    static constexpr double step = 0.125;
    /// Less than this is noise, whoever waited it.
    static constexpr UInt64 min_wait_microseconds = 1000;

    const size_t num_readers;
    const LoggerPtr log = getLogger("GPUCompressedColumns");

    mutable std::mutex mutex;
    double ratio TSA_GUARDED_BY(mutex);
    GPU::GroupByGPUAccumulator::Waits last TSA_GUARDED_BY(mutex);
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

        if (expandsOnDevice(part, column.column.name, state->decompression, state->device_decompression_max_ratio))
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

        if (expandsOnDevice(part, column.column.name, state->decompression, state->device_decompression_max_ratio))
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

                const GPU::GPUCodec codec = codecOrThrow(*reader.methodByte(), column.column.name, part);
                if (!accumulator)
                    accumulator = &accumulatorFor(column_idx, codec);
                else if (accumulators[column_idx]->codec != codec)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "The streams of column {} of part {} are compressed by different codecs", column.column.name, part.name);

                const std::string_view payload(block->payload, block->compressed_bytes);
                if (reads_sizes)
                {
                    accumulator->addSizesBlock(payload, block->decompressed_bytes);
                    sizes_read += block->decompressed_bytes;
                }
                else
                {
                    accumulator->addBlock(payload, block->decompressed_bytes);
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
        if (state->decompression == GPUDecompressionMode::AUTO)
            adaptive_ratio.emplace(state->device_decompression_max_ratio, num_readers);
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
    /// expanded on the host, a piece at a time.
    ///
    /// A column of values of varying width - a `String`, the one kind a part is read as, a key or a
    /// value - is two streams: its bytes, in the column's own file, which are read like the values of
    /// any other column, and their sizes, in the `.size` stream, which are read on the host ahead of
    /// the bytes and go to the device as the offsets the bytes end at. Its rows read are those whose
    /// bytes all are.
    struct ColumnReader
    {
        ColumnReader(const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings, bool on_device)
            : is_variable(GPU::columnTypeOrThrow(*column.type) == GPU::GPUElementType::String)
            , element_size(is_variable ? 1 : column.type->getSizeOfValueInMemory())
        {
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
                sizes = std::make_unique<CompressedReadBufferFromFile>(
                    MergeTreeCompressedBlockReader::openColumnFile(part, column, read_settings, stringSizesPath()));
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

        /// Of a variable-width key: the reader of its sizes, where the rows whose sizes are read and whose
        /// bytes are not all read yet end, and how many rows of either there are.
        std::unique_ptr<CompressedReadBufferFromFile> sizes;
        std::deque<UInt64> pending_ends;
        UInt64 last_end = 0;
        size_t rows_sized = 0;
        size_t variable_rows_read = 0;
        bool sizes_done = false;
        bool bytes_done = false;

        size_t rowsRead() const { return is_variable ? variable_rows_read : bytes_read / element_size; }
        size_t remainingBytes() const { return expected_bytes > staged_bytes ? expected_bytes - staged_bytes : 0; }

        /// A variable-width key reads the sizes of rows beyond the bytes read before it reads more bytes,
        /// and only sizes once the bytes have ended.
        bool readsSizesNext() const
        {
            return is_variable && !sizes_done && (bytes_done || pending_ends.empty() || last_end <= bytes_read);
        }

        void countVariableRowsRead()
        {
            while (!pending_ends.empty() && pending_ends.front() <= bytes_read)
            {
                pending_ends.pop_front();
                ++variable_rows_read;
            }
            done = sizes_done && bytes_done;
        }

        /// The file of the column's values, or of a variable-width key's bytes, has ended.
        void finishBytes()
        {
            if (!is_variable)
            {
                done = true;
                return;
            }

            bytes_done = true;
            countVariableRowsRead();
        }
    };

    /// How many sizes of a variable-width key are read on the host at a time.
    static constexpr size_t sizes_piece_rows = 64 * 1024;

    void readVariableSizes(size_t reader_index, size_t key_index, ColumnReader & column)
    {
        std::vector<UInt64> ends(sizes_piece_rows);
        const size_t read = column.sizes->readBig(reinterpret_cast<char *>(ends.data()), ends.size() * sizeof(UInt64));
        if (read % sizeof(UInt64) != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of variable-width key {} end in the middle of a size, after {} bytes", key_index, read);

        const size_t num_rows = read / sizeof(UInt64);
        if (num_rows == 0)
        {
            column.sizes_done = true;
            column.countVariableRowsRead();
            return;
        }

        for (size_t i = 0; i < num_rows; ++i)
        {
            column.last_end += ends[i];
            ends[i] = column.last_end;
            column.pending_ends.push_back(column.last_end);
        }
        column.rows_sized += num_rows;

        accumulator.addVariableOffsets(reader_index, key_index, std::span<const UInt64>(ends.data(), num_rows));
        column.countVariableRowsRead();
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

        const double max_ratio = adaptive_ratio ? adaptive_ratio->current() : state->device_decompression_max_ratio;
        const auto on_device = [&](const String & column_name)
        {
            return expandsOnDevice(part, column_name, state->decompression, max_ratio);
        };

        for (const auto & key : state->keys)
            readers.push_back(std::make_unique<ColumnReader>(part, key, state->read_settings, on_device(key.name)));
        for (const auto & column : state->columns)
            readers.push_back(std::make_unique<ColumnReader>(part, column.column, state->read_settings, on_device(column.column.name)));
        for (const auto & column : filter_columns)
            readers.push_back(std::make_unique<ColumnReader>(part, column, state->read_settings, on_device(column.name)));

        /// The offsets of a column of values of varying width start from the 0 its first row starts at.
        for (size_t i = 0; i < readers.size(); ++i)
        {
            if (readers[i]->is_variable)
            {
                const UInt64 start = 0;
                accumulator.addVariableOffsets(reader_index, i, std::span<const UInt64>(&start, 1));
            }
        }

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
                readVariableSizes(reader_index, behind, column);
            }
            else if (column.raw)
            {
                /// Every value is staged, and the buffer holds exactly them. Growing it only to learn
                /// that the file has ended would copy it; a byte past the end is counted instead, so
                /// the check below reports it.
                if (column.remainingBytes() == 0)
                {
                    char past_end;
                    column.bytes_read += column.raw->readBig(&past_end, 1);
                    column.finishBytes();
                    continue;
                }

                const std::span<char> room = accumulator.reserveRawBytes(reader_index, behind, raw_piece_bytes, column.remainingBytes());
                const size_t read = column.raw->readBig(room.data(), room.size());
                accumulator.commitRawBytes(reader_index, behind, read);
                if (read == 0)
                {
                    column.finishBytes();
                    continue;
                }

                column.bytes_read += read;
                column.staged_bytes += read;
                if (column.is_variable)
                    column.countVariableRowsRead();
            }
            else
            {
                const auto block = column.blocks->next();
                if (!block)
                {
                    column.finishBytes();
                    continue;
                }

                const GPU::GPUCodec codec = codecOrThrow(*column.blocks->methodByte(), std::to_string(behind), part);
                accumulator.addCompressedBlock(
                    reader_index,
                    behind,
                    codec,
                    std::string_view(block->payload, block->compressed_bytes),
                    block->decompressed_bytes,
                    column.remainingBytes());
                column.bytes_read += block->decompressed_bytes;
                column.staged_bytes += block->compressed_bytes;
                if (column.is_variable)
                    column.countVariableRowsRead();
            }

            checkNotCancelled();
        }

        for (size_t i = 0; i < readers.size(); ++i)
        {
            if (readers[i]->is_variable)
            {
                if (readers[i]->rows_sized != part.rows_count || readers[i]->variable_rows_read != part.rows_count
                    || readers[i]->bytes_read != readers[i]->last_end)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Variable-width column {} of part {} holds {} sizes adding up to {} bytes and {} bytes, where the part has {} rows",
                        i,
                        part.name,
                        readers[i]->rows_sized,
                        readers[i]->last_end,
                        readers[i]->bytes_read,
                        part.rows_count);
                continue;
            }

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

        if (adaptive_ratio)
            adaptive_ratio->update(accumulator.waits());
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
    /// Only under `gpu_aggregation_decompression = 'auto'`.
    std::optional<AdaptiveDecompressionRatio> adaptive_ratio;
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
    GPUDecompressionMode decompression_,
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
    , decompression(decompression_)
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
    state->decompression = decompression;
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
