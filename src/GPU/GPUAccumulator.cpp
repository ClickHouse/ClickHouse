#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <cstring>
#include <numeric>

namespace ProfileEvents
{
    extern const Event GPUAggregationRows;
    extern const Event GPUAggregationBatches;
    extern const Event GPUAggregationMicroseconds;
    extern const Event GPUGroupByKernelMicroseconds;
    extern const Event GPUDecompressionMicroseconds;
    extern const Event GPUDecompressionBytes;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

GPUAccumulator::GPUAccumulator(
    const IDataType & argument_type,
    const IDataType & result_type_,
    GPUAggregationKind aggregation_,
    size_t batch_bytes_,
    std::optional<GPUCodec> codec_)
    : element_type(reducibleElementTypeOrThrow(argument_type, result_type_, aggregation_))
    , result_type(sumResultTypeFor(element_type))
    , aggregation(aggregation_)
    , element_size(sizeOf(element_type))
    , batch_bytes(std::clamp(batch_bytes_, element_size, max_batch_rows * element_size))
    , reduction(onDevice(
          [&] { return std::make_unique<CudfReduction>(element_type, result_type, aggregation); },
          "Cannot set up a `{}` of {} on the device",
          aggregationName(aggregation_),
          argument_type.getName()))
{
    const size_t stage_bytes = std::min(batch_bytes, max_stage_bytes);
    if (codec_)
        pipe = std::make_unique<CompressedUploadPipe>(argument_type, stage_bytes, *codec_);
    else
        pipe = std::make_unique<ColumnUploadPipe>(argument_type, stage_bytes);
}

void GPUAccumulator::add(const IColumn & column)
{
    plainPipeOrThrow().stage(column);

    if (pipe->stagedRows() * element_size >= batch_bytes)
        reduceBatchOnDevice();
}

std::span<char> GPUAccumulator::reserveRaw(size_t max_bytes)
{
    return plainPipeOrThrow().reserveRaw(max_bytes);
}

void GPUAccumulator::commitRaw(size_t bytes)
{
    plainPipeOrThrow().commitRaw(bytes);

    if (pipe->stagedRows() * element_size >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::addBlock(std::string_view payload, size_t decompressed_bytes)
{
    auto * compressed_pipe = typeid_cast<CompressedUploadPipe *>(pipe.get());
    if (!compressed_pipe)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A compressed block for an accumulator of plain values");

    compressed_pipe->stageCompressedBlock(payload, decompressed_bytes);

    if (pipe->stagedRows() * element_size >= batch_bytes)
        reduceBatchOnDevice();
}

ColumnUploadPipe & GPUAccumulator::plainPipeOrThrow()
{
    auto * plain_pipe = typeid_cast<ColumnUploadPipe *>(pipe.get());
    if (!plain_pipe)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain values for an accumulator of compressed blocks");
    return *plain_pipe;
}

void GPUAccumulator::reduceBatchOnDevice()
{
    if (pipe->stagedRows() == 0)
        return;

    Stopwatch watch;

    const DeviceFixedColumn values = fixedOrThrow(pipe->flush().view());

    onDevice([&] { reduction->addBatch(values); }, "Cannot reduce {} values by `{}` on a GPU", values.rows, aggregationName(aggregation));

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, values.rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches, 1);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    pipe->reset();
}

Field GPUAccumulator::finalize()
{
    reduceBatchOnDevice();

    if (pipe->stagedBytes() != 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A column ended with {} bytes of a {}-byte value",
            pipe->stagedBytes(),
            element_size);

    Stopwatch watch;
    const UInt64 bits = onDevice([&] { return reduction->finalize(); }, "Cannot read a `{}` back from a GPU", aggregationName(aggregation));
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    switch (result_type)
    {
        case GPUElementType::UInt64:
            return Field(bits);
        case GPUElementType::Int64:
            return Field(static_cast<Int64>(bits));
        case GPUElementType::Float64:
            return Field(std::bit_cast<Float64>(bits));
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU reduction into {}, which is not eight bytes wide", result_type);
    }
}

namespace
{

std::vector<GPUGroupByValue> groupByValuesOrThrow(
    const DataTypes & argument_types, const DataTypes & result_types, const std::vector<GPUAggregationKind> & aggregations)
{
    if (argument_types.size() != result_types.size() || argument_types.size() != aggregations.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation of {} arguments, {} result types and {} aggregate functions",
            argument_types.size(),
            result_types.size(),
            aggregations.size());

    std::vector<GPUGroupByValue> values;
    values.reserve(argument_types.size());

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        const GPUElementType element_type = reducibleElementTypeOrThrow(*argument_types[i], *result_types[i], aggregations[i]);

        values.push_back({
            .element_type = element_type,
            .result_type = sumResultTypeFor(element_type),
            .aggregation = aggregations[i],
        });
    }

    return values;
}

size_t rowBytesOf(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUGroupByValue> & values,
    const std::vector<GPUElementType> & filter_element_types)
{
    size_t bytes = 0;
    for (const GPUElementType key_element_type : key_element_types)
        bytes += sizeOf(key_element_type);
    for (const GPUGroupByValue & value : values)
        bytes += sizeOf(value.element_type);
    for (const GPUElementType filter_element_type : filter_element_types)
        bytes += sizeOf(filter_element_type);
    return bytes;
}

std::vector<ColumnUploadPipe> pipesFor(const DataTypes & types, size_t batch_rows, bool compressed)
{
    std::vector<ColumnUploadPipe> pipes;
    if (compressed)
        return pipes;

    pipes.reserve(types.size());
    for (const auto & type : types)
    {
        const GPUElementType column_type = columnTypeOrThrow(*type);
        const size_t stage_bytes = columnKindOf(column_type) == GPUColumnKind::Fixed
            ? std::min(batch_rows * sizeOf(column_type), GPUAccumulator::max_stage_bytes)
            : GPUAccumulator::max_stage_bytes;
        pipes.emplace_back(*type, stage_bytes);
    }
    return pipes;
}

std::vector<GPUElementType> keyElementTypesOf(const std::vector<GPUElementType> & keys)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(keys.size());
    for (const auto & key : keys)
        element_types.push_back(key == GPUElementType::String ? GPUElementType::UInt8 : key);
    return element_types;
}

std::vector<size_t> variableKeyIndicesOf(const std::vector<GPUElementType> & keys)
{
    std::vector<size_t> indices;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (keys[i] == GPUElementType::String)
            indices.push_back(i);
    }
    return indices;
}

std::vector<DeviceFixedColumnBuffer> deviceColumnsFor(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUGroupByValue> & values,
    const std::vector<GPUElementType> & filter_element_types,
    size_t num_variable_keys,
    bool compressed)
{
    std::vector<DeviceFixedColumnBuffer> columns;
    if (!compressed)
        return columns;

    columns.reserve(key_element_types.size() + values.size() + filter_element_types.size() + num_variable_keys);
    for (const GPUElementType key_element_type : key_element_types)
        columns.emplace_back(key_element_type);
    for (const GPUGroupByValue & value : values)
        columns.emplace_back(value.element_type);
    for (const GPUElementType filter_element_type : filter_element_types)
        columns.emplace_back(filter_element_type);
    for (size_t i = 0; i < num_variable_keys; ++i)
        columns.emplace_back(GPUElementType::UInt64);
    return columns;
}

std::vector<DeviceFixedColumn> fixedColumnsOf(const std::vector<DeviceColumnView> & views)
{
    std::vector<DeviceFixedColumn> columns;
    columns.reserve(views.size());
    for (const auto & view : views)
        columns.push_back(fixedOrThrow(view));
    return columns;
}

size_t rowsCoveredBy(const std::vector<UInt64> & offsets, size_t chars_bytes)
{
    if (offsets.empty())
        return 0;
    return static_cast<size_t>(std::upper_bound(offsets.begin() + 1, offsets.end(), chars_bytes) - (offsets.begin() + 1));
}

constexpr size_t compressed_stage_bytes = 256UL * 1024 * 1024;

constexpr size_t queued_buffers_per_column = 2;

constexpr size_t group_piece_rows = 4UL << 20;

}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations,
    size_t batch_bytes,
    bool compressed_,
    size_t num_readers_,
    const DataTypes & filter_types,
    std::optional<GPUFilterProgram> filter_)
    : group_keys(groupByKeysOrThrow(key_types))
    , key_element_types(keyElementTypesOf(group_keys))
    , variable_key_indices(variableKeyIndicesOf(group_keys))
    , values(groupByValuesOrThrow(argument_types, result_types, aggregations))
    , filter_element_types(elementTypesOrThrow(filter_types))
    , filter(std::move(filter_))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_types, values, filter_element_types), size_t{1}, max_batch_rows))
    , compressed(compressed_)
    , key_pipes(pipesFor(key_types, batch_rows, compressed))
    , value_pipes(pipesFor(argument_types, batch_rows, compressed))
{
    if (variable_key_indices.empty())
        group_by = onDevice(
            [&] { return std::make_unique<RecordGroupBy>(key_element_types, values); },
            "Cannot set up a `GROUP BY` over {} keys on the device",
            key_element_types.size());
    else
    {
        variable_group_stream = std::make_unique<DeviceStream>();
        variable_group_by = onDevice(
            [&] { return std::make_unique<CudfGroupBy>(group_keys, values, variable_group_stream->get()); },
            "Cannot set up a `GROUP BY` over {} keys, {} of them of variable width, on the device",
            group_keys.size(),
            variable_key_indices.size());
    }

    if (filter && !variable_key_indices.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation by variable-width keys with a `WHERE` for the device");

    if (filter.has_value() != !filter_element_types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with a `WHERE` and the columns of one do not go together");

    if (filter && filter->num_columns != filter_element_types.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation's `WHERE` reads {} columns and is given {}",
            filter->num_columns,
            filter_element_types.size());

    if (!compressed)
    {
        if (filter)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over plain columns with a `WHERE` for the device");
        return;
    }

    if (num_readers_ == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over compressed blocks with no reading threads");

    const size_t num_columns = numColumns();
    readers.reserve(num_readers_);
    for (size_t i = 0; i < num_readers_; ++i)
    {
        Reader & reader = readers.emplace_back();
        reader.staging.resize(num_columns);
        reader.device_columns = deviceColumnsFor(key_element_types, values, filter_element_types, variable_key_indices.size(), compressed);
        reader.variable_offsets.resize(variable_key_indices.size());
        for (size_t j = 0; j < variable_key_indices.size(); ++j)
            reader.piece_offsets.emplace_back(StreamRegistry::get().compute);
    }

    work_queue = std::make_unique<ConcurrentBoundedQueue<DeviceWork>>(queued_buffers_per_column * (num_columns + 1) * num_readers_);

    device_thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup()]
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_GROUP_BY);
        runDeviceThread();
    });
}

GroupByGPUAccumulator::~GroupByGPUAccumulator()
{
    if (!device_thread.joinable())
        return;

    try
    {
        work_queue->finish();
        device_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void GroupByGPUAccumulator::add(const Columns & key_columns, const Columns & value_columns)
{
    if (compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of plain columns for a GPU aggregation over compressed blocks");

    if (key_columns.size() != group_keys.size() || value_columns.size() != value_pipes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A block of {} key and {} value columns for a GPU aggregation over {} and {}",
            key_columns.size(),
            value_columns.size(),
            group_keys.size(),
            value_pipes.size());

    const size_t num_rows = key_columns.front()->size();
    if (num_rows == 0)
        return;

    if (stagedRows() + num_rows > max_batch_rows)
        sendBatchToDevice();

    for (size_t i = 0; i < key_columns.size(); ++i)
        key_pipes[i].stage(*key_columns[i]);

    for (size_t i = 0; i < value_columns.size(); ++i)
        value_pipes[i].stage(*value_columns[i]);

    if (stagedRows() >= batch_rows || stagedVariableBytes() >= GPUAccumulator::max_stage_bytes)
        sendBatchToDevice();
}

size_t GroupByGPUAccumulator::stagedVariableBytes() const
{
    size_t bytes = 0;
    for (const auto & pipe : key_pipes)
    {
        if (pipe.type() == GPUElementType::String)
            bytes += pipe.stagedBytes();
    }
    return bytes;
}

size_t GroupByGPUAccumulator::variableOffsetsColumnOf(size_t key_index) const
{
    const auto it = std::find(variable_key_indices.begin(), variable_key_indices.end(), key_index);
    if (it == variable_key_indices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key {} of a GPU aggregation is not of variable width", key_index);

    return group_keys.size() + values.size() + filter_element_types.size() + static_cast<size_t>(it - variable_key_indices.begin());
}

void GroupByGPUAccumulator::addVariableOffsets(size_t reader_index, size_t key_index, std::span<const UInt64> offsets)
{
    Reader & reader = readerOrThrow(reader_index);
    rethrowDeviceError();

    const size_t column_index = variableOffsetsColumnOf(key_index);
    StagedColumn & column = reader.staging[column_index];

    const size_t bytes = offsets.size_bytes();
    if (!column.staged.empty() && column.staged.size() + bytes > compressed_stage_bytes)
        handOff(reader_index, column_index);

    column.raw = true;
    column.staged.append({reinterpret_cast<const char *>(offsets.data()), bytes});
    column.decompressed_bytes += bytes;
}

std::optional<size_t> GroupByGPUAccumulator::variableOrdinalOfOffsetsColumn(size_t column_index) const
{
    const size_t first = group_keys.size() + values.size() + filter_element_types.size();
    if (column_index < first || column_index >= numColumns())
        return std::nullopt;
    return column_index - first;
}

void GroupByGPUAccumulator::sendBatchToDevice()
{
    const size_t num_rows = stagedRows();
    if (num_rows == 0)
        return;

    Stopwatch watch;

    std::vector<DeviceColumnView> keys;
    keys.reserve(key_pipes.size());
    for (auto & pipe : key_pipes)
        keys.push_back(pipe.flush().view());

    std::vector<DeviceFixedColumn> value_columns;
    value_columns.reserve(value_pipes.size());
    for (auto & pipe : value_pipes)
        value_columns.push_back(fixedOrThrow(pipe.flush().view()));

    double kernel_microseconds = 0;
    if (variable_group_by)
        groupByVariable(keys, value_columns, num_rows);
    else
        kernel_microseconds = onDevice(
            [&] { return group_by->addBatch(fixedColumnsOf(keys), value_columns, {}, nullptr); }, "Cannot group {} rows on the device", num_rows);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUGroupByKernelMicroseconds, static_cast<UInt64>(kernel_microseconds));

    for (auto & pipe : key_pipes)
        pipe.reset();
    for (auto & pipe : value_pipes)
        pipe.reset();
}

GroupByGPUAccumulator::Reader & GroupByGPUAccumulator::readerOrThrow(size_t reader)
{
    if (!compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A compressed block for a GPU aggregation over plain columns");

    if (reader >= readers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reader {} of a GPU aggregation with {} readers", reader, readers.size());

    return readers[reader];
}

void GroupByGPUAccumulator::addCompressedBlock(
    size_t reader_index, size_t column_index, GPUCodec codec, std::string_view payload, size_t decompressed_bytes, size_t expected_bytes)
{
    Reader & reader = readerOrThrow(reader_index);

    if (column_index >= reader.staging.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation over {} columns", column_index, reader.staging.size());

    rethrowDeviceError();

    StagedColumn & column = reader.staging[column_index];

    if (column.raw || (!column.blocks.empty() && (column.decompressed_bytes + decompressed_bytes > compressed_stage_bytes || column.codec != codec)))
        handOff(reader_index, column_index);

    if (column.blocks.empty())
        column.staged.reserve(std::min(compressed_stage_bytes, std::max(expected_bytes, payload.size())));

    column.codec = codec;
    column.blocks.push_back({
        .offset = column.staged.size(),
        .compressed_bytes = payload.size(),
        .decompressed_bytes = decompressed_bytes,
    });
    column.staged.append(payload);
    column.decompressed_bytes += decompressed_bytes;
}

std::span<char> GroupByGPUAccumulator::reserveRawBytes(size_t reader_index, size_t column_index, size_t max_bytes, size_t expected_bytes)
{
    Reader & reader = readerOrThrow(reader_index);

    if (column_index >= reader.staging.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation over {} columns", column_index, reader.staging.size());

    rethrowDeviceError();

    StagedColumn & column = reader.staging[column_index];

    if (!column.blocks.empty() || column.staged.size() >= compressed_stage_bytes)
        handOff(reader_index, column_index);

    column.raw = true;
    column.staged.reserve(std::min(compressed_stage_bytes, column.staged.size() + std::max<size_t>(expected_bytes, 1)));
    return {
        column.staged.data() + column.staged.size(),
        std::min({max_bytes, compressed_stage_bytes - column.staged.size(), column.staged.available()})};
}

void GroupByGPUAccumulator::commitRawBytes(size_t reader_index, size_t column_index, size_t bytes)
{
    StagedColumn & column = readerOrThrow(reader_index).staging[column_index];
    column.staged.grow(bytes);
    column.decompressed_bytes += bytes;

    if (column.staged.size() >= compressed_stage_bytes)
        handOff(reader_index, column_index);
}

void GroupByGPUAccumulator::handOff(size_t reader_index, size_t column_index)
{
    StagedColumn & column = readers[reader_index].staging[column_index];
    if (column.staged.empty())
        return;

    DeviceWork work;
    work.kind = column.raw ? DeviceWork::Kind::Raw : DeviceWork::Kind::Blocks;
    work.reader = reader_index;
    work.column_index = column_index;
    if (!column.raw)
        work.codec = *column.codec;
    work.staged = std::move(column.staged);
    work.blocks = std::move(column.blocks);

    work.on_device = DeviceBuffer(StreamRegistry::get().upload);
    work.on_device.append(work.staged.bytes());
    work.uploaded.emplace();
    work.uploaded->record(StreamRegistry::get().upload);

    column.staged = PinnedBuffer{};
    column.blocks = {};
    column.codec.reset();
    column.raw = false;
    column.decompressed_bytes = 0;

    enqueue(std::move(work));
}

void GroupByGPUAccumulator::enqueue(DeviceWork && work)
{
    rethrowDeviceError();

    Stopwatch watch;
    const bool pushed = work_queue->push(std::move(work));
    readers_wait_microseconds.fetch_add(watch.elapsedMicroseconds(), std::memory_order_relaxed);

    if (!pushed)
    {
        rethrowDeviceError();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The GPU aggregation's device thread stopped taking work without an error");
    }
}

void GroupByGPUAccumulator::rethrowDeviceError()
{
    if (!device_failed.load(std::memory_order_acquire))
        return;

    std::lock_guard lock(device_error_mutex);
    std::rethrow_exception(device_error);
}

void GroupByGPUAccumulator::finishPart(size_t reader_index, size_t num_rows)
{
    Reader & reader = readerOrThrow(reader_index);

    for (size_t i = 0; i < reader.staging.size(); ++i)
        handOff(reader_index, i);

    DeviceWork work;
    work.kind = DeviceWork::Kind::EndOfPart;
    work.reader = reader_index;
    work.num_rows = num_rows;
    enqueue(std::move(work));
}

void GroupByGPUAccumulator::runDeviceThread()
{
    try
    {
        std::optional<DeviceWork> carried;

        while (true)
        {
            std::vector<DeviceWork> blocks;
            std::optional<DeviceWork> other;

            flushPendingRaw(false);

            const bool idle = expanding.empty() && raw_pending.empty() && !carried && !hasPieceToGroup();
            DeviceWork work;
            bool taken = false;
            if (carried)
            {
                work = std::move(*carried);
                carried.reset();
                taken = true;
            }
            else
            {
                if (idle)
                {
                    Stopwatch watch;
                    taken = work_queue->pop(work);
                    device_wait_microseconds.fetch_add(watch.elapsedMicroseconds(), std::memory_order_relaxed);
                }
                else
                {
                    taken = work_queue->tryPop(work);
                }
                if (!taken && idle)
                    return;
            }

            while (taken)
            {
                if (work.kind != DeviceWork::Kind::Blocks || (!blocks.empty() && work.codec != blocks.front().codec))
                {
                    other = std::move(work);
                    break;
                }
                blocks.push_back(std::move(work));
                taken = work_queue->tryPop(work);
            }

            if (!blocks.empty())
            {
                finishExpansion();
                startExpansion(std::move(blocks));
            }

            if (other)
            {
                switch (other->kind)
                {
                    case DeviceWork::Kind::Blocks:
                        carried = std::move(*other);
                        break;
                    case DeviceWork::Kind::Raw:
                        appendRaw(std::move(*other));
                        break;
                    case DeviceWork::Kind::EndOfPart:
                        finishPartOnDevice(other->reader, other->num_rows);
                        break;
                    case DeviceWork::Kind::Finish:
                        finishExpansion();
                        flushPendingRaw(true);
                        while (groupPiece())
                        {
                        }
                        freeCopiedRaw(true);
                        return;
                }
                continue;
            }

            if (groupPiece())
                continue;

            if (!expanding.empty())
            {
                finishExpansion();
                continue;
            }

            Stopwatch watch;
            flushPendingRaw(!raw_pending.empty());
            device_wait_microseconds.fetch_add(watch.elapsedMicroseconds(), std::memory_order_relaxed);
        }
    }
    catch (...)
    {
        try
        {
            flushPendingRaw(true);
            freeCopiedRaw(true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        {
            std::lock_guard lock(device_error_mutex);
            device_error = std::current_exception();
        }
        device_failed.store(true, std::memory_order_release);
        work_queue->finish();
    }
}

void GroupByGPUAccumulator::appendRaw(DeviceWork && work)
{
    const size_t num_columns = numColumns();
    if (work.reader >= readers.size() || work.column_index >= num_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A buffer of column {} from reader {} on the GPU aggregation's device thread", work.column_index, work.reader);

    raw_pending.push_back(std::move(work));
}

void GroupByGPUAccumulator::flushPendingRaw(bool wait)
{
    freeCopiedRaw(false);

    size_t landed = 0;
    for (; landed < raw_pending.size(); ++landed)
    {
        DeviceWork & work = raw_pending[landed];
        if (wait)
            work.uploaded->wait();
        else if (!work.uploaded->isComplete())
            break;

        const size_t bytes = work.on_device.size();
        DeviceFixedColumnBuffer & column = readers[work.reader].device_columns[work.column_index];
        checkCuda(
            cudaMemcpyAsync(column.grow(bytes), work.on_device.data(), bytes, cudaMemcpyDeviceToDevice, StreamRegistry::get().compute),
            "Cannot append {} plain bytes to a device column",
            bytes);

        if (const auto ordinal = variableOrdinalOfOffsetsColumn(work.column_index))
        {
            const std::string_view landed_offsets = work.staged.bytes();
            if (landed_offsets.size() % sizeof(UInt64) != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of the offsets of a variable-width key", landed_offsets.size());

            std::vector<UInt64> & mirror = readers[work.reader].variable_offsets[*ordinal];
            const size_t at = mirror.size();
            mirror.resize(at + landed_offsets.size() / sizeof(UInt64));
            memcpy(mirror.data() + at, landed_offsets.data(), landed_offsets.size());
        }

        work.staged = PinnedBuffer{};
        CopiedRaw copied{.work = std::move(work), .copied = DeviceEvent{}};
        copied.copied.record(StreamRegistry::get().compute);
        raw_in_flight.push_back(std::move(copied));
    }

    raw_pending.erase(raw_pending.begin(), raw_pending.begin() + landed);
}

void GroupByGPUAccumulator::freeCopiedRaw(bool wait)
{
    if (wait)
    {
        for (const CopiedRaw & copied : raw_in_flight)
            copied.copied.wait();
        raw_in_flight.clear();
        return;
    }

    std::erase_if(raw_in_flight, [](const CopiedRaw & copied) { return copied.copied.isComplete(); });
}

void GroupByGPUAccumulator::startExpansion(std::vector<DeviceWork> && works)
{
    expansion_watch.restart();

    const size_t num_columns = numColumns();

    std::vector<CompressedPiece> pieces;
    pieces.reserve(works.size());
    for (DeviceWork & work : works)
    {
        if (work.reader >= readers.size() || work.column_index >= num_columns)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "A buffer of column {} from reader {} on the GPU aggregation's device thread", work.column_index, work.reader);

        pieces.push_back({
            .device_compressed = work.on_device.data(),
            .compressed_bytes = work.on_device.size(),
            .blocks = work.blocks,
            .uploaded = &*work.uploaded,
        });
    }

    decompressor.launch(works.front().codec, pieces);
    expanding = std::move(works);
}

void GroupByGPUAccumulator::finishExpansion()
{
    if (expanding.empty())
        return;

    const char * expanded = decompressor.wait();

    size_t compressed_bytes = 0;
    size_t at = 0;
    for (DeviceWork & work : expanding)
    {
        const size_t bytes = decompressedBytesOf(work.blocks);
        DeviceFixedColumnBuffer & column = readers[work.reader].device_columns[work.column_index];
        checkCuda(
            cudaMemcpyAsync(column.grow(bytes), expanded + at, bytes, cudaMemcpyDeviceToDevice, StreamRegistry::get().compute),
            "Cannot append {} expanded bytes to a device column",
            bytes);
        at += bytes;
        compressed_bytes += work.on_device.size();
    }

    decompressor.release();
    expanding.clear();

    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, expansion_watch.elapsedMicroseconds());
}

bool GroupByGPUAccumulator::hasPieceToGroup() const
{
    for (const Reader & reader : readers)
    {
        const size_t ready = rowsOnDeviceInEveryColumn(reader);
        if (ready > reader.grouped_rows && (reader.grouped_rows > 0 || ready - reader.grouped_rows >= batch_rows))
            return true;
    }
    return false;
}

bool GroupByGPUAccumulator::groupPiece()
{
    for (Reader & reader : readers)
    {
        const size_t ready = rowsOnDeviceInEveryColumn(reader);
        if (ready <= reader.grouped_rows || (reader.grouped_rows == 0 && ready - reader.grouped_rows < batch_rows))
            continue;

        groupRowsOnDevice(reader, std::min(ready, reader.grouped_rows + group_piece_rows));
        if (reader.grouped_rows == ready)
            dropGroupedRows(reader);
        return true;
    }
    return false;
}

void GroupByGPUAccumulator::finishPartOnDevice(size_t reader_index, size_t num_rows)
{
    if (reader_index >= readers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The end of a part from reader {} on the GPU aggregation's device thread", reader_index);

    finishExpansion();
    flushPendingRaw(true);

    Reader & reader = readers[reader_index];

    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (i < group_keys.size() && group_keys[i] == GPUElementType::String)
            continue;

        const DeviceFixedColumnBuffer & column = reader.device_columns[i];
        if (reader.dropped_rows * column.elementSize() + column.bytes() != num_rows * column.elementSize())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of a part came to {} rows and {} bytes on the device, where the part has {} rows of {} bytes",
                i,
                reader.dropped_rows,
                column.bytes(),
                num_rows,
                column.elementSize());
    }

    for (size_t j = 0; j < variable_key_indices.size(); ++j)
    {
        const std::vector<UInt64> & offsets = reader.variable_offsets[j];
        const size_t chars_bytes = reader.device_columns[variable_key_indices[j]].bytes();
        if (offsets.empty() || reader.dropped_rows + offsets.size() - 1 != num_rows || offsets.back() != chars_bytes)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Variable-width key {} of a part came to {} rows of {} offsets and {} bytes on the device, where the part has {} rows",
                variable_key_indices[j],
                reader.dropped_rows,
                offsets.size(),
                chars_bytes,
                num_rows);
    }

    groupRowsOnDevice(reader, num_rows - reader.dropped_rows);

    for (auto & column : reader.device_columns)
        column.clear();
    for (auto & offsets : reader.variable_offsets)
        offsets.clear();
    reader.grouped_rows = 0;
    reader.dropped_rows = 0;
}

size_t GroupByGPUAccumulator::rowsOnDeviceInEveryColumn(const Reader & reader) const
{
    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();

    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (i < group_keys.size() && group_keys[i] == GPUElementType::String)
            continue;
        rows = std::min(rows, reader.device_columns[i].rows());
    }

    for (size_t j = 0; j < variable_key_indices.size(); ++j)
        rows = std::min(rows, rowsCoveredBy(reader.variable_offsets[j], reader.device_columns[variable_key_indices[j]].bytes()));

    return rows;
}

void GroupByGPUAccumulator::groupRowsOnDevice(Reader & reader, size_t up_to)
{
    const size_t num_rows = up_to - reader.grouped_rows;
    if (num_rows == 0)
        return;

    std::vector<DeviceColumnView> keys;
    std::vector<DeviceFixedColumn> value_columns;
    std::vector<DeviceFixedColumn> filter_columns;
    keys.reserve(group_keys.size());
    value_columns.reserve(values.size());
    filter_columns.reserve(filter_element_types.size());

    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();
    size_t next_variable = 0;
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (i < group_keys.size() && group_keys[i] == GPUElementType::String)
        {
            const std::vector<UInt64> & host_offsets = reader.variable_offsets[next_variable];
            const UInt64 first = host_offsets[reader.grouped_rows];
            const UInt64 last = host_offsets[up_to];

            const DeviceFixedColumn offsets = reader.device_columns[first_offsets_column + next_variable].fixedView();
            DeviceBuffer & rebased = reader.piece_offsets[next_variable];
            rebased.clear();
            auto * rebased_offsets = reinterpret_cast<uint64_t *>(rebased.grow((num_rows + 1) * sizeof(UInt64)));
            onDevice(
                [&]
                {
                    subtractFromOffsets(
                        reinterpret_cast<const uint64_t *>(offsets.data) + reader.grouped_rows,
                        num_rows + 1,
                        first,
                        rebased_offsets,
                        StreamRegistry::get().compute);
                },
                "Cannot view the offsets of {} rows of a variable-width key from 0",
                num_rows);

            keys.push_back(DeviceVariableColumn{
                .offsets = rebased_offsets,
                .chars = reader.device_columns[i].fixedView().data + first,
                .rows = num_rows,
                .chars_bytes = last - first,
            });
            ++next_variable;
            continue;
        }

        DeviceFixedColumn view = reader.device_columns[i].fixedView();
        view.data += reader.grouped_rows * sizeOf(view.element_type);
        view.rows = num_rows;

        if (i < group_keys.size())
            keys.push_back(view);
        else if (i < group_keys.size() + values.size())
            value_columns.push_back(view);
        else
            filter_columns.push_back(view);
    }

    Stopwatch watch;

    double kernel_microseconds = 0;
    if (variable_group_by)
        groupByVariable(keys, value_columns, num_rows);
    else
        kernel_microseconds = onDevice(
            [&] { return group_by->addBatch(fixedColumnsOf(keys), value_columns, filter_columns, filter ? &*filter : nullptr); },
            "Cannot group {} rows on the device",
            num_rows);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUGroupByKernelMicroseconds, static_cast<UInt64>(kernel_microseconds));

    reader.grouped_rows = up_to;
}

void GroupByGPUAccumulator::groupByVariable(
    const std::vector<DeviceColumnView> & keys, const std::vector<DeviceFixedColumn> & value_columns, size_t num_rows)
{
    const rmm::cuda_stream_view compute = StreamRegistry::get().compute;
    const rmm::cuda_stream_view grouping = variable_group_stream->get();

    DeviceEvent filled;
    filled.record(compute);
    filled.waitOn(grouping);

    onDevice([&] { variable_group_by->addBatch(keys, value_columns); }, "Cannot group {} rows by variable-width keys on the device", num_rows);

    DeviceEvent grouped;
    grouped.record(grouping);
    grouped.waitOn(compute);
}

void GroupByGPUAccumulator::dropGroupedRows(Reader & reader)
{
    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (i < group_keys.size() && group_keys[i] == GPUElementType::String)
            continue;
        reader.device_columns[i].dropFront(reader.grouped_rows);
    }

    for (size_t j = 0; j < variable_key_indices.size(); ++j)
    {
        std::vector<UInt64> & offsets = reader.variable_offsets[j];
        const UInt64 dropped_bytes = offsets[reader.grouped_rows];

        reader.device_columns[variable_key_indices[j]].dropFront(dropped_bytes);

        DeviceFixedColumnBuffer & device_offsets = reader.device_columns[first_offsets_column + j];
        device_offsets.dropFront(reader.grouped_rows);
        onDevice(
            [&]
            {
                auto * offsets_on_device = reinterpret_cast<uint64_t *>(device_offsets.mutableData());
                subtractFromOffsets(offsets_on_device, device_offsets.rows(), dropped_bytes, offsets_on_device, StreamRegistry::get().compute);
            },
            "Cannot move the offsets of a variable-width key back by {} bytes",
            dropped_bytes);

        offsets.erase(offsets.begin(), offsets.begin() + reader.grouped_rows);
        for (UInt64 & offset : offsets)
            offset -= dropped_bytes;
    }

    reader.dropped_rows += reader.grouped_rows;
    reader.grouped_rows = 0;
}

size_t GroupByGPUAccumulator::finalize()
{
    if (compressed)
    {
        DeviceWork work;
        work.kind = DeviceWork::Kind::Finish;
        enqueue(std::move(work));
        device_thread.join();
        rethrowDeviceError();
    }
    else
    {
        sendBatchToDevice();
    }

    Stopwatch watch;
    const size_t groups = variable_group_by
        ? onDevice([&] { return variable_group_by->finalize(); }, "Cannot finalize a `GROUP BY` by variable-width keys on the device")
        : onDevice([&] { return group_by->finalize(); }, "Cannot finalize a `GROUP BY` on the device");
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    num_groups = groups;
    return groups;
}

void GroupByGPUAccumulator::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    if (variable_group_by)
    {
        copyVariableGroupsTo(key_columns, value_columns);
        return;
    }

    std::vector<HostColumnView> keys;
    keys.reserve(key_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i)
        keys.push_back(resizeForElementType(*key_columns[i], *num_groups, key_element_types[i]));

    std::vector<HostColumnView> groups;
    groups.reserve(value_columns.size());
    for (size_t i = 0; i < value_columns.size(); ++i)
    {
        const GPUElementType left_in = values[i].aggregation == GPUAggregationKind::Sum ? values[i].result_type : values[i].element_type;
        groups.push_back(resizeForElementType(*value_columns[i], *num_groups, left_in));
    }

    if (*num_groups == 0)
        return;

    Stopwatch watch;
    onDevice([&] { group_by->copyGroupsOut(keys, groups); }, "Cannot copy {} groups back from the device", *num_groups);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

void GroupByGPUAccumulator::copyVariableGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    const size_t rows = *num_groups;

    if (key_columns.size() != group_keys.size() || value_columns.size() != values.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} value columns for the groups of a GPU aggregation over {} and {}",
            key_columns.size(),
            value_columns.size(),
            group_keys.size(),
            values.size());

    if (rows == 0)
        return;

    Stopwatch watch;

    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(group_keys.size() + values.size());
    to.reserve(group_keys.size() + values.size());

    for (size_t i = 0; i < group_keys.size(); ++i)
    {
        const DeviceColumnView key = onDevice([&] { return variable_group_by->key(i); }, "Cannot view key {} of the groups", i);
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }

    for (size_t i = 0; i < values.size(); ++i)
    {
        const DeviceFixedColumn value = onDevice([&] { return variable_group_by->value(i); }, "Cannot view value {} of the groups", i);
        if (value.rows != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of value {} of {} groups", value.rows, i, rows);
        from.push_back(value);
        to.push_back(value_columns[i].get());
    }

    copyDeviceToHost(from, to, variable_group_stream->get());

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}
}

#endif
