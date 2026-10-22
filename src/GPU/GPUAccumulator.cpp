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
    extern const Event GPUDecompressionMicroseconds;
    extern const Event GPUDecompressionBytes;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

bool isComplete(cudaEvent_t event)
{
    const cudaError_t status = cudaEventQuery(event);
    if (status == cudaErrorNotReady)
        return false;
    checkCuda(status, "Cannot ask whether the device has reached a point in its stream");
    return true;
}

}

namespace
{

/// The result of a `sum` is wider than what it sums; a `min` or a `max` is of the type it takes.
GPUElementType resultTypeFor(GPUElementType element_type, GPUAggregationKind aggregation)
{
    return aggregation == GPUAggregationKind::Sum ? sumResultTypeFor(element_type) : element_type;
}

/// What the bytes of a column are held as on the device: those of a column of values of varying width as bytes.
GPUElementType dataElementTypeOf(GPUElementType column_type)
{
    return columnKindOf(column_type) == GPUColumnKind::Variable ? GPUElementType::UInt8 : column_type;
}

/// A batch of fixed-width values holds whole values, and at most as many as the device takes at once.
size_t batchBytesFor(size_t batch_bytes, GPUElementType element_type, size_t max_batch_rows)
{
    if (columnKindOf(element_type) == GPUColumnKind::Variable)
        return std::max<size_t>(batch_bytes, 1);
    const size_t element_size = sizeOf(element_type);
    return std::clamp(batch_bytes, element_size, max_batch_rows * element_size);
}

}

GPUAccumulator::GPUAccumulator(
    const IDataType & argument_type,
    const IDataType & result_type_,
    GPUAggregationKind aggregation_,
    size_t batch_bytes_,
    std::optional<GPUCodec> codec_)
    : element_type(reducibleElementTypeOrThrow(argument_type, result_type_, aggregation_))
    , result_type(resultTypeFor(element_type, aggregation_))
    , result_data_type(result_type_.getPtr())
    , aggregation(aggregation_)
    , batch_bytes(batchBytesFor(batch_bytes_, element_type, max_batch_rows))
    , reduction(std::make_unique<CudfReduction>(element_type, result_type, aggregation))
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

    if (pipe->stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

std::span<char> GPUAccumulator::reserveRaw(size_t max_bytes)
{
    return plainPipeOrThrow().reserveRaw(max_bytes);
}

void GPUAccumulator::commitRaw(size_t bytes)
{
    plainPipeOrThrow().commitRaw(bytes);

    if (pipe->stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::addBlock(std::string_view payload, size_t decompressed_bytes)
{
    compressedPipeOrThrow().stageCompressedBlock(payload, decompressed_bytes);

    if (pipe->stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::addSizesBlock(std::string_view payload, size_t decompressed_bytes)
{
    compressedPipeOrThrow().stageCompressedSizesBlock(payload, decompressed_bytes);

    if (pipe->stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

ColumnUploadPipe & GPUAccumulator::plainPipeOrThrow()
{
    auto * plain_pipe = typeid_cast<ColumnUploadPipe *>(pipe.get());
    if (!plain_pipe)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain values for an accumulator of compressed blocks");
    return *plain_pipe;
}

CompressedUploadPipe & GPUAccumulator::compressedPipeOrThrow()
{
    auto * compressed_pipe = typeid_cast<CompressedUploadPipe *>(pipe.get());
    if (!compressed_pipe)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A compressed block for an accumulator of plain values");
    return *compressed_pipe;
}

void GPUAccumulator::reduceBatchOnDevice()
{
    if (pipe->stagedRows() == 0)
        return;

    Stopwatch watch;

    /// Of a column of values of varying width, the rows whose sizes and bytes have not both arrived wait for the next batch.
    const DeviceColumnView values = pipe->flush().view();
    if (values.rows() != 0)
        reduction->addBatch(values);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, values.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches, 1);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    pipe->reset();
}

Field GPUAccumulator::finalize()
{
    reduceBatchOnDevice();

    if (pipe->stagedBytes() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A column ended with {} bytes that make no whole value", pipe->stagedBytes());

    Stopwatch watch;
    const DeviceColumnView result = reduction->finalize();
    if (result.rows() == 0)
        return result_data_type->getDefault();

    MutableColumnPtr column = result_data_type->createColumn();
    IColumn * const to = column.get();
    copyDeviceToHost(std::span<const DeviceColumnView>(&result, 1), std::span<IColumn * const>(&to, 1), GPUResources::get().compute.get());
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    return (*column)[0];
}

namespace
{

std::vector<GPUElementType> valueTypesOrThrow(const DataTypes & value_types)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(value_types.size());
    for (const auto & type : value_types)
        element_types.push_back(columnTypeOrThrow(*type));
    return element_types;
}

std::vector<GPUGroupByAggregate> aggregatesOrThrow(size_t num_columns, std::vector<GPUGroupByAggregate> aggregates)
{
    if (aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation without aggregates");

    for (const auto & aggregate : aggregates)
    {
        if (aggregate.column != GPUGroupByAggregate::no_column && aggregate.column >= num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregate of value {} of {}", aggregate.column, num_columns);
    }
    return aggregates;
}

std::vector<GPUElementType> withDerived(std::vector<GPUElementType> values, size_t num_derived)
{
    values.insert(values.end(), num_derived, GPUElementType::Float64);
    return values;
}

size_t rowBytesOf(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUElementType> & values,
    const std::vector<GPUElementType> & filter_element_types)
{
    size_t bytes = 0;
    for (const GPUElementType key_element_type : key_element_types)
        bytes += sizeOf(key_element_type);
    for (const GPUElementType value : values)
        bytes += sizeOf(dataElementTypeOf(value));
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
        const GPUElementType column_type = uploadTypeOrThrow(*type);
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
        element_types.push_back(dataElementTypeOf(key));
    return element_types;
}

/// The keys and then the values that are of varying width, by their place among the columns.
std::vector<size_t> variableColumnIndicesOf(const std::vector<GPUElementType> & keys, const std::vector<GPUElementType> & values)
{
    std::vector<size_t> indices;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (columnKindOf(keys[i]) == GPUColumnKind::Variable)
            indices.push_back(i);
    }
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (columnKindOf(values[i]) == GPUColumnKind::Variable)
            indices.push_back(keys.size() + i);
    }
    return indices;
}

std::vector<DeviceFixedColumnBuffer> deviceColumnsFor(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUElementType> & values,
    const std::vector<GPUElementType> & filter_element_types,
    size_t num_variable_columns,
    bool compressed)
{
    std::vector<DeviceFixedColumnBuffer> columns;
    if (!compressed)
        return columns;

    columns.reserve(key_element_types.size() + values.size() + filter_element_types.size() + num_variable_columns);
    for (const GPUElementType key_element_type : key_element_types)
        columns.emplace_back(key_element_type);
    for (const GPUElementType value : values)
        columns.emplace_back(dataElementTypeOf(value));
    for (const GPUElementType filter_element_type : filter_element_types)
        columns.emplace_back(filter_element_type);
    for (size_t i = 0; i < num_variable_columns; ++i)
        columns.emplace_back(GPUElementType::UInt64);
    return columns;
}

size_t rowsCoveredBy(const std::vector<UInt64> & offsets, size_t chars_bytes)
{
    if (offsets.empty())
        return 0;
    return static_cast<size_t>(std::upper_bound(offsets.begin() + 1, offsets.end(), chars_bytes) - (offsets.begin() + 1));
}

/// What a reader stages of one column before it hands the piece over: the uncompressed bytes of the blocks staged, or the
/// raw bytes. Every piece in the work queue is on the device already, so this times the queue's length bounds the device
/// memory of the queue, and the pinned memory of the staging.
constexpr size_t compressed_stage_bytes = 32UL * 1024 * 1024;

/// The uncompressed bytes the device thread expands in one batched call, and so the size of its buffer for them.
constexpr size_t max_expansion_bytes = 256UL * 1024 * 1024;

/// The fewest rows a reader groups at once, below which the calls into cuDF cost more than the rows.
constexpr size_t min_reader_batch_rows = 1UL << 20;

/// What the pieces in the work queue may take of the device's memory, together, see `DeviceStagingBudget`.
constexpr size_t max_staged_device_bytes = 2UL * 1024 * 1024 * 1024;

constexpr size_t queued_buffers_per_column = 2;

/// The batches handed over to the grouping thread that wait for it, besides the one it groups: one, for the device
/// thread to go on while it groups, as each holds its rows on the device besides those the readers gather.
constexpr size_t queued_grouping_batches = 1;

}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    size_t batch_bytes,
    bool compressed_,
    size_t num_readers_,
    const DataTypes & filter_types,
    std::optional<GPUFilterProgram> filter_,
    std::vector<GPUDerivedValue> derived_)
    : group_keys(groupByKeysOrThrow(key_types))
    , key_element_types(keyElementTypesOf(group_keys))
    , values(valueTypesOrThrow(value_types))
    , derived(std::move(derived_))
    , aggregates(aggregatesOrThrow(values.size() + derived.size(), std::move(aggregates_)))
    , variable_column_indices(variableColumnIndicesOf(group_keys, values))
    , filter_element_types(filterElementTypesOrThrow(filter_types))
    , filter(std::move(filter_))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_types, values, filter_element_types), size_t{1}, max_batch_rows))
    , reader_batch_rows(std::min(batch_rows, std::max(batch_rows / std::max<size_t>(num_readers_, 1), min_reader_batch_rows)))
    , compressed(compressed_)
    , key_pipes(pipesFor(key_types, batch_rows, compressed))
    , value_pipes(pipesFor(value_types, batch_rows, compressed))
    , staging_budget(max_staged_device_bytes)
{
    group_stream = createStream();
    group_by = std::make_unique<CudfGroupBy>(
        group_keys, withDerived(values, derived.size()), derived, aggregates, cuda::stream_ref{group_stream.get()});

    for (const auto & value : derived)
    {
        for (uint32_t i = 0; i < value.num_columns; ++i)
        {
            if (value.columns[i] >= values.size() || values[value.columns[i]] != GPUElementType::Float64)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A value the GPU derives from value {} of {}", value.columns[i], values.size());
        }
    }

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
        if (!derived.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over plain columns of values the device derives");
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
        reader.device_columns = deviceColumnsFor(key_element_types, values, filter_element_types, variable_column_indices.size(), compressed);
        reader.variable_offsets.resize(variable_column_indices.size());
    }

    work_queue = std::make_unique<ConcurrentBoundedQueue<DeviceWork>>(queued_buffers_per_column * (num_columns + 1) * num_readers_);
    grouping_queue = std::make_unique<ConcurrentBoundedQueue<GroupingBatch>>(queued_grouping_batches);

    grouping_thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup()]
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_GROUP_BY);
        runGroupingThread();
    });

    try
    {
        device_thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup()]
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_EXPANSION);
            runDeviceThread();
            /// After the last batch, or after an error, which has already stopped the grouping thread.
            grouping_queue->finish();
        });
    }
    catch (...)
    {
        /// No destructor runs for an object whose constructor throws.
        grouping_queue->finish();
        grouping_thread.join();
        throw;
    }
}

GroupByGPUAccumulator::~GroupByGPUAccumulator()
{
    if (!device_thread.joinable() && !grouping_thread.joinable())
        return;

    try
    {
        work_queue->finish();
        staging_budget.cancel();
        /// Lets the device thread through, should it wait for room for a batch.
        grouping_queue->clearAndFinish();
        if (device_thread.joinable())
            device_thread.join();
        if (grouping_thread.joinable())
            grouping_thread.join();
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
    for (const auto * pipes : {&key_pipes, &value_pipes})
    {
        for (const auto & pipe : *pipes)
        {
            if (columnKindOf(pipe.type()) == GPUColumnKind::Variable)
                bytes += pipe.stagedBytes();
        }
    }
    return bytes;
}

bool GroupByGPUAccumulator::isVariableColumn(size_t column_index) const
{
    return std::find(variable_column_indices.begin(), variable_column_indices.end(), column_index) != variable_column_indices.end();
}

size_t GroupByGPUAccumulator::variableOffsetsColumnOf(size_t column_index) const
{
    const auto it = std::find(variable_column_indices.begin(), variable_column_indices.end(), column_index);
    if (it == variable_column_indices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation is not of varying width", column_index);

    return group_keys.size() + values.size() + filter_element_types.size() + static_cast<size_t>(it - variable_column_indices.begin());
}

void GroupByGPUAccumulator::addVariableOffsets(size_t reader_index, size_t column_index, std::span<const UInt64> offsets)
{
    Reader & reader = readerOrThrow(reader_index);
    rethrowDeviceError();

    const size_t offsets_column_index = variableOffsetsColumnOf(column_index);
    StagedColumn & column = reader.staging[offsets_column_index];

    const size_t bytes = offsets.size_bytes();
    if (!column.staged.empty() && column.staged.size() + bytes > compressed_stage_bytes)
        handOff(reader_index, offsets_column_index);

    column.raw = true;
    appendHostBytes(column.staged, {reinterpret_cast<const char *>(offsets.data()), bytes});
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

    std::vector<DeviceColumnView> value_columns;
    value_columns.reserve(value_pipes.size());
    for (auto & pipe : value_pipes)
        value_columns.push_back(pipe.flush().view());

    groupBatch(keys, value_columns, {});

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

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
    size_t reader_index,
    size_t column_index,
    GPUCodec codec,
    std::string_view payload,
    size_t decompressed_bytes,
    size_t expected_bytes,
    size_t skipped_bytes,
    std::optional<size_t> kept_bytes)
{
    Reader & reader = readerOrThrow(reader_index);

    if (column_index >= reader.staging.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation over {} columns", column_index, reader.staging.size());

    rethrowDeviceError();

    StagedColumn & column = reader.staging[column_index];

    const size_t kept = kept_bytes.value_or(decompressed_bytes - std::min(skipped_bytes, decompressed_bytes));
    if (skipped_bytes + kept > decompressed_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept, skipped_bytes, decompressed_bytes);

    /// A piece skips bytes before its first block only.
    if (column.raw || column.cut || (!column.blocks.empty() && skipped_bytes != 0)
        || (!column.blocks.empty() && (column.decompressed_bytes + decompressed_bytes > compressed_stage_bytes || column.codec != codec)))
        handOff(reader_index, column_index);

    if (column.blocks.empty())
        column.skipped_bytes = skipped_bytes;
    column.kept_bytes += kept;
    column.cut = skipped_bytes + kept != decompressed_bytes;

    if (column.blocks.empty())
        column.staged.reserve(std::min(compressed_stage_bytes, std::max(expected_bytes, payload.size())));

    column.codec = codec;
    stageCompressedBlock(column.staged, column.blocks, codec, payload, decompressed_bytes);
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
        std::min({max_bytes, compressed_stage_bytes - column.staged.size(), column.staged.capacity() - column.staged.size()})};
}

void GroupByGPUAccumulator::commitRawBytes(size_t reader_index, size_t column_index, size_t bytes)
{
    StagedColumn & column = readerOrThrow(reader_index).staging[column_index];
    growBy(column.staged, bytes);
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
    work.skipped_bytes = column.skipped_bytes;
    work.kept_bytes = column.kept_bytes;

    Stopwatch watch;
    try
    {
        work.ticket = DeviceStagingTicket(staging_budget, work.staged.size());
    }
    catch (...)
    {
        /// The budget is cancelled when the device thread fails, and its error is the one to report.
        rethrowDeviceError();
        throw;
    }
    readers_wait_microseconds.fetch_add(watch.elapsedMicroseconds(), std::memory_order_relaxed);

    work.on_device = DeviceBuffer(GPUResources::get().upload.get());
    work.on_device.append(bytesOf(work.staged));
    work.uploaded = createEvent();
    checkCuda(cudaEventRecord(work.uploaded.get(), GPUResources::get().upload.get()), "Cannot mark a point in the upload stream");

    column.staged = PinnedBuffer{};
    column.blocks = {};
    column.codec.reset();
    column.raw = false;
    column.decompressed_bytes = 0;
    column.skipped_bytes = 0;
    column.kept_bytes = 0;
    column.cut = false;

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

void DeviceStagingBudget::acquire(size_t bytes)
{
    std::unique_lock lock(mutex);
    room.wait(lock, [&] { return cancelled || used == 0 || used + bytes <= limit; });
    if (cancelled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation's reader waits for device memory after the aggregation stopped");
    used += bytes;
}

void DeviceStagingBudget::release(size_t bytes) noexcept
{
    {
        std::lock_guard lock(mutex);
        used -= std::min(used, bytes);
    }
    room.notify_all();
}

void DeviceStagingBudget::cancel() noexcept
{
    {
        std::lock_guard lock(mutex);
        cancelled = true;
    }
    room.notify_all();
}

void GroupByGPUAccumulator::rethrowDeviceError()
{
    if (!device_failed.load(std::memory_order_acquire))
        return;

    std::lock_guard lock(device_error_mutex);
    std::rethrow_exception(device_error);
}

void GroupByGPUAccumulator::fail(std::exception_ptr error) noexcept
{
    {
        std::lock_guard lock(device_error_mutex);
        if (!device_error)
            device_error = std::move(error);
    }
    device_failed.store(true, std::memory_order_release);
    work_queue->finish();
    grouping_queue->clearAndFinish();
    staging_budget.cancel();
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

            size_t blocks_bytes = 0;
            while (taken)
            {
                const size_t work_bytes = work.kind == DeviceWork::Kind::Blocks ? decompressedBytesOf(work.blocks) : 0;
                if (work.kind != DeviceWork::Kind::Blocks || (!blocks.empty() && work.codec != blocks.front().codec)
                    || (!blocks.empty() && blocks_bytes + work_bytes > max_expansion_bytes))
                {
                    other = std::move(work);
                    break;
                }
                blocks_bytes += work_bytes;
                blocks.push_back(std::move(work));
                if (blocks_bytes >= max_expansion_bytes)
                    break;
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
                /// Between pieces as well, so that while the readers keep the queue full the rows already in every column
                /// are grouped and dropped, and do not pile up on the device until it runs dry.
                groupPiece();
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
        fail(std::current_exception());
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
            checkCuda(cudaEventSynchronize(work.uploaded.get()), "Cannot wait for an upload to the device");
        else if (!isComplete(work.uploaded.get()))
            break;

        const size_t bytes = work.on_device.size();
        DeviceFixedColumnBuffer & column = readers[work.reader].device_columns[work.column_index];
        checkCuda(
            cudaMemcpyAsync(column.grow(bytes), work.on_device.data(), bytes, cudaMemcpyDeviceToDevice, GPUResources::get().compute.get()),
            "Cannot append plain bytes to a device column");

        if (const auto ordinal = variableOrdinalOfOffsetsColumn(work.column_index))
        {
            const std::string_view landed_offsets = bytesOf(work.staged);
            if (landed_offsets.size() % sizeof(UInt64) != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of the offsets of a variable-width key", landed_offsets.size());

            std::vector<UInt64> & mirror = readers[work.reader].variable_offsets[*ordinal];
            const size_t at = mirror.size();
            mirror.resize(at + landed_offsets.size() / sizeof(UInt64));
            memcpy(mirror.data() + at, landed_offsets.data(), landed_offsets.size());
        }

        work.staged = PinnedBuffer{};
        CopiedRaw copied{.work = std::move(work), .copied = createEvent()};
        checkCuda(cudaEventRecord(copied.copied.get(), GPUResources::get().compute.get()), "Cannot mark a point in the compute stream");
        raw_in_flight.push_back(std::move(copied));
    }

    raw_pending.erase(raw_pending.begin(), raw_pending.begin() + landed);
}

void GroupByGPUAccumulator::freeCopiedRaw(bool wait)
{
    if (wait)
    {
        for (const CopiedRaw & copied : raw_in_flight)
            checkCuda(cudaEventSynchronize(copied.copied.get()), "Cannot wait for plain bytes to be copied into a device column");
        raw_in_flight.clear();
        return;
    }

    std::erase_if(raw_in_flight, [](const CopiedRaw & copied) { return isComplete(copied.copied.get()); });
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
            .uploaded = work.uploaded.get(),
        });
    }

    size_t expanded_bytes = 0;
    for (const DeviceWork & work : works)
        expanded_bytes += decompressedBytesOf(work.blocks);
    expanded.clear();
    decompressor.launch(works.front().codec, pieces, expanded.grow(expanded_bytes));
    expanding = std::move(works);
}

void GroupByGPUAccumulator::finishExpansion()
{
    if (expanding.empty())
        return;

    decompressor.wait();

    size_t compressed_bytes = 0;
    size_t at = 0;
    for (DeviceWork & work : expanding)
    {
        const size_t bytes = decompressedBytesOf(work.blocks);
        DeviceFixedColumnBuffer & column = readers[work.reader].device_columns[work.column_index];
        if (work.kept_bytes != 0)
            checkCuda(
                cudaMemcpyAsync(
                    column.grow(work.kept_bytes),
                    expanded.data() + at + work.skipped_bytes,
                    work.kept_bytes,
                    cudaMemcpyDeviceToDevice,
                    GPUResources::get().compute.get()),
                "Cannot append expanded bytes to a device column");
        at += bytes;
        compressed_bytes += work.on_device.size();
    }

    expanding.clear();

    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, expansion_watch.elapsedMicroseconds());
}

bool GroupByGPUAccumulator::hasPieceToGroup() const
{
    /// Of columns of varying width, the rows of a part are grouped at its end, see `sendRowsToGrouping`.
    if (!variable_column_indices.empty())
        return false;

    for (const Reader & reader : readers)
    {
        if (rowsOnDeviceInEveryColumn(reader) >= reader_batch_rows)
            return true;
    }
    return false;
}

bool GroupByGPUAccumulator::groupPiece()
{
    if (!variable_column_indices.empty())
        return false;

    for (Reader & reader : readers)
    {
        const size_t ready = rowsOnDeviceInEveryColumn(reader);
        if (ready < reader_batch_rows)
            continue;

        /// At once, so that a reader keeps on the device only what is not handed over yet.
        sendRowsToGrouping(reader, ready);
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
        if (isVariableColumn(i))
            continue;

        const DeviceFixedColumnBuffer & column = reader.device_columns[i];
        if (reader.sent_rows * column.elementSize() + column.bytes() != num_rows * column.elementSize())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of a part came to {} rows grouped and {} bytes on the device, where the part has {} rows of {} bytes",
                i,
                reader.sent_rows,
                column.bytes(),
                num_rows,
                column.elementSize());
    }

    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const std::vector<UInt64> & offsets = reader.variable_offsets[j];
        const size_t chars_bytes = reader.device_columns[variable_column_indices[j]].bytes();
        if (offsets.empty() || reader.sent_rows + offsets.size() - 1 != num_rows || offsets.back() != chars_bytes)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of varying width of a part came to {} rows grouped, {} offsets and {} bytes on the device, where the part has {} rows",
                variable_column_indices[j],
                reader.sent_rows,
                offsets.size(),
                chars_bytes,
                num_rows);
    }

    sendRowsToGrouping(reader, num_rows - reader.sent_rows);

    for (auto & column : reader.device_columns)
        column.clear();
    for (auto & offsets : reader.variable_offsets)
        offsets.clear();
    reader.sent_rows = 0;
}

size_t GroupByGPUAccumulator::rowsOnDeviceInEveryColumn(const Reader & reader) const
{
    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();

    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (isVariableColumn(i))
            continue;
        rows = std::min(rows, reader.device_columns[i].rows());
    }

    for (size_t j = 0; j < variable_column_indices.size(); ++j)
        rows = std::min(rows, rowsCoveredBy(reader.variable_offsets[j], reader.device_columns[variable_column_indices[j]].bytes()));

    return rows;
}

void GroupByGPUAccumulator::sendRowsToGrouping(Reader & reader, size_t num_rows)
{
    if (num_rows == 0)
        return;

    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();

    GroupingBatch batch;
    batch.rows = num_rows;
    batch.columns.reserve(numColumns());
    size_t next_variable = 0;
    for (size_t i = 0; i < numColumns(); ++i)
    {
        DeviceFixedColumnBuffer & column = reader.device_columns[i];

        /// The offsets of the rows of varying width still to arrive count from the part's start, so only the end of the
        /// part hands them over, with all of its rows.
        size_t bytes = num_rows * column.elementSize();
        if (i >= first_offsets_column)
            bytes = (num_rows + 1) * sizeof(UInt64);
        else if (isVariableColumn(i))
            bytes = reader.variable_offsets[next_variable++][num_rows];

        if ((i >= first_offsets_column || isVariableColumn(i)) && (reader.sent_rows != 0 || bytes != column.bytes()))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} rows of column {} of varying width of a part are grouped, of which {} were before, in {} bytes of {}",
                num_rows,
                i,
                reader.sent_rows,
                bytes,
                column.bytes());

        batch.columns.push_back(column.takeFront(bytes));
    }

    batch.filled = createEvent();
    checkCuda(cudaEventRecord(batch.filled.get(), GPUResources::get().compute.get()), "Cannot mark a point in the compute stream");

    if (!grouping_queue->push(std::move(batch)))
    {
        rethrowDeviceError();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The GPU aggregation's grouping thread stopped taking rows without an error");
    }

    reader.sent_rows += num_rows;
}

void GroupByGPUAccumulator::runGroupingThread()
{
    try
    {
        GroupingBatch batch;
        while (grouping_queue->pop(batch))
        {
            groupOnDevice(batch);
            /// Freed in the order of the grouping stream, after the groups are made of them.
            batch = {};
        }
    }
    catch (...)
    {
        fail(std::current_exception());
    }
}

void GroupByGPUAccumulator::groupOnDevice(GroupingBatch & batch)
{
    const cuda::stream_ref grouping = cuda::stream_ref{group_stream.get()};
    checkCuda(cudaStreamWaitEvent(grouping.get(), batch.filled.get(), 0), "Cannot make the grouping stream wait for the compute stream");
    for (DeviceBuffer & column : batch.columns)
        column.setStream(grouping);

    std::vector<DeviceColumnView> keys;
    std::vector<DeviceColumnView> value_columns;
    std::vector<DeviceFixedColumn> filter_columns;
    keys.reserve(group_keys.size());
    value_columns.reserve(values.size());
    filter_columns.reserve(filter_element_types.size());

    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();
    size_t next_variable = 0;
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        const DeviceBuffer & column = batch.columns[i];
        if (isVariableColumn(i))
        {
            const DeviceVariableColumn view{
                .offsets = reinterpret_cast<const uint64_t *>(batch.columns[first_offsets_column + next_variable].data()),
                .chars = column.data(),
                .rows = batch.rows,
                .chars_bytes = column.size(),
            };
            (i < group_keys.size() ? keys : value_columns).push_back(view);
            ++next_variable;
            continue;
        }

        const GPUElementType element_type = i < group_keys.size()
            ? key_element_types[i]
            : (i < group_keys.size() + values.size() ? dataElementTypeOf(values[i - group_keys.size()])
                                                      : filter_element_types[i - group_keys.size() - values.size()]);
        const DeviceFixedColumn view{.element_type = element_type, .data = column.data(), .rows = batch.rows};

        if (i < group_keys.size())
            keys.push_back(view);
        else if (i < group_keys.size() + values.size())
            value_columns.push_back(view);
        else
            filter_columns.push_back(view);
    }

    Stopwatch watch;

    group_by->addBatch(keys, value_columns, filter_columns, filter ? &*filter : nullptr);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

void GroupByGPUAccumulator::groupBatch(
    const std::vector<DeviceColumnView> & keys,
    const std::vector<DeviceColumnView> & value_columns,
    const std::vector<DeviceFixedColumn> & filter_columns)
{
    const cuda::stream_ref compute = GPUResources::get().compute;
    const cuda::stream_ref grouping = cuda::stream_ref{group_stream.get()};

    const EventPtr filled = createEvent();
    checkCuda(cudaEventRecord(filled.get(), compute.get()), "Cannot mark a point in the compute stream");
    checkCuda(cudaStreamWaitEvent(grouping.get(), filled.get(), 0), "Cannot make the grouping stream wait for the compute stream");

    group_by->addBatch(keys, value_columns, filter_columns, filter ? &*filter : nullptr);

    const EventPtr grouped = createEvent();
    checkCuda(cudaEventRecord(grouped.get(), grouping.get()), "Cannot mark a point in the grouping stream");
    checkCuda(cudaStreamWaitEvent(compute.get(), grouped.get(), 0), "Cannot make the compute stream wait for the grouping stream");
}

size_t GroupByGPUAccumulator::finalize()
{
    if (compressed)
    {
        DeviceWork work;
        work.kind = DeviceWork::Kind::Finish;
        enqueue(std::move(work));
        device_thread.join();
        grouping_thread.join();
        rethrowDeviceError();
    }
    else
    {
        sendBatchToDevice();
    }

    Stopwatch watch;
    const size_t groups = group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    num_groups = groups;
    return groups;
}

size_t GroupByGPUAccumulator::filterGroups(const GPUFilterProgram & program, const std::vector<uint32_t> & columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were filtered before it was finalized");

    Stopwatch watch;
    num_groups = group_by->filterGroups(program, columns);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GroupByGPUAccumulator::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    const size_t rows = *num_groups;

    if (key_columns.size() != group_keys.size() || value_columns.size() != aggregates.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} aggregate columns for the groups of a GPU aggregation of {} keys and {} aggregates",
            key_columns.size(),
            value_columns.size(),
            group_keys.size(),
            aggregates.size());

    if (rows == 0)
        return;

    Stopwatch watch;

    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(group_keys.size() + aggregates.size());
    to.reserve(group_keys.size() + aggregates.size());

    for (size_t i = 0; i < group_keys.size(); ++i)
    {
        const DeviceColumnView key = group_by->key(i);
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const DeviceColumnView value = group_by->value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of aggregate {} of {} groups", value.rows(), i, rows);
        from.push_back(value);
        to.push_back(value_columns[i].get());
    }

    copyDeviceToHost(from, to, cuda::stream_ref{group_stream.get()});

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}
}

#endif
