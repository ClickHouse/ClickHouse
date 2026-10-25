#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>
#include <GPU/OffsetsKernels.cuh>

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
    extern const Event GPUReadersWaitMicroseconds;
    extern const Event GPUDeviceWaitMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
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

bool GPUAccumulator::takesOnDevice(const DeviceColumnView & column) const
{
    return column.kind == GPUColumnKind::Fixed && column.fixed.element_type == element_type;
}

void GPUAccumulator::addOnDevice(const GPUDeviceColumns & columns, size_t position)
{
    const DeviceColumnView & values = columns.views().at(position);
    if (!takesOnDevice(values))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A device column of another type than a GPU aggregation reduces");
    if (values.rows() == 0)
        return;

    Stopwatch watch;

    /// The reduction runs on the compute stream and waits for its result, so the columns are no longer read once it returns.
    columns.waitIn(GPUResources::get().compute);
    reduction->addBatch(values);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, values.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches, 1);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
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

void GPUAccumulator::addBlock(UInt8 method, std::string_view payload, size_t decompressed_bytes)
{
    compressedPipeOrThrow().stageCompressedBlock(method, payload, decompressed_bytes);

    if (pipe->stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::addSizesBlock(UInt8 method, std::string_view payload, size_t decompressed_bytes)
{
    compressedPipeOrThrow().stageCompressedSizesBlock(method, payload, decompressed_bytes);

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

/// Of rows collected rather than grouped, the values are carried by their bytes, see `collectElementTypeOf`.
std::vector<GPUElementType> valueTypesOrThrow(const DataTypes & value_types, bool collect)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(value_types.size());
    for (const auto & type : value_types)
    {
        if (!collect)
        {
            element_types.push_back(columnTypeOrThrow(*type));
            continue;
        }
        const auto element_type = collectElementTypeOf(*type);
        if (!element_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} collected on the GPU", type->getName());
        element_types.push_back(*element_type);
    }
    return element_types;
}

std::vector<GPUGroupByAggregate> aggregatesOrThrow(size_t num_columns, std::vector<GPUGroupByAggregate> aggregates, bool collect)
{
    /// Without keys, the rows are collected, not aggregated.
    if (collect != aggregates.empty())
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

std::vector<bool> floatKeysOf(const DataTypes & key_types)
{
    std::vector<bool> float_keys;
    float_keys.reserve(key_types.size());
    for (const auto & type : key_types)
        float_keys.push_back(isFloatKey(*type));
    return float_keys;
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
    bool compressed,
    cuda::stream_ref stream)
{
    std::vector<DeviceFixedColumnBuffer> columns;
    if (!compressed)
        return columns;

    columns.reserve(key_element_types.size() + values.size() + filter_element_types.size() + num_variable_columns);
    for (const GPUElementType key_element_type : key_element_types)
        columns.emplace_back(key_element_type, stream);
    for (const GPUElementType value : values)
        columns.emplace_back(dataElementTypeOf(value), stream);
    for (const GPUElementType filter_element_type : filter_element_types)
        columns.emplace_back(filter_element_type, stream);
    for (size_t i = 0; i < num_variable_columns; ++i)
        columns.emplace_back(GPUElementType::UInt64, stream);
    return columns;
}


/// What a reader stages of one column before it hands the piece over: the uncompressed bytes of the blocks staged, or the
/// raw bytes. Every piece in the work queue is on the device already, so this times the queue's length bounds the device
/// memory of the queue, and the pinned memory of the staging.
constexpr size_t compressed_stage_bytes = 32UL * 1024 * 1024;

/// The uncompressed bytes the device thread expands in one batched call, and so the size of its buffer for them.
constexpr size_t max_expansion_bytes = 256UL * 1024 * 1024;

/// Of rows collected rather than grouped: how many are kept on the device before they are handed over.
constexpr size_t collect_flush_rows = 1UL << 20;

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
    std::vector<GPUDerivedValue> derived_,
    std::function<void(MutableColumns)> on_collected_,
    std::vector<std::optional<uint32_t>> filter_aliases_,
    std::function<void(std::shared_ptr<const GPUDeviceColumns>)> on_collected_on_device_,
    std::vector<MembershipTest> memberships_)
    : group_keys(groupByKeysOrThrow(key_types))
    , float_keys(floatKeysOf(key_types))
    , key_element_types(keyElementTypesOf(group_keys))
    , values(valueTypesOrThrow(value_types, key_types.empty()))
    , value_data_types(value_types)
    , on_collected(std::move(on_collected_))
    , on_collected_on_device(std::move(on_collected_on_device_))
    , derived(std::move(derived_))
    , aggregates(aggregatesOrThrow(values.size() + derived.size(), std::move(aggregates_), key_types.empty()))
    , variable_column_indices(variableColumnIndicesOf(group_keys, values))
    , filter_element_types(filterElementTypesOrThrow(filter_types))
    , filter(std::move(filter_))
    , filter_aliases(std::move(filter_aliases_))
    , memberships(std::move(memberships_))
    , key_sets(memberships.size())
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

    if ((on_collected || on_collected_on_device) && !group_keys.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with keys that hands rows over as collected");

    for (const MembershipTest & membership : memberships)
    {
        if (!membership.keys || membership.key_column >= filter_element_types.size()
            || !isInteger(filter_element_types[membership.key_column]) || membership.keys->keyElementType() != filter_element_types[membership.key_column])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A runtime filter of a GPU aggregation over other than an integer column of the keys' type");
    }
    if (!memberships.empty() && !compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A runtime filter of a GPU aggregation over plain columns");

    if (filter && filter->num_columns != filter_element_types.size() + memberships.size())
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

    compute_stream = createStream();
    decompression_stream = createStream();
    for (size_t i = 0; i < 2; ++i)
        slots.push_back(std::make_unique<ExpansionSlot>(cuda::stream_ref{decompression_stream.get()}, compute()));

    const size_t num_columns = numColumns();
    readers.reserve(num_readers_);
    for (size_t i = 0; i < num_readers_; ++i)
    {
        Reader & reader = readers.emplace_back();
        reader.upload = createStream();
        reader.staging.resize(num_columns);
        reader.device_columns
            = deviceColumnsFor(key_element_types, values, filter_element_types, variable_column_indices.size(), compressed, compute());
        reader.variable_base.assign(variable_column_indices.size(), 0);
        reader.variable_checkpoints.resize(variable_column_indices.size());
        if (!variable_column_indices.empty())
            growBy(reader.checkpoint_ring, checkpoint_ring_slots * sizeof(UInt64));
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
    stop_waiting.store(true, std::memory_order_relaxed);
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

bool GroupByGPUAccumulator::takesOnDevice(
    const GPUDeviceColumns & columns, const std::vector<size_t> & key_positions, const std::vector<size_t> & value_positions) const
{
    if (compressed || key_positions.size() != group_keys.size() || value_positions.size() != values.size())
        return false;

    const auto takes = [&](size_t position, GPUElementType expected)
    {
        if (position >= columns.views().size())
            return false;
        const DeviceColumnView & view = columns.views()[position];
        if (expected == GPUElementType::String)
            return view.kind == GPUColumnKind::Variable;
        return view.kind == GPUColumnKind::Fixed && view.fixed.element_type == expected;
    };

    for (size_t i = 0; i < group_keys.size(); ++i)
    {
        /// A float key arrives as the float it is, and is grouped as the integer of its bits.
        const GPUElementType expected = !float_keys[i] ? group_keys[i]
            : (group_keys[i] == GPUElementType::UInt32 ? GPUElementType::Float32 : GPUElementType::Float64);
        if (!takes(key_positions[i], expected))
            return false;
    }
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (!takes(value_positions[i], values[i]))
            return false;
    }
    return true;
}

void GroupByGPUAccumulator::addOnDevice(
    const GPUDeviceColumns & columns, const std::vector<size_t> & key_positions, const std::vector<size_t> & value_positions)
{
    if (!takesOnDevice(columns, key_positions, value_positions))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Device columns of other types than a GPU aggregation groups");
    if (columns.rows() == 0)
        return;

    /// The rows staged in host memory go first, as a batch of their own.
    sendBatchToDevice();

    Stopwatch watch;

    std::vector<DeviceColumnView> keys;
    keys.reserve(key_positions.size());
    for (size_t i = 0; i < key_positions.size(); ++i)
    {
        const DeviceColumnView & view = columns.views()[key_positions[i]];
        if (!float_keys[i])
            keys.push_back(view);
        else
            keys.push_back(DeviceFixedColumn{.element_type = group_keys[i], .data = view.fixed.data, .rows = view.fixed.rows});
    }
    std::vector<DeviceColumnView> value_columns;
    value_columns.reserve(value_positions.size());
    for (const size_t position : value_positions)
        value_columns.push_back(columns.views()[position]);

    columns.waitIn(GPUResources::get().compute);
    groupBatch(keys, value_columns, {});
    /// The columns are the chunk's, which the transform drops once this returns.
    checkCuda(cudaStreamSynchronize(group_stream.get()), "Cannot wait for the grouping of device columns");

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, columns.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
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

bool GroupByGPUAccumulator::isAliasedColumn(size_t column_index) const
{
    const size_t first_filter = group_keys.size() + values.size();
    return column_index >= first_filter && column_index - first_filter < filter_aliases.size() && filter_aliases[column_index - first_filter].has_value();
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
    for (size_t i = 0; i < key_pipes.size(); ++i)
    {
        const DeviceColumnView view = key_pipes[i].flush().view();
        if (!float_keys[i])
        {
            keys.push_back(view);
            continue;
        }

        /// A float key is grouped as the integer of its bits.
        keys.push_back(DeviceFixedColumn{.element_type = group_keys[i], .data = view.fixed.data, .rows = view.fixed.rows});
    }

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
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes,
    size_t expected_bytes,
    size_t skipped_bytes,
    std::optional<size_t> kept_bytes)
{
    Reader & reader = readerOrThrow(reader_index);

    const auto codec = deviceCodecOf(method, payload);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A block of compression method {:#x} for a GPU aggregation, which the device does not expand", static_cast<UInt16>(method));

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
    stageCompressedBlock(column.staged, column.blocks, column.packed, method, payload, decompressed_bytes);
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
    work.packed = std::move(column.packed);
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

    const cuda::stream_ref upload{readers[reader_index].upload.get()};
    work.on_device = DeviceBuffer(upload);
    work.on_device.append(bytesOf(work.staged));
    work.uploaded = createEvent();
    checkCuda(cudaEventRecord(work.uploaded.get(), upload.get()), "Cannot mark a point in the upload stream");

    column.staged = takeSpareStaging();
    column.blocks = {};
    column.packed = {};
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
    stop_waiting.store(true, std::memory_order_relaxed);
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

            const bool idle = in_flight.empty() && raw_pending.empty() && !carried && !hasPieceToGroup();
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
                const size_t work_bytes = work.kind == DeviceWork::Kind::Blocks ? valueBytesOf(work.blocks, work.packed) : 0;
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
                startExpansion(std::move(blocks));

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
                        finishAllExpansions();
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

            if (!in_flight.empty())
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

        /// Plain bytes of a column, or the sizes of a column of varying width expanded on the host.
        appendToDeviceColumn(readers[work.reader], work.column_index, work.on_device.data(), work.on_device.size());

        returnSpareStaging(std::move(work.staged));
        CopiedRaw copied{.work = std::move(work), .copied = createEvent()};
        checkCuda(cudaEventRecord(copied.copied.get(), compute().get()), "Cannot mark a point in the compute stream");
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
    if (in_flight.size() == slots.size())
        finishExpansion();

    ExpansionSlot * slot = nullptr;
    for (const auto & candidate : slots)
    {
        if (std::find(in_flight.begin(), in_flight.end(), candidate.get()) == in_flight.end())
        {
            slot = candidate.get();
            break;
        }
    }
    if (!slot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No slot to expand blocks in on the GPU aggregation's device thread");

    slot->watch.restart();

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
            .packed = work.packed,
            .uploaded = work.uploaded.get(),
        });
    }

    size_t expanded_bytes = 0;
    for (const DeviceWork & work : works)
        expanded_bytes += valueBytesOf(work.blocks, work.packed);

    /// The expansion waits for the copies out of the slot's buffer since it was used last, or for its allocation.
    const size_t allocated = slot->expanded.allocated();
    slot->expanded.clear();
    char * const destination = slot->expanded.grow(expanded_bytes);
    if (slot->expanded.allocated() != allocated || !slot->released_recorded)
    {
        checkCuda(cudaEventRecord(slot->released.get(), compute().get()), "Cannot mark a point in the compute stream");
        slot->released_recorded = true;
    }

    slot->decompressor.launch(works.front().codec, pieces, destination, slot->released.get());
    slot->works = std::move(works);
    in_flight.push_back(slot);
}

void GroupByGPUAccumulator::appendToDeviceColumn(Reader & reader, size_t column_index, const char * device_source, size_t bytes)
{
    DeviceFixedColumnBuffer & column = reader.device_columns[column_index];
    const bool offsets = variableOrdinalOfOffsetsColumn(column_index).has_value();

    /// The offsets of a part start from the 0 its first row starts at, before the ends of its rows.
    if (offsets && column.bytes() == 0)
        checkCuda(
            cudaMemsetAsync(column.grow(sizeof(UInt64)), 0, sizeof(UInt64), compute().get()),
            "Cannot start the offsets of a column of varying width");

    if (bytes == 0)
        return;

    char * const destination = column.grow(bytes);
    checkCuda(cudaMemcpyAsync(destination, device_source, bytes, cudaMemcpyDeviceToDevice, compute().get()), "Cannot append bytes to a device column");

    /// The sizes of the rows become the offsets the rows end at, from the end of the row before; the last of them
    /// comes to the host, a checkpoint of how far the offsets reach.
    if (offsets)
    {
        if (bytes % sizeof(UInt64) != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of the sizes of a column of varying width", bytes);
        const size_t count = bytes / sizeof(UInt64);
        auto * const sizes = reinterpret_cast<uint64_t *>(destination);
        offsetsFromSizes(sizes, count, sizes - 1, compute());

        size_t live = 0;
        for (const auto & checkpoints : reader.variable_checkpoints)
            live += checkpoints.size();
        if (live >= checkpoint_ring_slots)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} checkpoints of the offsets of a reader's columns of varying width at once", live);

        VariableCheckpoint checkpoint;
        checkpoint.rows = reader.sent_rows + column.rows() - 1;
        checkpoint.slot = reader.next_checkpoint_slot++ % checkpoint_ring_slots;
        checkCuda(
            cudaMemcpyAsync(
                reinterpret_cast<UInt64 *>(reader.checkpoint_ring.data()) + checkpoint.slot,
                sizes + count - 1,
                sizeof(UInt64),
                cudaMemcpyDeviceToHost,
                compute().get()),
            "Cannot copy an offset back");
        checkpoint.ready = createEvent();
        checkCuda(cudaEventRecord(checkpoint.ready.get(), compute().get()), "Cannot mark a point in the compute stream");
        reader.variable_checkpoints[*variableOrdinalOfOffsetsColumn(column_index)].push_back(std::move(checkpoint));
    }
}

PinnedBuffer GroupByGPUAccumulator::takeSpareStaging()
{
    std::lock_guard lock(spare_staging_mutex);
    if (spare_staging.empty())
        return {};
    PinnedBuffer buffer = std::move(spare_staging.back());
    spare_staging.pop_back();
    return buffer;
}

void GroupByGPUAccumulator::returnSpareStaging(PinnedBuffer && buffer)
{
    if (buffer.capacity() == 0)
        return;
    buffer.clear();
    std::lock_guard lock(spare_staging_mutex);
    /// As many as the readers stage at once, at most.
    if (spare_staging.size() < readers.size() * numColumns())
        spare_staging.push_back(std::move(buffer));
}

void GroupByGPUAccumulator::finishExpansion()
{
    if (in_flight.empty())
        return;

    ExpansionSlot & slot = *in_flight.front();
    slot.decompressor.wait();

    size_t compressed_bytes = 0;
    size_t at = 0;
    for (DeviceWork & work : slot.works)
    {
        const size_t bytes = valueBytesOf(work.blocks, work.packed);
        appendToDeviceColumn(readers[work.reader], work.column_index, slot.expanded.data() + at + work.skipped_bytes, work.kept_bytes);
        at += bytes;
        compressed_bytes += work.on_device.size();
    }

    checkCuda(cudaEventRecord(slot.released.get(), compute().get()), "Cannot mark a point in the compute stream");
    slot.released_recorded = true;
    /// The pieces are on the device, expanded: their staging buffers go round.
    for (DeviceWork & work : slot.works)
        returnSpareStaging(std::move(work.staged));
    slot.works.clear();
    in_flight.pop_front();

    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, slot.watch.elapsedMicroseconds());
}

void GroupByGPUAccumulator::finishAllExpansions()
{
    while (!in_flight.empty())
        finishExpansion();
}

bool GroupByGPUAccumulator::hasPieceToGroup() const
{
    for (const Reader & reader : readers)
    {
        if (rowsOnDeviceInEveryColumn(reader) >= reader_batch_rows)
            return true;
    }
    return false;
}

bool GroupByGPUAccumulator::groupPiece()
{
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

    finishAllExpansions();
    flushPendingRaw(true);

    Reader & reader = readers[reader_index];

    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (isVariableColumn(i) || isAliasedColumn(i))
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

    const size_t rows_left = num_rows - reader.sent_rows;
    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const DeviceFixedColumnBuffer & offsets = reader.device_columns[first_offsets_column + j];
        const size_t chars_bytes = reader.device_columns[variable_column_indices[j]].bytes();
        /// A part of no rows has no offsets at all: not even the 0 its first row would start at.
        const bool fits = offsets.rows() == 0 ? rows_left == 0 && chars_bytes == 0
                                              : offsets.rows() == rows_left + 1 && offsetAt(offsets, rows_left) - reader.variable_base[j] == chars_bytes;
        if (!fits)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of varying width of a part came to {} rows grouped, {} offsets and {} bytes on the device, where the part has {} rows",
                variable_column_indices[j],
                reader.sent_rows,
                offsets.rows(),
                chars_bytes,
                num_rows);
    }

    sendRowsToGrouping(reader, rows_left);

    for (auto & column : reader.device_columns)
        column.clear();
    for (auto & base : reader.variable_base)
        base = 0;
    for (auto & checkpoints : reader.variable_checkpoints)
        checkpoints.clear();
    reader.sent_rows = 0;
}

size_t GroupByGPUAccumulator::rowsOnDeviceInEveryColumn(const Reader & reader, bool exact) const
{
    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();

    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < first_offsets_column; ++i)
    {
        if (isVariableColumn(i) || isAliasedColumn(i))
            continue;
        rows = std::min(rows, reader.device_columns[i].rows());
    }

    /// Fewer rows than a piece in a column of fixed width is answer enough without a wait for the device.
    if (variable_column_indices.empty() || (!exact && rows < reader_batch_rows))
        return rows;

    /// Of a column of varying width, the rows up to the last checkpoint that is within the rows of the other columns,
    /// is on the host already - or is waited for, where the number must be exact - and whose rows' bytes are all there.
    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const UInt64 chars_end = reader.variable_base[j] + reader.device_columns[variable_column_indices[j]].bytes();
        size_t covered = 0;
        const auto & checkpoints = reader.variable_checkpoints[j];
        for (auto it = checkpoints.rbegin(); it != checkpoints.rend(); ++it)
        {
            if (it->rows <= reader.sent_rows || it->rows - reader.sent_rows > rows)
                continue;
            if (exact)
                checkCuda(cudaEventSynchronize(it->ready.get()), "Cannot wait for an offset to come back");
            else if (!isComplete(it->ready.get()))
                continue;
            if (reader.endOffsetOf(*it) <= chars_end)
            {
                covered = it->rows - reader.sent_rows;
                break;
            }
        }
        rows = std::min(rows, covered);
    }

    return rows;
}

UInt64 GroupByGPUAccumulator::offsetAt(const DeviceFixedColumnBuffer & offsets, size_t index) const
{
    if (index >= offsets.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Offset {} of a column of {} offsets on the device", index, offsets.rows());
    UInt64 value = 0;
    checkCuda(
        cudaMemcpyAsync(&value, reinterpret_cast<const UInt64 *>(offsets.fixedView().data) + index, sizeof(UInt64), cudaMemcpyDeviceToHost, compute().get()),
        "Cannot copy an offset back");
    checkCuda(cudaStreamSynchronize(compute().get()), "Cannot wait for an offset to come back");
    return value;
}

void GroupByGPUAccumulator::sendRowsToGrouping(Reader & reader, size_t num_rows)
{
    if (num_rows == 0)
        return;

    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();

    const cuda::stream_ref compute = this->compute();

    /// Of each column of varying width: where the rows handed over end, from the start of the part - from the
    /// checkpoint at the last of them, or from the device.
    std::vector<UInt64> ends(variable_column_indices.size());
    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const DeviceFixedColumnBuffer & offsets = reader.device_columns[first_offsets_column + j];
        if (offsets.rows() < num_rows + 1)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} rows of column {} of varying width are grouped, of {} offsets on the device",
                num_rows,
                variable_column_indices[j],
                offsets.rows());

        std::optional<UInt64> end;
        for (const VariableCheckpoint & checkpoint : reader.variable_checkpoints[j])
        {
            if (checkpoint.rows == reader.sent_rows + num_rows)
            {
                checkCuda(cudaEventSynchronize(checkpoint.ready.get()), "Cannot wait for an offset to come back");
                end = reader.endOffsetOf(checkpoint);
                break;
            }
        }
        ends[j] = end ? *end : offsetAt(offsets, num_rows);
    }

    GroupingBatch batch;
    batch.rows = num_rows;
    batch.columns.reserve(numColumns());
    size_t next_variable = 0;
    for (size_t i = 0; i < numColumns(); ++i)
    {
        DeviceFixedColumnBuffer & column = reader.device_columns[i];

        if (i >= first_offsets_column)
        {
            /// Of a column of varying width, the offsets of the rows handed over, and the one after the last, which the
            /// rows left start at: from where the first row starts, as the batch's own.
            const UInt64 base = reader.variable_base[i - first_offsets_column];
            DeviceBuffer taken = column.takeFront(num_rows * sizeof(UInt64));
            DeviceBuffer rebased(compute);
            auto * to = reinterpret_cast<uint64_t *>(rebased.grow((num_rows + 1) * sizeof(UInt64)));
            subtractFromOffsets(reinterpret_cast<const uint64_t *>(taken.data()), num_rows, base, to, compute);
            subtractFromOffsets(reinterpret_cast<const uint64_t *>(column.fixedView().data), 1, base, to + num_rows, compute);
            batch.columns.push_back(std::move(rebased));
            continue;
        }

        if (isAliasedColumn(i))
        {
            /// The grouping takes the bytes of the column it reads instead.
            batch.columns.emplace_back(compute);
            continue;
        }

        size_t bytes = num_rows * column.elementSize();
        if (isVariableColumn(i))
        {
            bytes = ends[next_variable] - reader.variable_base[next_variable];
            ++next_variable;
        }
        if (bytes > column.bytes())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "{} rows of column {} are grouped, in {} bytes of the {} on the device", num_rows, i, bytes, column.bytes());

        batch.columns.push_back(column.takeFront(bytes));
    }

    reader.variable_base = ends;
    for (auto & checkpoints : reader.variable_checkpoints)
        while (!checkpoints.empty() && checkpoints.front().rows <= reader.sent_rows + num_rows)
            checkpoints.pop_front();

    batch.filled = createEvent();
    checkCuda(cudaEventRecord(batch.filled.get(), compute.get()), "Cannot mark a point in the compute stream");

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

    const size_t first_filter = group_keys.size() + values.size();
    const size_t first_offsets_column = first_filter + filter_element_types.size();
    const auto element_type_of = [&](size_t i)
    {
        return i < group_keys.size() ? key_element_types[i] : dataElementTypeOf(values[i - group_keys.size()]);
    };
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

        /// A column of the filter that is a key or a value as well reads the bytes of that column.
        size_t source = i;
        if (i >= first_filter && i - first_filter < filter_aliases.size() && filter_aliases[i - first_filter])
            source = *filter_aliases[i - first_filter];
        const GPUElementType element_type = i < first_filter ? element_type_of(i) : filter_element_types[i - first_filter];
        if (source != i && (source >= first_filter || isVariableColumn(source) || element_type_of(source) != element_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of the filter of a GPU aggregation reads column {}, which is not one of its type", i - first_filter, source);
        const DeviceFixedColumn view{.element_type = element_type, .data = batch.columns[source].data(), .rows = batch.rows};

        if (i < group_keys.size())
            keys.push_back(view);
        else if (i < group_keys.size() + values.size())
            value_columns.push_back(view);
        else
            filter_columns.push_back(view);
    }

    Stopwatch watch;

    /// The runtime filters: a column of whether each row's key is among the keys of the join the rows go to, which the
    /// first batch waits for.
    std::vector<DeviceBuffer> membership_masks;
    membership_masks.reserve(memberships.size());
    for (size_t i = 0; i < memberships.size(); ++i)
    {
        if (!key_sets[i])
        {
            Stopwatch waited;
            key_sets[i] = memberships[i].keys->waitForKeySet([this] { return stop_waiting.load(std::memory_order_relaxed); });
            if (!key_sets[i])
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "The keys of a runtime filter were waited for in vain");
            ProfileEvents::increment(ProfileEvents::GPUDeviceWaitMicroseconds, waited.elapsedMicroseconds());
        }
        const DeviceFixedColumn & key_column = filter_columns[memberships[i].key_column];
        membership_masks.emplace_back(grouping);
        char * const mask = membership_masks.back().grow(batch.rows);
        key_sets[i]->contains(key_column, reinterpret_cast<uint8_t *>(mask), grouping);
        filter_columns.push_back(DeviceFixedColumn{.element_type = GPUElementType::UInt8, .data = mask, .rows = batch.rows});
    }

    group_by->addBatch(keys, value_columns, filter_columns, filter ? &*filter : nullptr);
    /// The masks are freed in the order of the grouping stream, after the batch is grouped.

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    if ((on_collected || on_collected_on_device) && group_by->collectedRows() >= collect_flush_rows)
        flushCollected();
}

std::shared_ptr<const GPUDeviceColumns> GroupByGPUAccumulator::takeCollectedOnDevice()
{
    const size_t rows = num_groups.value_or(0);
    std::vector<DeviceColumnView> views;
    views.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i)
    {
        const DeviceColumnView value = group_by->value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of column {} of {} collected", value.rows(), i, rows);
        views.push_back(value);
    }
    std::vector<std::shared_ptr<const void>> owners{std::shared_ptr<const void>(
        group_by->releaseCollected(), [](const void * collected)
        { CudfGroupBy::destroyCollected(static_cast<CudfGroupBy::Collected *>(const_cast<void *>(collected))); })};
    return std::make_shared<GPUDeviceColumns>(std::move(views), rows, std::move(owners), cuda::stream_ref{group_stream.get()});
}

void GroupByGPUAccumulator::flushCollected()
{
    const size_t rows = group_by->takeCollected();
    if (rows == 0)
        return;

    if (on_collected_on_device)
    {
        num_groups = rows;
        on_collected_on_device(takeCollectedOnDevice());
        num_groups.reset();
        return;
    }

    Stopwatch watch;

    MutableColumns columns;
    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    columns.reserve(values.size());
    from.reserve(values.size());
    to.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i)
    {
        const DeviceColumnView value = group_by->value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of column {} of {} collected", value.rows(), i, rows);
        columns.push_back(value_data_types[i]->createColumn());
        from.push_back(value);
        to.push_back(columns.back().get());
    }

    copyDeviceToHost(from, to, cuda::stream_ref{group_stream.get()});
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    on_collected(std::move(columns));
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
    ProfileEvents::increment(ProfileEvents::GPUReadersWaitMicroseconds, readers_wait_microseconds.load(std::memory_order_relaxed));
    ProfileEvents::increment(ProfileEvents::GPUDeviceWaitMicroseconds, device_wait_microseconds.load(std::memory_order_relaxed));
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

    /// Of rows collected rather than grouped, the values are the columns collected.
    const size_t num_values = group_keys.empty() ? values.size() : aggregates.size();
    if (key_columns.size() != group_keys.size() || value_columns.size() != num_values)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} aggregate columns for the groups of a GPU aggregation of {} keys and {} aggregates",
            key_columns.size(),
            value_columns.size(),
            group_keys.size(),
            num_values);

    if (rows == 0)
        return;

    Stopwatch watch;

    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(group_keys.size() + num_values);
    to.reserve(group_keys.size() + num_values);

    for (size_t i = 0; i < group_keys.size(); ++i)
    {
        const DeviceColumnView grouped = group_by->key(i);
        /// The bits of a float key go back into its column as they are.
        const DeviceColumnView key = float_keys[i]
            ? DeviceColumnView(DeviceFixedColumn{
                .element_type = sizeOf(group_keys[i]) == 8 ? GPUElementType::Float64 : GPUElementType::Float32,
                .data = grouped.fixed.data,
                .rows = grouped.fixed.rows})
            : grouped;
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }

    for (size_t i = 0; i < num_values; ++i)
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
