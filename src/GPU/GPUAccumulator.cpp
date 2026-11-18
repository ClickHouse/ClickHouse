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
}

namespace DB::GPU
{

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

std::vector<GPUGroupByAggregate> aggregatesOrThrow(size_t num_columns, std::vector<GPUGroupByAggregate> aggregates, bool keyed)
{
    /// Without aggregates, the rows are collected, not aggregated, which they are not by keys.
    if (keyed && aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with keys and without aggregates");

    for (const auto & aggregate : aggregates)
    {
        if (aggregate.column != GPUGroupByAggregate::no_column && aggregate.column >= num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregate of value {} of {}", aggregate.column, num_columns);
    }
    return aggregates;
}

std::vector<GPUElementType> withDerived(std::vector<GPUElementType> values, const std::vector<GPUDerivedValue> & derived)
{
    for (const auto & value : derived)
        values.push_back(value.expression.value_type);
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

std::vector<ColumnUploadPipe> pipesFor(const DataTypes & types, size_t batch_rows)
{
    std::vector<ColumnUploadPipe> pipes;
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

/// What a worker of a `GPUCompressedGroupBy` stages of one column in pinned memory, at most, before it expands a batch:
/// the compressed blocks staged, or the raw bytes.
constexpr size_t compressed_stage_bytes = 32UL * 1024 * 1024;

/// Of rows collected rather than grouped: how many are kept on the device before they are handed over.
constexpr size_t collect_flush_rows = 1UL << 20;


}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    size_t batch_bytes)
    : group_keys(groupByKeysOrThrow(key_types))
    , key_element_types(keyElementTypesOf(group_keys))
    , values(valueTypesOrThrow(value_types, /*collect=*/false))
    , aggregates(aggregatesOrThrow(values.size(), std::move(aggregates_), !key_types.empty()))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_types, values, {}), size_t{1}, max_batch_rows))
    , key_pipes(pipesFor(key_types, batch_rows))
    , value_pipes(pipesFor(value_types, batch_rows))
{
    if (aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over plain columns without aggregates");

    group_stream = createStream();
    group_by = std::make_unique<CudfGroupBy>(
        group_keys, values, std::vector<GPUDerivedValue>{}, aggregates, cuda::stream_ref{group_stream.get()});
}

void GroupByGPUAccumulator::add(const Columns & key_columns, const Columns & value_columns)
{
    if (key_columns.size() != group_keys.size() || value_columns.size() != value_pipes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A block of {} key and {} value columns for a GPU aggregation over {} and {}",
            key_columns.size(),
            value_columns.size(),
            group_keys.size(),
            value_pipes.size());

    const size_t num_rows = value_columns.front()->size();
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

    groupBatch(keys, value_columns);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    for (auto & pipe : key_pipes)
        pipe.reset();
    for (auto & pipe : value_pipes)
        pipe.reset();
}

void GroupByGPUAccumulator::groupBatch(const std::vector<DeviceColumnView> & keys, const std::vector<DeviceColumnView> & value_columns)
{
    const cuda::stream_ref compute = GPUResources::get().compute;
    const cuda::stream_ref grouping = cuda::stream_ref{group_stream.get()};

    /// The pipes send their columns in the order of the compute stream, and stage the next batch into them once the
    /// grouping is done with them.
    const EventPtr filled = createEvent();
    checkCuda(cudaEventRecord(filled.get(), compute.get()), "Cannot mark a point in the compute stream");
    checkCuda(cudaStreamWaitEvent(grouping.get(), filled.get(), 0), "Cannot make the grouping stream wait for the compute stream");

    group_by->addBatch(keys, value_columns);

    const EventPtr grouped = createEvent();
    checkCuda(cudaEventRecord(grouped.get(), grouping.get()), "Cannot mark a point in the grouping stream");
    checkCuda(cudaStreamWaitEvent(compute.get(), grouped.get(), 0), "Cannot make the compute stream wait for the grouping stream");
}

size_t GroupByGPUAccumulator::finalize()
{
    sendBatchToDevice();

    Stopwatch watch;
    const size_t groups = group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    num_groups = groups;
    return groups;
}

size_t GroupByGPUAccumulator::filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were filtered before it was finalized");

    Stopwatch watch;
    num_groups = group_by->filterGroups(expression, columns);
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

namespace
{

/// The grouping lanes of a `GPUCompressedGroupBy`, at most: as Sirius runs 4 to 8 tasks on a device at once, enough that
/// one waiting for the device in cuDF leaves it to the others, and few, as each keeps the groups so far.
constexpr size_t max_grouping_lanes = 8;

/// The rows of a batch of `GPUCompressedGroupBy`, at most: what cuDF takes as the rows of a column.
constexpr size_t max_compressed_batch_rows = (1UL << 31) - 1;

/// Hands a lane back to the free ones however its batch ends.
class LaneTicket
{
public:
    LaneTicket(ConcurrentBoundedQueue<size_t> & free_lanes_, size_t lane_) : free_lanes(free_lanes_), lane(lane_) { }
    ~LaneTicket() { [[maybe_unused]] const bool pushed = free_lanes.push(lane); }
    LaneTicket(const LaneTicket &) = delete;
    LaneTicket & operator=(const LaneTicket &) = delete;

private:
    ConcurrentBoundedQueue<size_t> & free_lanes;
    const size_t lane;
};

}

GPUCompressedGroupBy::GPUCompressedGroupBy(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    size_t batch_bytes,
    size_t num_readers,
    const DataTypes & filter_types,
    std::optional<GPUExpression> filter_,
    std::vector<GPUDerivedValue> derived_,
    std::function<void(MutableColumns)> on_collected_,
    std::vector<std::optional<uint32_t>> filter_aliases_)
    : group_keys(groupByKeysOrThrow(key_types))
    , key_element_types(keyElementTypesOf(group_keys))
    , values(valueTypesOrThrow(value_types, aggregates_.empty()))
    , value_data_types(value_types)
    , on_collected(std::move(on_collected_))
    , derived(std::move(derived_))
    , aggregates(aggregatesOrThrow(values.size() + derived.size(), std::move(aggregates_), !key_types.empty()))
    , variable_column_indices(variableColumnIndicesOf(group_keys, values))
    , filter_element_types(filterElementTypesOrThrow(filter_types))
    , filter(std::move(filter_))
    , filter_aliases(std::move(filter_aliases_))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_types, values, filter_element_types), size_t{1}, max_compressed_batch_rows))
    , free_lanes(std::clamp<size_t>(num_readers, 1, max_grouping_lanes))
{
    if (num_readers == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over compressed blocks with no reading threads");

    for (const auto & value : derived)
    {
        for (const uint32_t column : value.columns)
        {
            if (column >= values.size() || columnKindOf(values[column]) != GPUColumnKind::Fixed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A value the GPU derives from value {} of {}", column, values.size());
        }
    }

    if (filter.has_value() != !filter_element_types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with a `WHERE` and the columns of one do not go together");

    if (on_collected && !aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with aggregates that hands rows over as collected");

    if (filter && filter->num_columns != filter_element_types.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation's `WHERE` reads {} columns and is given {}",
            filter->num_columns,
            filter_element_types.size());

    const size_t num_lanes = std::clamp<size_t>(num_readers, 1, max_grouping_lanes);
    lanes.resize(num_lanes);
    for (size_t i = 0; i < num_lanes; ++i)
    {
        Lane & lane = lanes[i];
        lane.group_by = std::make_unique<CudfGroupBy>(
            group_keys, withDerived(values, derived), derived, aggregates, cuda::stream_ref{lane.stream.get()});
        /// Rows collected are not grouped, and there is nothing to decide.
        lane.decided = aggregates.empty();
        if (!free_lanes.push(i))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No room for a lane of a GPU aggregation");
    }

    const size_t num_columns = numColumns();
    workers.reserve(num_readers);
    for (size_t i = 0; i < num_readers; ++i)
    {
        auto worker = std::make_unique<Worker>();
        worker->staging.resize(num_columns);
        worker->carried.reserve(num_columns);
        for (size_t c = 0; c < num_columns; ++c)
            worker->carried.emplace_back(cuda::stream_ref{worker->stream.get()});
        workers.push_back(std::move(worker));
    }
}

GPUCompressedGroupBy::~GPUCompressedGroupBy() = default;

bool GPUCompressedGroupBy::isAliasedColumn(size_t column_index) const
{
    const size_t first_filter = group_keys.size() + values.size();
    return column_index >= first_filter && column_index - first_filter < filter_aliases.size() && filter_aliases[column_index - first_filter];
}

bool GPUCompressedGroupBy::isVariableColumn(size_t column_index) const
{
    return std::find(variable_column_indices.begin(), variable_column_indices.end(), column_index) != variable_column_indices.end();
}

std::optional<size_t> GPUCompressedGroupBy::variableOrdinalOfOffsetsColumn(size_t column_index) const
{
    const size_t first = group_keys.size() + values.size() + filter_element_types.size();
    if (column_index < first || column_index >= numColumns())
        return std::nullopt;
    return column_index - first;
}

size_t GPUCompressedGroupBy::offsetsColumnOf(size_t column_index) const
{
    const auto it = std::find(variable_column_indices.begin(), variable_column_indices.end(), column_index);
    if (it == variable_column_indices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation is not of varying width", column_index);
    return group_keys.size() + values.size() + filter_element_types.size() + static_cast<size_t>(it - variable_column_indices.begin());
}

GPUElementType GPUCompressedGroupBy::elementTypeOf(size_t column_index) const
{
    if (variableOrdinalOfOffsetsColumn(column_index))
        return GPUElementType::UInt64;
    if (column_index < group_keys.size())
        return key_element_types[column_index];
    if (column_index < group_keys.size() + values.size())
        return dataElementTypeOf(values[column_index - group_keys.size()]);
    return filter_element_types.at(column_index - group_keys.size() - values.size());
}

size_t GPUCompressedGroupBy::elementBytesOf(size_t column_index) const
{
    if (isVariableColumn(column_index))
        return 1;
    return sizeOf(elementTypeOf(column_index));
}

GPUCompressedGroupBy::Worker & GPUCompressedGroupBy::workerOrThrow(size_t reader)
{
    if (reader >= workers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reader {} of a GPU aggregation with {} readers", reader, workers.size());
    return *workers[reader];
}

bool GPUCompressedGroupBy::batchIsStaged(const Worker & worker) const
{
    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t c = 0; c < numColumns(); ++c)
    {
        if (isAliasedColumn(c))
            continue;
        const StagedColumn & column = worker.staging[c];
        /// Staged as far as the pinned memory of a column goes, whatever the other columns have.
        if (column.staged.size() >= compressed_stage_bytes)
            return true;
        /// The bytes of a column of varying width make no rows of their own: its sizes do.
        if (isVariableColumn(c))
            continue;
        const size_t carried_bytes = worker.carried[c].size();
        const size_t carried_rows = variableOrdinalOfOffsetsColumn(c)
            ? (carried_bytes == 0 ? 0 : carried_bytes / sizeof(UInt64) - 1)
            : carried_bytes / elementBytesOf(c);
        rows = std::min(rows, carried_rows + column.kept_bytes / elementBytesOf(c));
    }
    return rows != std::numeric_limits<size_t>::max() && rows >= batch_rows;
}

void GPUCompressedGroupBy::addCompressedBlock(
    size_t reader,
    size_t column_index,
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes,
    size_t expected_bytes,
    size_t skipped_bytes,
    std::optional<size_t> kept_bytes)
{
    Worker & worker = workerOrThrow(reader);

    const auto codec = deviceCodecOf(method, payload);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A block of compression method {:#x} for a GPU aggregation, which the device does not expand", static_cast<UInt16>(method));

    if (column_index >= worker.staging.size() || isAliasedColumn(column_index))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of column {} of a GPU aggregation over {} columns", column_index, worker.staging.size());

    const size_t kept = kept_bytes.value_or(decompressed_bytes - std::min(skipped_bytes, decompressed_bytes));
    if (skipped_bytes + kept > decompressed_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept, skipped_bytes, decompressed_bytes);

    /// A batch has one run of bytes of each column: it starts at a block that skips bytes, and ends at one cut short.
    {
        const StagedColumn & column = worker.staging[column_index];
        if (column.raw || column.cut || (!column.blocks.empty() && (skipped_bytes != 0 || column.codec != codec)))
            flush(worker);
    }

    StagedColumn & column = worker.staging[column_index];
    if (column.blocks.empty())
    {
        column.skipped_bytes = skipped_bytes;
        column.staged.reserve(std::min(compressed_stage_bytes, std::max(expected_bytes, payload.size())));
    }
    column.kept_bytes += kept;
    column.cut = skipped_bytes + kept != decompressed_bytes;
    column.codec = codec;
    stageCompressedBlock(column.staged, column.blocks, column.packed, method, payload, decompressed_bytes);
    column.decompressed_bytes += decompressed_bytes;

    if (batchIsStaged(worker))
        flush(worker);
}

std::span<char> GPUCompressedGroupBy::reserveRawBytes(size_t reader, size_t column_index, size_t max_bytes, size_t expected_bytes)
{
    Worker & worker = workerOrThrow(reader);
    if (column_index >= worker.staging.size() || isAliasedColumn(column_index))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Raw bytes of column {} of a GPU aggregation over {} columns", column_index, worker.staging.size());

    if (!worker.staging[column_index].blocks.empty() || worker.staging[column_index].staged.size() >= compressed_stage_bytes)
        flush(worker);

    StagedColumn & column = worker.staging[column_index];
    column.raw = true;
    column.staged.reserve(std::min(compressed_stage_bytes, column.staged.size() + std::max<size_t>(expected_bytes, 1)));
    return {
        column.staged.data() + column.staged.size(),
        std::min({max_bytes, compressed_stage_bytes - column.staged.size(), column.staged.capacity() - column.staged.size()})};
}

void GPUCompressedGroupBy::commitRawBytes(size_t reader, size_t column_index, size_t bytes)
{
    Worker & worker = workerOrThrow(reader);
    StagedColumn & column = worker.staging.at(column_index);
    growBy(column.staged, bytes);
    column.decompressed_bytes += bytes;
    column.kept_bytes += bytes;

    if (batchIsStaged(worker))
        flush(worker);
}

void GPUCompressedGroupBy::finishPart(size_t reader, size_t num_rows)
{
    flush(workerOrThrow(reader), num_rows);
}

void GPUCompressedGroupBy::flush(Worker & worker, std::optional<size_t> part_rows)
{
    const cuda::stream_ref stream{worker.stream.get()};
    const size_t num_columns = numColumns();
    const size_t first_offsets_column = group_keys.size() + values.size() + filter_element_types.size();

    Stopwatch expansion_watch;

    /// Of each column, the buffer of the batch: what was carried, then what the staged blocks expand to. A column of a
    /// fixed width, or the bytes of one of varying width, take `columns_bytes` from `starts`; a column of sizes holds the
    /// offsets of `columns_bytes` rows, from 0, one more than them.
    std::vector<DeviceBuffer> buffers;
    buffers.reserve(num_columns);
    std::vector<char *> starts(num_columns, nullptr);
    std::vector<size_t> columns_bytes(num_columns, 0);
    std::vector<size_t> carried_rows(num_columns, 0);

    size_t compressed_total = 0;
    for (const StagedColumn & column : worker.staging)
        if (!column.raw)
            compressed_total += column.staged.size();
    DeviceBuffer uploaded(stream);
    char * const uploaded_base = compressed_total ? uploaded.grow(compressed_total) : nullptr;
    size_t uploaded_at = 0;

    std::vector<std::pair<GPUCodec, std::vector<CompressedPiece>>> pieces;
    const auto piecesOf = [&](GPUCodec codec) -> std::vector<CompressedPiece> &
    {
        for (auto & [of, list] : pieces)
            if (of == codec)
                return list;
        return pieces.emplace_back(codec, std::vector<CompressedPiece>{}).second;
    };

    for (size_t c = 0; c < num_columns; ++c)
    {
        buffers.emplace_back(stream);
        if (isAliasedColumn(c))
            continue;

        StagedColumn & column = worker.staging[c];
        const DeviceBuffer & carried = worker.carried[c];
        const size_t expanded = column.raw ? column.decompressed_bytes : valueBytesOf(column.blocks, column.packed);
        if (column.raw && column.staged.size() != expanded)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} raw bytes of column {} staged for the GPU, {} counted", column.staged.size(), c, expanded);

        char * destination = nullptr;
        if (variableOrdinalOfOffsetsColumn(c))
        {
            if (column.skipped_bytes != 0 || column.cut)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a column of varying width for the GPU read in ranges of marks");
            const size_t carried_entries = carried.empty() ? 1 : carried.size() / sizeof(UInt64);
            char * const base = buffers[c].grow(carried_entries * sizeof(UInt64) + expanded);
            if (carried.empty())
                checkCuda(cudaMemsetAsync(base, 0, sizeof(UInt64), stream.get()), "Cannot start the offsets of a column of varying width");
            else
                checkCuda(cudaMemcpyAsync(base, carried.data(), carried.size(), cudaMemcpyDeviceToDevice, stream.get()), "Cannot carry offsets on the device");
            destination = base + carried_entries * sizeof(UInt64);
            starts[c] = base;
            carried_rows[c] = carried_entries - 1;
            columns_bytes[c] = carried_rows[c] + column.kept_bytes / sizeof(UInt64);
        }
        else
        {
            /// The blocks expand right after where the carried bytes end, the bytes they skip over the end of those, which
            /// are copied in once they are expanded.
            const size_t carried_bytes = carried.size();
            const size_t lead = std::max(carried_bytes, column.skipped_bytes);
            char * const base = buffers[c].grow(lead - column.skipped_bytes + expanded);
            destination = base + lead - column.skipped_bytes;
            starts[c] = base + lead - carried_bytes;
            columns_bytes[c] = carried_bytes + column.kept_bytes;
        }

        if (expanded == 0)
            continue;
        if (column.raw)
        {
            checkCuda(
                cudaMemcpyAsync(destination, column.staged.data(), column.staged.size(), cudaMemcpyHostToDevice, stream.get()),
                "Cannot send raw bytes to the device");
            continue;
        }
        char * const at = uploaded_base + uploaded_at;
        checkCuda(
            cudaMemcpyAsync(at, column.staged.data(), column.staged.size(), cudaMemcpyHostToDevice, stream.get()),
            "Cannot send compressed blocks to the device");
        piecesOf(*column.codec).push_back({
            .device_compressed = at,
            .compressed_bytes = column.staged.size(),
            .blocks = column.blocks,
            .packed = column.packed,
            .destination = destination,
        });
        uploaded_at += column.staged.size();
    }

    /// Waits for the device, and for the uploads before it, so that the staging buffers are free once it returns.
    checkCuda(cudaEventRecord(worker.ready.get(), stream.get()), "Cannot mark a point in a worker's stream");
    for (auto & [codec, list] : pieces)
    {
        worker.decompression.launch(codec, list, nullptr, worker.ready.get());
        worker.decompression.waitAndCheck();
    }

    for (size_t c = 0; c < num_columns; ++c)
    {
        if (isAliasedColumn(c) || variableOrdinalOfOffsetsColumn(c) || worker.carried[c].empty())
            continue;
        checkCuda(
            cudaMemcpyAsync(starts[c], worker.carried[c].data(), worker.carried[c].size(), cudaMemcpyDeviceToDevice, stream.get()),
            "Cannot carry bytes on the device");
    }

    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const size_t o = first_offsets_column + j;
        auto * const offsets = reinterpret_cast<uint64_t *>(starts[o]);
        const size_t fresh = columns_bytes[o] - carried_rows[o];
        if (fresh != 0)
            offsetsFromSizes(offsets + carried_rows[o] + 1, fresh, offsets + carried_rows[o], stream);
    }

    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, compressed_total);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, expansion_watch.elapsedMicroseconds());

    /// The rows every column has: of one of varying width, those whose bytes are all there.
    Stopwatch wait_watch;
    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t c = 0; c < first_offsets_column; ++c)
    {
        if (isAliasedColumn(c) || isVariableColumn(c))
            continue;
        rows = std::min(rows, columns_bytes[c] / elementBytesOf(c));
    }
    std::vector<CoveredRows> covered(variable_column_indices.size());
    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const size_t o = first_offsets_column + j;
        covered[j] = rowsCoveredBy(
            reinterpret_cast<const uint64_t *>(starts[o]), columns_bytes[o], columns_bytes[variable_column_indices[j]], stream);
        rows = std::min(rows, covered[j].rows);
    }
    if (rows == std::numeric_limits<size_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A batch of a GPU aggregation without columns");

    std::vector<uint64_t> bytes_of_rows(variable_column_indices.size());
    for (size_t j = 0; j < variable_column_indices.size(); ++j)
    {
        const size_t o = first_offsets_column + j;
        bytes_of_rows[j] = covered[j].rows == rows
            ? covered[j].bytes
            : rowsCoveredBy(reinterpret_cast<const uint64_t *>(starts[o]), rows, columns_bytes[variable_column_indices[j]], stream).bytes;
    }
    device_wait_microseconds.fetch_add(wait_watch.elapsedMicroseconds(), std::memory_order_relaxed);

    if (part_rows)
    {
        if (*part_rows < worker.rows_grouped || rows != *part_rows - worker.rows_grouped)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A part of {} rows came to {} rows grouped and {} on the device in every column of a GPU aggregation",
                *part_rows,
                worker.rows_grouped,
                rows);
        for (size_t c = 0; c < first_offsets_column; ++c)
        {
            if (isAliasedColumn(c) || isVariableColumn(c))
                continue;
            if (columns_bytes[c] != rows * elementBytesOf(c))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Column {} of a part came to {} bytes on the device, where the part has {} rows", c, columns_bytes[c], *part_rows);
        }
        for (size_t j = 0; j < variable_column_indices.size(); ++j)
        {
            if (columns_bytes[first_offsets_column + j] != rows || bytes_of_rows[j] != columns_bytes[variable_column_indices[j]])
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} of varying width of a part came to {} sizes and {} bytes on the device, of which its {} rows take {}",
                    variable_column_indices[j],
                    columns_bytes[first_offsets_column + j],
                    columns_bytes[variable_column_indices[j]],
                    rows,
                    bytes_of_rows[j]);
        }
    }

    if (rows != 0)
    {
        const auto view_of = [&](size_t c) -> DeviceColumnView
        {
            if (isVariableColumn(c))
            {
                const size_t j = static_cast<size_t>(
                    std::find(variable_column_indices.begin(), variable_column_indices.end(), c) - variable_column_indices.begin());
                return DeviceVariableColumn{
                    .offsets = reinterpret_cast<const uint64_t *>(starts[first_offsets_column + j]),
                    .chars = starts[c],
                    .rows = rows,
                    .chars_bytes = bytes_of_rows[j],
                };
            }
            return DeviceFixedColumn{.element_type = elementTypeOf(c), .data = starts[c], .rows = rows};
        };

        std::vector<DeviceColumnView> keys;
        keys.reserve(group_keys.size());
        for (size_t i = 0; i < group_keys.size(); ++i)
            keys.push_back(view_of(i));

        std::vector<DeviceColumnView> value_columns;
        value_columns.reserve(values.size());
        for (size_t i = 0; i < values.size(); ++i)
            value_columns.push_back(view_of(group_keys.size() + i));

        /// A column of the filter that is a key or a value as well reads the bytes of that column.
        const size_t first_filter = group_keys.size() + values.size();
        std::vector<DeviceFixedColumn> filter_columns;
        filter_columns.reserve(filter_element_types.size());
        for (size_t i = 0; i < filter_element_types.size(); ++i)
        {
            const size_t c = first_filter + i;
            const size_t source = isAliasedColumn(c) ? *filter_aliases[i] : c;
            if (source != c && (source >= first_filter || isVariableColumn(source) || elementTypeOf(source) != filter_element_types[i]))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Column {} of the filter of a GPU aggregation reads column {}, which is not of its type", i, source);
            filter_columns.push_back({.element_type = filter_element_types[i], .data = starts[source], .rows = rows});
        }

        group(keys, value_columns, filter_columns, stream);
        worker.rows_grouped += rows;
    }

    /// What the batch did not take is carried to the next one, in the order of the worker's stream, which waited for the
    /// lane to be done with the batch; the buffers of the batch are freed in that order as well.
    for (size_t c = 0; c < num_columns; ++c)
    {
        if (isAliasedColumn(c))
            continue;

        DeviceBuffer next(stream);
        if (const auto j = variableOrdinalOfOffsetsColumn(c))
        {
            const size_t left = columns_bytes[c] - rows;
            if (left != 0)
            {
                auto * const to = reinterpret_cast<uint64_t *>(next.grow((left + 1) * sizeof(UInt64)));
                subtractFromOffsets(reinterpret_cast<const uint64_t *>(starts[c]) + rows, left + 1, bytes_of_rows[*j], to, stream);
            }
        }
        else
        {
            size_t taken = rows * elementBytesOf(c);
            if (isVariableColumn(c))
                taken = bytes_of_rows[static_cast<size_t>(
                    std::find(variable_column_indices.begin(), variable_column_indices.end(), c) - variable_column_indices.begin())];
            const size_t left = columns_bytes[c] - taken;
            if (left != 0)
                checkCuda(
                    cudaMemcpyAsync(next.grow(left), starts[c] + taken, left, cudaMemcpyDeviceToDevice, stream.get()),
                    "Cannot carry bytes on the device");
        }
        worker.carried[c] = std::move(next);

        StagedColumn & column = worker.staging[c];
        column.staged.clear();
        column.blocks.clear();
        column.packed.clear();
        column.codec.reset();
        column.raw = false;
        column.decompressed_bytes = 0;
        column.skipped_bytes = 0;
        column.kept_bytes = 0;
        column.cut = false;
    }

    if (part_rows)
        worker.rows_grouped = 0;
}

void GPUCompressedGroupBy::group(
    const std::vector<DeviceColumnView> & keys,
    const std::vector<DeviceColumnView> & value_columns,
    const std::vector<DeviceFixedColumn> & filter_columns,
    cuda::stream_ref worker_stream)
{
    /// The first lane free: a worker waits only where every lane is grouping.
    Stopwatch wait_watch;
    size_t lane_index = 0;
    if (!free_lanes.pop(lane_index))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No grouping lane of a GPU aggregation is left");
    const LaneTicket ticket(free_lanes, lane_index);
    lanes_wait_microseconds.fetch_add(wait_watch.elapsedMicroseconds(), std::memory_order_relaxed);

    Lane & lane = lanes[lane_index];
    const cuda::stream_ref lane_stream{lane.stream.get()};

    /// The batch is on the device in the order of the worker's stream.
    const EventPtr batch_ready = createEvent();
    checkCuda(cudaEventRecord(batch_ready.get(), worker_stream.get()), "Cannot mark a point in a worker's stream");
    checkCuda(cudaStreamWaitEvent(lane_stream.get(), batch_ready.get(), 0), "Cannot make a lane wait for a worker");

    Stopwatch watch;
    const GPUExpression * const where = filter ? &*filter : nullptr;
    if (lane.decided)
    {
        lane.group_by->addBatch(keys, value_columns, filter_columns, where);
    }
    else
    {
        /// The lanes reduce their batches alike, so that they merge: as the first batch with rows decided.
        std::unique_lock lock(decision_mutex);
        if (reduce_runs)
        {
            lane.group_by->reduceRuns(*reduce_runs);
            lane.decided = true;
            lock.unlock();
            lane.group_by->addBatch(keys, value_columns, filter_columns, where);
        }
        else
        {
            lane.group_by->addBatch(keys, value_columns, filter_columns, where);
            if (lane.group_by->decided())
            {
                reduce_runs = lane.group_by->reducesRuns();
                lane.decided = true;
            }
        }
    }

    const size_t rows = keys.empty() ? (value_columns.empty() ? 0 : value_columns.front().rows()) : keys.front().rows();
    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    /// The worker frees or reuses the batch's buffers only after the lane is done with them.
    checkCuda(cudaEventRecord(lane.consumed.get(), lane_stream.get()), "Cannot mark a point in a lane's stream");
    checkCuda(cudaStreamWaitEvent(worker_stream.get(), lane.consumed.get(), 0), "Cannot make a worker wait for a lane");

    if (on_collected && lane.group_by->collectedRows() >= collect_flush_rows)
        flushCollected(lane);
}

void GPUCompressedGroupBy::flushCollected(Lane & lane)
{
    const size_t rows = lane.group_by->takeCollected();
    if (rows == 0)
        return;

    Stopwatch watch;

    MutableColumns columns;
    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    columns.reserve(values.size());
    from.reserve(values.size());
    to.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i)
    {
        const DeviceColumnView value = lane.group_by->value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of column {} of {} collected", value.rows(), i, rows);
        columns.push_back(value_data_types[i]->createColumn());
        from.push_back(value);
        to.push_back(columns.back().get());
    }

    copyDeviceToHost(from, to, cuda::stream_ref{lane.stream.get()});
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    on_collected(std::move(columns));
}

size_t GPUCompressedGroupBy::finalize()
{
    ProfileEvents::increment(ProfileEvents::GPUReadersWaitMicroseconds, lanes_wait_microseconds.load(std::memory_order_relaxed));
    ProfileEvents::increment(ProfileEvents::GPUDeviceWaitMicroseconds, device_wait_microseconds.load(std::memory_order_relaxed));

    Stopwatch watch;
    /// Into the first lane, which keeps the groups.
    for (size_t i = 1; i < lanes.size(); ++i)
        lanes.front().group_by->absorb(*lanes[i].group_by);
    const size_t groups = lanes.front().group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    num_groups = groups;
    return groups;
}

size_t GPUCompressedGroupBy::filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were filtered before it was finalized");

    Stopwatch watch;
    num_groups = lanes.front().group_by->filterGroups(expression, columns);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GPUCompressedGroupBy::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    const size_t rows = *num_groups;
    const size_t num_values = aggregates.empty() ? values.size() : aggregates.size();
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
    CudfGroupBy & group_by = *lanes.front().group_by;

    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(group_keys.size() + num_values);
    to.reserve(group_keys.size() + num_values);
    for (size_t i = 0; i < group_keys.size(); ++i)
    {
        const DeviceColumnView key = group_by.key(i);
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }
    for (size_t i = 0; i < num_values; ++i)
    {
        const DeviceColumnView value = group_by.value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of aggregate {} of {} groups", value.rows(), i, rows);
        from.push_back(value);
        to.push_back(value_columns[i].get());
    }

    copyDeviceToHost(from, to, cuda::stream_ref{lanes.front().stream.get()});
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

}

#endif
