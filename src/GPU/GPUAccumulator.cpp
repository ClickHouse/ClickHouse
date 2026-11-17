#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event GPUAggregationRows;
    extern const Event GPUAggregationBatches;
    extern const Event GPUAggregationMicroseconds;
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
    const IDataType & argument_type, const IDataType & result_type_, GPUAggregationKind aggregation_, size_t batch_bytes_)
    : element_type(reducibleElementTypeOrThrow(argument_type, result_type_, aggregation_))
    , result_type(resultTypeFor(element_type, aggregation_))
    , result_data_type(result_type_.getPtr())
    , aggregation(aggregation_)
    , batch_bytes(batchBytesFor(batch_bytes_, element_type, max_batch_rows))
    , reduction(std::make_unique<CudfReduction>(element_type, result_type, aggregation))
    , pipe(argument_type, std::min(batch_bytes, max_stage_bytes))
{
}

void GPUAccumulator::add(const IColumn & column)
{
    pipe.stage(column);

    if (pipe.stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::reduceBatchOnDevice()
{
    if (pipe.stagedRows() == 0)
        return;

    Stopwatch watch;

    const DeviceColumnView values = pipe.flush().view();
    reduction->addBatch(values);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, values.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    pipe.reset();
}

Field GPUAccumulator::finalize()
{
    reduceBatchOnDevice();

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

/// What a row takes on the device: the bytes of a column of values of varying width are counted as one.
size_t rowBytesOf(const std::vector<GPUElementType> & keys, const std::vector<GPUElementType> & values)
{
    size_t bytes = 0;
    for (const auto * types : {&keys, &values})
    {
        for (const GPUElementType type : *types)
            bytes += columnKindOf(type) == GPUColumnKind::Variable ? 1 : sizeOf(type);
    }
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

}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    size_t batch_bytes)
    : group_keys(groupByKeysOrThrow(key_types))
    , values(valueTypesOrThrow(value_types))
    , aggregates(aggregatesOrThrow(values.size(), std::move(aggregates_)))
    , batch_rows(std::clamp(batch_bytes / std::max<size_t>(rowBytesOf(group_keys, values), 1), size_t{1}, max_batch_rows))
    , key_pipes(pipesFor(key_types, batch_rows))
    , value_pipes(pipesFor(value_types, batch_rows))
    , group_stream(createStream())
    , group_by(std::make_unique<CudfGroupBy>(group_keys, values, aggregates, cuda::stream_ref{group_stream.get()}))
{
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

    /// The columns are uploaded in the compute stream and grouped in the grouping stream, which waits for them; the
    /// compute stream waits for the grouping before the pipes reuse their buffers.
    const cuda::stream_ref compute = GPUResources::get().compute;
    const cuda::stream_ref grouping{group_stream.get()};

    const EventPtr filled = createEvent();
    checkCuda(cudaEventRecord(filled.get(), compute.get()), "Cannot mark a point in the compute stream");
    checkCuda(cudaStreamWaitEvent(grouping.get(), filled.get(), 0), "Cannot make the grouping stream wait for the compute stream");

    group_by->addBatch(keys, value_columns);

    const EventPtr grouped = createEvent();
    checkCuda(cudaEventRecord(grouped.get(), grouping.get()), "Cannot mark a point in the grouping stream");
    checkCuda(cudaStreamWaitEvent(compute.get(), grouped.get(), 0), "Cannot make the compute stream wait for the grouping stream");

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    for (auto & pipe : key_pipes)
        pipe.reset();
    for (auto & pipe : value_pipes)
        pipe.reset();
}

size_t GroupByGPUAccumulator::finalize()
{
    sendBatchToDevice();

    Stopwatch watch;
    num_groups = group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GroupByGPUAccumulator::copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    if (key_columns.size() != group_keys.size() || aggregate_columns.size() != aggregates.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} aggregate columns for the groups of a GPU aggregation of {} keys and {} aggregates",
            key_columns.size(),
            aggregate_columns.size(),
            group_keys.size(),
            aggregates.size());

    const size_t rows = *num_groups;
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
        to.push_back(aggregate_columns[i].get());
    }

    copyDeviceToHost(from, to, cuda::stream_ref{group_stream.get()});

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

}

#endif
