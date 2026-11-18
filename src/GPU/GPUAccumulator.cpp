#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <span>

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

constexpr size_t max_batch_rows = (1UL << 31) - 1;

/// What a row takes on the device: the bytes of a column of values of varying width are counted as one.
size_t rowBytesOf(const DataTypes & types)
{
    size_t bytes = 0;
    for (const auto & type : types)
    {
        const GPUElementType element_type = uploadTypeOrThrow(*type);
        bytes += columnKindOf(element_type) == GPUColumnKind::Variable ? 1 : sizeOf(element_type);
    }
    return bytes;
}

size_t batchRowsFor(size_t batch_bytes, const DataTypes & types)
{
    return std::clamp(batch_bytes / std::max<size_t>(rowBytesOf(types), 1), size_t{1}, max_batch_rows);
}

std::vector<ColumnUploadPipe> pipesFor(const DataTypes & types, size_t batch_rows, cuda::stream_ref stream)
{
    std::vector<ColumnUploadPipe> pipes;
    pipes.reserve(types.size());
    for (const auto & type : types)
    {
        const GPUElementType column_type = uploadTypeOrThrow(*type);
        const size_t stage_bytes = columnKindOf(column_type) == GPUColumnKind::Fixed
            ? std::min(batch_rows * sizeOf(column_type), GPUBatchUploader::max_stage_bytes)
            : GPUBatchUploader::max_stage_bytes;
        pipes.emplace_back(*type, stage_bytes, stream);
    }
    return pipes;
}

std::shared_ptr<StreamPtr> sharedStream()
{
    return std::make_shared<StreamPtr>(createStream());
}

/// The result of a `sum` is wider than what it sums; a `min` or a `max` is of the type it takes.
GPUElementType resultTypeFor(GPUElementType element_type, GPUAggregationKind aggregation)
{
    return aggregation == GPUAggregationKind::Sum ? sumResultTypeFor(element_type) : element_type;
}

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

/// The bytes of a mask of a bit per row of `rows`, in 32-bit words.
size_t maskBytes(size_t rows)
{
    return (rows + 31) / 32 * sizeof(uint32_t);
}

std::vector<GPUExpressionInput> inputsOf(const GPUExpression & expression, std::span<const DeviceColumnView> columns, size_t rows)
{
    if (columns.size() != expression.num_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU predicate of {} columns over {}", expression.num_columns, columns.size());

    std::vector<GPUExpressionInput> inputs;
    inputs.reserve(columns.size());
    for (const auto & column : columns)
    {
        const DeviceFixedColumn & fixed = fixedOrThrow(column);
        if (fixed.rows != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of a GPU predicate of {} rows among {}", fixed.rows, rows);
        inputs.push_back({.data = fixed.data, .type = fixed.element_type});
    }
    return inputs;
}

}

GPUDeviceBatch::~GPUDeviceBatch()
{
    if (!consumed || !consumer || !upload_stream)
        return;

    /// The uploader frees the memory in the order of its stream, after what the consumer's stream did with it.
    try
    {
        checkCuda(cudaEventRecord(consumed.get(), consumer), "Cannot mark a point in the stream of a GPU batch's consumer");
        checkCuda(cudaStreamWaitEvent(upload_stream->get(), consumed.get(), 0), "Cannot make an upload stream wait for a GPU batch's consumer");
    }
    catch (...)
    {
        /// The memory is not freed before the consumer's stream is done with it.
        tryLogCurrentException(__PRETTY_FUNCTION__);
        cudaStreamSynchronize(consumer);
    }
}

void GPUDeviceBatch::moveTo(cuda::stream_ref stream)
{
    if (consumer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch handed to a stream twice");

    checkCuda(cudaStreamWaitEvent(stream.get(), ready.get(), 0), "Cannot make a stream wait for a GPU batch");
    consumed = createEvent();
    consumer = stream.get();
}

GPUBatchUploader::GPUBatchUploader(const DataTypes & types, size_t batch_bytes)
    : stream(sharedStream())
    , batch_rows(batchRowsFor(batch_bytes, types))
    , pipes(pipesFor(types, batch_rows, cuda::stream_ref{stream->get()}))
{
    if (types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation of no columns");
}

size_t GPUBatchUploader::stagedVariableBytes() const
{
    size_t bytes = 0;
    for (const auto & pipe : pipes)
    {
        if (columnKindOf(pipe.type()) == GPUColumnKind::Variable)
            bytes += pipe.stagedBytes();
    }
    return bytes;
}

bool GPUBatchUploader::wouldOverflow(const Columns & columns) const
{
    const size_t staged = stagedRows();
    if (staged == 0 || columns.empty())
        return false;

    const size_t rows = columns.front()->size();
    if (staged + rows > batch_rows)
        return true;

    size_t variable_bytes = 0;
    for (size_t i = 0; i < columns.size() && i < pipes.size(); ++i)
    {
        if (columnKindOf(pipes[i].type()) == GPUColumnKind::Variable)
            variable_bytes += columns[i]->byteSize();
    }
    return variable_bytes != 0 && stagedVariableBytes() + variable_bytes > max_stage_bytes;
}

void GPUBatchUploader::add(const Columns & columns)
{
    if (columns.size() != pipes.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of {} columns for a GPU aggregation of {}", columns.size(), pipes.size());

    const size_t rows = columns.front()->size();
    if (stagedRows() + rows > max_batch_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch of more than {} rows", max_batch_rows);

    for (size_t i = 0; i < columns.size(); ++i)
        pipes[i].stage(*columns[i]);
}

std::optional<GPUDeviceBatch> GPUBatchUploader::send()
{
    const size_t rows = stagedRows();
    if (rows == 0)
        return std::nullopt;

    GPUDeviceBatch batch;
    batch.upload_stream = stream;
    batch.num_rows = rows;
    batch.buffers.reserve(pipes.size());
    batch.views.reserve(pipes.size());
    for (auto & pipe : pipes)
    {
        pipe.sendWithoutWaiting();
        batch.buffers.push_back(pipe.takeDeviceColumn());
        batch.views.push_back(std::visit([](const auto & column) { return column.view(); }, batch.buffers.back()));
        if (batch.views.back().rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch of {} rows with a column of {}", rows, batch.views.back().rows());
    }
    checkCuda(cudaEventRecord(batch.ready.get(), stream->get()), "Cannot mark a point in an upload stream");
    return batch;
}

GPUGroupingLane::GPUGroupingLane(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    std::optional<CompiledGPUExpression> where_,
    std::shared_ptr<GPUGroupingDecision> decision_)
    : group_keys(groupByKeysOrThrow(key_types))
    , values(valueTypesOrThrow(value_types))
    , aggregates(aggregatesOrThrow(values.size(), std::move(aggregates_)))
    , where(std::move(where_))
    , decision(std::move(decision_))
    , stream(createStream())
    , group_by(createCudfGroupBy(group_keys, values, aggregates, cuda::stream_ref{stream.get()}))
    , kept(cuda::stream_ref{stream.get()})
{
    if (where && where->expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` of a GPU aggregation that is not a predicate");
}

void GPUGroupingLane::add(GPUDeviceBatch batch)
{
    const size_t num_keys = group_keys.size();
    const size_t num_values = values.size();
    const size_t num_where_columns = where ? where->columns.size() : 0;
    const std::vector<DeviceColumnView> & columns = batch.columns();
    if (columns.size() != num_keys + num_values + num_where_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU batch of {} columns for a lane of {} keys, {} values and {} filter columns",
            columns.size(),
            num_keys,
            num_values,
            num_where_columns);

    const cuda::stream_ref own{stream.get()};
    batch.moveTo(own);

    Stopwatch watch;
    const std::vector<DeviceColumnView> keys(columns.begin(), columns.begin() + num_keys);
    const std::vector<DeviceColumnView> batch_values(columns.begin() + num_keys, columns.begin() + num_keys + num_values);

    const uint32_t * mask = nullptr;
    if (where)
    {
        const size_t rows = batch.rows();
        const std::vector<GPUExpressionInput> inputs = inputsOf(where->expression, std::span(columns).subspan(num_keys + num_values), rows);
        kept.clear();
        char * const bits = kept.grow(maskBytes(rows));
        evaluateGPUExpression(where->expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, bits, own);
        mask = reinterpret_cast<const uint32_t *>(bits);
    }

    if (decided)
    {
        group_by->addBatch(keys, batch_values, mask);
    }
    else
    {
        /// The lanes reduce their batches alike, so that they merge: as the first batch with rows decided.
        std::unique_lock lock(decision->mutex);
        if (decision->reduce_runs)
        {
            group_by->reduceRuns(*decision->reduce_runs);
            decided = true;
            lock.unlock();
            group_by->addBatch(keys, batch_values, mask);
        }
        else
        {
            group_by->addBatch(keys, batch_values, mask);
            if (group_by->decided())
            {
                decision->reduce_runs = group_by->reducesRuns();
                decided = true;
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

size_t GPUGroupingLane::partition(size_t num_parts)
{
    Stopwatch watch;
    const size_t groups = group_by->partition(num_parts);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return groups;
}

void GPUGroupingLane::absorbPart(const GPUGroupingLane & other, size_t part)
{
    Stopwatch watch;
    group_by->absorbPart(*other.group_by, part);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

std::optional<size_t> GPUGroupingLane::numGroups() const
{
    if (group_by->reducesRuns())
        return std::nullopt;
    return group_by->numGroups();
}

void GPUGroupingLane::reserveGroups(size_t groups)
{
    Stopwatch watch;
    group_by->reserveGroups(groups);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

void GPUGroupingLane::absorb(GPUGroupingLane & other)
{
    Stopwatch watch;
    group_by->absorb(*other.group_by);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

size_t GPUGroupingLane::finalize()
{
    Stopwatch watch;
    num_groups = group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

size_t GPUGroupingLane::filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were filtered before it was finalized");
    if (having.expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `HAVING` of a GPU aggregation that is not a predicate");
    if (*num_groups == 0)
        return 0;

    Stopwatch watch;

    std::vector<DeviceColumnView> group_columns;
    group_columns.reserve(columns.size());
    for (const size_t column : columns)
    {
        if (column < group_keys.size())
            group_columns.push_back(group_by->key(column));
        else if (column - group_keys.size() < aggregates.size())
            group_columns.push_back(group_by->value(column - group_keys.size()));
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "A `HAVING` over column {} of {} keys and {} aggregates", column, group_keys.size(), aggregates.size());
    }

    const cuda::stream_ref own{stream.get()};
    const std::vector<GPUExpressionInput> inputs = inputsOf(having.expression, group_columns, *num_groups);
    kept.clear();
    char * const mask = kept.grow(maskBytes(*num_groups));
    evaluateGPUExpression(having.expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, *num_groups, mask, own);
    num_groups = group_by->keepGroups(reinterpret_cast<const uint32_t *>(mask));
    /// The caller holds the kernel only for this call.
    synchronizeStream(own);

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GPUGroupingLane::copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns)
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

    copyDeviceToHost(from, to, cuda::stream_ref{stream.get()});

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

GPUAccumulator::GPUAccumulator(
    const DataTypes & argument_types, const DataTypes & result_types_, const std::vector<GPUAggregationKind> & aggregations)
    : result_types(result_types_)
    , reduce_stream(createStream())
{
    if (argument_types.size() != result_types.size() || argument_types.size() != aggregations.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation of {} arguments into {} results by {} aggregates",
            argument_types.size(),
            result_types.size(),
            aggregations.size());

    reductions.reserve(aggregations.size());
    for (size_t i = 0; i < aggregations.size(); ++i)
    {
        const GPUElementType element_type = reducibleElementTypeOrThrow(*argument_types[i], *result_types[i], aggregations[i]);
        reductions.push_back(std::make_unique<CudfReduction>(
            element_type, resultTypeFor(element_type, aggregations[i]), aggregations[i], cuda::stream_ref{reduce_stream.get()}));
    }
}

void GPUAccumulator::add(GPUDeviceBatch batch)
{
    const std::vector<DeviceColumnView> & columns = batch.columns();
    if (columns.size() != reductions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch of {} columns for {} reductions", columns.size(), reductions.size());

    batch.moveTo(cuda::stream_ref{reduce_stream.get()});

    Stopwatch watch;
    for (size_t i = 0; i < columns.size(); ++i)
        reductions[i]->addBatch(columns[i]);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

std::vector<Field> GPUAccumulator::finalize()
{
    Stopwatch watch;
    std::vector<Field> results;
    results.reserve(reductions.size());
    for (size_t i = 0; i < reductions.size(); ++i)
    {
        const DeviceColumnView result = reductions[i]->finalize();
        if (result.rows() == 0)
        {
            results.push_back(result_types[i]->getDefault());
            continue;
        }

        MutableColumnPtr column = result_types[i]->createColumn();
        IColumn * const to = column.get();
        copyDeviceToHost(std::span<const DeviceColumnView>(&result, 1), std::span<IColumn * const>(&to, 1), cuda::stream_ref{reduce_stream.get()});
        results.push_back((*column)[0]);
    }
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    return results;
}

}

#endif
