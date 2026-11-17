#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>

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
constexpr size_t num_slots = 2;

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
            ? std::min(batch_rows * sizeOf(column_type), GPUAccumulator::max_stage_bytes)
            : GPUAccumulator::max_stage_bytes;
        pipes.emplace_back(*type, stage_bytes, stream);
    }
    return pipes;
}

}

GPUBatchPipeline::GPUBatchPipeline(const DataTypes & types, size_t batch_rows, Process process_, cuda::stream_ref stream_)
    : process(std::move(process_))
    , stream(stream_)
    , work(num_slots)
    , free_slots(num_slots)
{
    if (types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation of no columns");

    slots.resize(num_slots);
    for (size_t i = 0; i < num_slots; ++i)
    {
        slots[i].pipes = pipesFor(types, batch_rows, cuda::stream_ref{slots[i].upload.get()});
        if (!free_slots.push(i))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot hand a slot of a GPU aggregation over");
    }

    thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup()]
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_AGGREGATION);
        run();
    });
}

GPUBatchPipeline::~GPUBatchPipeline()
{
    if (!thread.joinable())
        return;

    try
    {
        work.clearAndFinish();
        free_slots.clearAndFinish();
        thread.join();
        checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for a GPU aggregation");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void GPUBatchPipeline::add(const Columns & columns)
{
    if (!current)
    {
        size_t slot_index = 0;
        if (!free_slots.pop(slot_index))
            throwEnded();

        Slot & slot = slots[slot_index];
        /// The batch before in the slot may still be read on the device.
        checkCuda(
            cudaStreamWaitEvent(slot.upload.get(), slot.processed.get(), 0), "Cannot make an upload stream wait for a GPU aggregation");
        for (auto & pipe : slot.pipes)
            pipe.reset();
        current = slot_index;
    }

    Slot & slot = slots[*current];
    if (columns.size() != slot.pipes.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of {} columns for a GPU aggregation of {}", columns.size(), slot.pipes.size());

    for (size_t i = 0; i < columns.size(); ++i)
        slot.pipes[i].stage(*columns[i]);
}

size_t GPUBatchPipeline::stagedRows() const
{
    return current ? slots[*current].pipes.front().stagedRows() : 0;
}

size_t GPUBatchPipeline::stagedVariableBytes() const
{
    if (!current)
        return 0;

    size_t bytes = 0;
    for (const auto & pipe : slots[*current].pipes)
    {
        if (columnKindOf(pipe.type()) == GPUColumnKind::Variable)
            bytes += pipe.stagedBytes();
    }
    return bytes;
}

void GPUBatchPipeline::send()
{
    if (!current)
        return;

    const size_t slot_index = *current;
    current.reset();

    Slot & slot = slots[slot_index];
    for (auto & pipe : slot.pipes)
        pipe.sendWithoutWaiting();
    checkCuda(cudaEventRecord(slot.uploaded.get(), slot.upload.get()), "Cannot mark a point in an upload stream");

    if (!work.push(slot_index))
        throwEnded();
}

void GPUBatchPipeline::finish()
{
    send();
    work.finish();
    thread.join();
    rethrowError();
    checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for a GPU aggregation");
}

void GPUBatchPipeline::run()
{
    try
    {
        size_t slot_index = 0;
        while (work.pop(slot_index))
        {
            Slot & slot = slots[slot_index];

            std::vector<DeviceColumnView> columns;
            columns.reserve(slot.pipes.size());
            for (const auto & pipe : slot.pipes)
                columns.push_back(pipe.deviceColumn().view());

            const size_t rows = columns.front().rows();
            if (rows != 0)
            {
                Stopwatch watch;
                checkCuda(cudaStreamWaitEvent(stream.get(), slot.uploaded.get(), 0), "Cannot make a GPU aggregation wait for its upload");
                process(columns);
                ProfileEvents::increment(ProfileEvents::GPUAggregationRows, rows);
                ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
                ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
            }

            /// Recorded before the slot is given back, so that the stream of the next upload into it waits for this.
            checkCuda(cudaEventRecord(slot.processed.get(), stream.get()), "Cannot mark a point in a GPU aggregation");
            if (!free_slots.push(slot_index))
                return;
        }
    }
    catch (...)
    {
        fail(std::current_exception());
    }
}

void GPUBatchPipeline::fail(std::exception_ptr exception) noexcept
{
    {
        std::lock_guard lock(error_mutex);
        if (!error)
            error = exception;
    }
    work.clearAndFinish();
    free_slots.clearAndFinish();
}

void GPUBatchPipeline::rethrowError()
{
    std::lock_guard lock(error_mutex);
    if (error)
        std::rethrow_exception(error);
}

void GPUBatchPipeline::throwEnded()
{
    rethrowError();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation was used after it ended");
}


namespace
{

/// The result of a `sum` is wider than what it sums; a `min` or a `max` is of the type it takes.
GPUElementType resultTypeFor(GPUElementType element_type, GPUAggregationKind aggregation)
{
    return aggregation == GPUAggregationKind::Sum ? sumResultTypeFor(element_type) : element_type;
}

}

GPUAccumulator::GPUAccumulator(
    const DataTypes & argument_types,
    const DataTypes & result_types_,
    const std::vector<GPUAggregationKind> & aggregations,
    size_t batch_bytes)
    : result_types(result_types_)
    , reduce_stream(createStream())
    , batch_rows(batchRowsFor(batch_bytes, argument_types))
    , pipeline(
          argument_types,
          batch_rows,
          [this](const std::vector<DeviceColumnView> & columns)
          {
              for (size_t i = 0; i < columns.size(); ++i)
                  reductions[i]->addBatch(columns[i]);
          },
          cuda::stream_ref{reduce_stream.get()})
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

void GPUAccumulator::add(const Columns & columns)
{
    const size_t num_rows = columns.front()->size();
    if (num_rows == 0)
        return;

    if (pipeline.stagedRows() + num_rows > max_batch_rows)
        pipeline.send();

    pipeline.add(columns);

    if (pipeline.stagedRows() >= batch_rows || pipeline.stagedVariableBytes() >= max_stage_bytes)
        pipeline.send();
}

std::vector<Field> GPUAccumulator::finalize()
{
    pipeline.finish();

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

DataTypes concatenated(const DataTypes & first, const DataTypes & second)
{
    DataTypes types = first;
    types.insert(types.end(), second.begin(), second.end());
    return types;
}

DataTypes typesOf(const std::optional<CompiledGPUExpression> & expression)
{
    DataTypes types;
    if (expression)
    {
        for (const auto & column : expression->columns)
            types.push_back(column.type);
    }
    return types;
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

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    size_t batch_bytes,
    std::optional<CompiledGPUExpression> where_)
    : group_keys(groupByKeysOrThrow(key_types))
    , values(valueTypesOrThrow(value_types))
    , aggregates(aggregatesOrThrow(values.size(), std::move(aggregates_)))
    , batch_rows(batchRowsFor(batch_bytes, concatenated(concatenated(key_types, value_types), typesOf(where_))))
    , where(std::move(where_))
    , group_stream(createStream())
    , group_by(createCudfGroupBy(group_keys, values, aggregates, cuda::stream_ref{group_stream.get()}))
    , kept(cuda::stream_ref{group_stream.get()})
    , pipeline(
          concatenated(concatenated(key_types, value_types), typesOf(where)),
          batch_rows,
          [this](const std::vector<DeviceColumnView> & columns)
          {
              const size_t num_keys = group_keys.size();
              const size_t num_values = values.size();
              const std::vector<DeviceColumnView> keys(columns.begin(), columns.begin() + num_keys);
              const std::vector<DeviceColumnView> batch_values(columns.begin() + num_keys, columns.begin() + num_keys + num_values);
              if (!where)
              {
                  group_by->addBatch(keys, batch_values);
                  return;
              }

              const size_t rows = batch_values.front().rows();
              const std::vector<GPUExpressionInput> inputs
                  = inputsOf(where->expression, std::span(columns).subspan(num_keys + num_values), rows);
              kept.clear();
              char * const mask = kept.grow(maskBytes(rows));
              evaluateGPUExpression(
                  where->expression,
                  {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())},
                  rows,
                  mask,
                  cuda::stream_ref{group_stream.get()});
              group_by->addBatch(keys, batch_values, reinterpret_cast<const uint32_t *>(mask));
          },
          cuda::stream_ref{group_stream.get()})
{
    if (where && where->expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` of a GPU aggregation that is not a predicate");
}

void GroupByGPUAccumulator::add(const Columns & key_columns, const Columns & value_columns, const Columns & where_columns)
{
    const size_t num_where_columns = where ? where->columns.size() : 0;
    if (key_columns.size() != group_keys.size() || value_columns.size() != values.size() || where_columns.size() != num_where_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A block of {} key, {} value and {} filter columns for a GPU aggregation over {}, {} and {}",
            key_columns.size(),
            value_columns.size(),
            where_columns.size(),
            group_keys.size(),
            values.size(),
            num_where_columns);

    const size_t num_rows = value_columns.front()->size();
    if (num_rows == 0)
        return;

    if (pipeline.stagedRows() + num_rows > max_batch_rows)
        pipeline.send();

    Columns columns = key_columns;
    columns.insert(columns.end(), value_columns.begin(), value_columns.end());
    columns.insert(columns.end(), where_columns.begin(), where_columns.end());
    pipeline.add(columns);

    if (pipeline.stagedRows() >= batch_rows || pipeline.stagedVariableBytes() >= GPUAccumulator::max_stage_bytes)
        pipeline.send();
}

size_t GroupByGPUAccumulator::finalize()
{
    pipeline.finish();

    Stopwatch watch;
    num_groups = group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

size_t GroupByGPUAccumulator::filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns)
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

    const cuda::stream_ref stream{group_stream.get()};
    const std::vector<GPUExpressionInput> inputs = inputsOf(having.expression, group_columns, *num_groups);
    kept.clear();
    char * const mask = kept.grow(maskBytes(*num_groups));
    evaluateGPUExpression(having.expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, *num_groups, mask, stream);
    num_groups = group_by->keepGroups(reinterpret_cast<const uint32_t *>(mask));
    /// The caller holds the kernel only for this call.
    synchronizeStream(stream);

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
