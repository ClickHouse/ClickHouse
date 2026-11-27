#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <limits>
#include <utility>

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

GPUElementType resultTypeFor(GPUElementType element_type, GPUAggregationKind aggregation)
{
    return aggregation == GPUAggregationKind::Sum ? sumResultTypeFor(element_type) : element_type;
}

bool isVariable(GPUElementType type)
{
    return columnKindOf(type) == GPUColumnKind::Variable;
}

std::vector<GPUElementType> typesAt(const std::vector<GPUElementType> & column_types, const std::vector<size_t> & positions)
{
    std::vector<GPUElementType> types;
    types.reserve(positions.size());
    for (const size_t position : positions)
    {
        if (position >= column_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU batch of {} columns", position, column_types.size());
        types.push_back(column_types[position]);
    }
    return types;
}

std::vector<DeviceColumnView> columnsAt(const GPUDeviceBatch & batch, const std::vector<size_t> & positions)
{
    const auto & columns = batch.columns();
    std::vector<DeviceColumnView> result;
    result.reserve(positions.size());
    for (const size_t position : positions)
    {
        if (position >= columns.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU batch of {} columns", position, columns.size());
        result.push_back(columns[position]);
    }
    return result;
}

std::vector<DeviceFixedColumn> fixedColumnsAt(const GPUDeviceBatch & batch, const std::vector<size_t> & positions)
{
    std::vector<DeviceFixedColumn> result;
    result.reserve(positions.size());
    for (const DeviceColumnView & column : columnsAt(batch, positions))
        result.push_back(fixedOrThrow(column));
    return result;
}

std::vector<GPUGroupByAggregate> aggregatesOrThrow(size_t num_values, std::vector<GPUGroupByAggregate> aggregates, bool keyed)
{
    if (keyed && aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with keys and without aggregates");

    for (const auto & aggregate : aggregates)
    {
        if (aggregate.column != GPUGroupByAggregate::no_column && aggregate.column >= num_values)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregate of value {} of {}", aggregate.column, num_values);
    }
    return aggregates;
}

std::vector<GPUComputedArgument> computedArgumentsOrThrow(const std::vector<GPUElementType> & values, std::vector<GPUComputedArgument> computed)
{
    for (const auto & value : computed)
    {
        const GPUExpression & expression = value.expression.expression;
        if (expression.result != GPUExpressionResult::Value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "An argument of a GPU aggregate computed by a predicate");
        if (value.columns.size() != expression.num_columns)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "An argument of a GPU aggregate computed by an expression of {} columns from {}", expression.num_columns, value.columns.size());
        for (const uint32_t column : value.columns)
        {
            if (column >= values.size() || columnKindOf(values[column]) != GPUColumnKind::Fixed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "An argument of a GPU aggregate computed from value {} of {}", column, values.size());
        }
    }
    return computed;
}

}

GPUGroupingLane::GPUGroupingLane(
    const std::vector<GPUElementType> & column_types,
    GPUBatchColumns columns_,
    std::vector<GPUGroupByAggregate> aggregates_,
    std::vector<GPUComputedArgument> computed_,
    std::optional<CompiledGPUExpression> where_,
    size_t num_parts)
    : batch_columns(std::move(columns_))
    , keys(typesAt(column_types, batch_columns.keys))
    , values(typesAt(column_types, batch_columns.values))
    , aggregates(aggregatesOrThrow(values.size() + computed_.size(), std::move(aggregates_), !keys.empty()))
    , computed(computedArgumentsOrThrow(values, std::move(computed_)))
    , where(std::move(where_))
    , lane_stream(createStream())
{
    if (where && where->expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` of a GPU aggregation that is not a predicate");

    const size_t num_where_columns = where ? where->columns.size() : 0;
    if (batch_columns.where.size() != num_where_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A `WHERE` of {} columns over {} columns of a GPU batch", num_where_columns, batch_columns.where.size());
    for (const GPUElementType type : typesAt(column_types, batch_columns.where))
    {
        if (isVariable(type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` on the GPU over a column of varying width");
    }

    if (aggregates.empty())
    {
        if (!computed.empty() || !where)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows filtered on the GPU with computed arguments or without a `WHERE`");
        row_filter = std::make_unique<CudfRowFilter>(values, where->expression, stream());
        return;
    }

    std::vector<GPUElementType> all_values = values;
    computed_values.reserve(computed.size());
    for (const auto & argument : computed)
    {
        all_values.push_back(argument.expression.expression.value_type);
        computed_values.emplace_back(stream());
    }
    group_by = std::make_unique<CudfGroupBy>(keys, all_values, aggregates, num_parts, stream());
}

CudfGroupBy & GPUGroupingLane::groupByOrThrow() const
{
    if (!group_by)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU lane that filters rows");
    return *group_by;
}

void GPUGroupingLane::add(GPUFuture<GPUDeviceBatch> future)
{
    CudfGroupBy & groups = groupByOrThrow();
    const GPUDeviceBatch batch = takeOn(std::move(future), stream());
    const std::vector<DeviceColumnView> key_columns = columnsAt(batch, batch_columns.keys);
    const std::vector<DeviceColumnView> value_columns = columnsAt(batch, batch_columns.values);
    const std::vector<DeviceFixedColumn> filter_columns = fixedColumnsAt(batch, batch_columns.where);
    const size_t rows = batch.rows();
    const GPUExpression * const filter = where ? &where->expression : nullptr;

    Stopwatch watch;
    if (computed.empty())
        groups.addBatch(key_columns, value_columns, filter_columns, filter);
    else
        groups.addBatch(key_columns, withComputedArguments(value_columns, rows), filter_columns, filter);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

std::vector<DeviceColumnView> GPUGroupingLane::withComputedArguments(const std::vector<DeviceColumnView> & value_columns, size_t rows)
{
    std::vector<DeviceColumnView> all_values = value_columns;
    std::vector<GPUExpressionInput> inputs;
    for (size_t i = 0; i < computed.size(); ++i)
    {
        const GPUExpression & expression = computed[i].expression.expression;
        inputs.clear();
        for (const uint32_t column : computed[i].columns)
            inputs.push_back({.data = value_columns[column].fixed.data, .type = value_columns[column].fixed.element_type});

        DeviceBuffer & out = computed_values[i];
        out.clear();
        char * const result = out.grow(rows * sizeOf(expression.value_type));
        evaluateGPUExpression(expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, result, stream());
        all_values.emplace_back(DeviceFixedColumn{.element_type = expression.value_type, .data = result, .rows = rows});
    }
    return all_values;
}

MutableColumns GPUGroupingLane::filterRows(GPUFuture<GPUDeviceBatch> future, const DataTypes & types)
{
    if (!row_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows filtered by a GPU lane that groups them");
    if (types.size() != values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} types of {} columns filtered on the GPU", types.size(), values.size());

    const GPUDeviceBatch batch = takeOn(std::move(future), stream());
    const std::vector<DeviceColumnView> value_columns = columnsAt(batch, batch_columns.values);
    const std::vector<DeviceFixedColumn> where_columns = fixedColumnsAt(batch, batch_columns.where);

    Stopwatch watch;
    MutableColumns kept;
    const size_t rows = row_filter->filter(value_columns, where_columns);
    if (rows != 0)
    {
        std::vector<DeviceColumnView> from;
        std::vector<IColumn *> to;
        kept.reserve(types.size());
        for (size_t i = 0; i < types.size(); ++i)
        {
            from.push_back(row_filter->value(i));
            kept.push_back(types[i]->createColumn());
            to.push_back(kept.back().get());
        }
        copyDeviceToHost(from, to, stream());
    }

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return kept;
}

void GPUGroupingLane::absorbPart(const GPUGroupingLane & other, size_t part)
{
    Stopwatch watch;
    groupByOrThrow().absorbPart(other.groupByOrThrow(), part);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

size_t GPUGroupingLane::finalize()
{
    Stopwatch watch;
    num_groups = groupByOrThrow().finalize();
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

    const cuda::stream_ref own = stream();
    /// The caller holds the kernel only for this call, so the stream is drained before it returns, on an exception too.
    waitOnFailure(own, [&] { num_groups = groupByOrThrow().filterGroups(having.expression, columns); });
    synchronizeStream(own);

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GPUGroupingLane::copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    if (key_columns.size() != keys.size() || aggregate_columns.size() != aggregates.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} aggregate columns for the groups of a GPU aggregation of {} keys and {} aggregates",
            key_columns.size(),
            aggregate_columns.size(),
            keys.size(),
            aggregates.size());

    const size_t rows = *num_groups;
    if (rows == 0)
        return;

    Stopwatch watch;

    const CudfGroupBy & groups = groupByOrThrow();
    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(keys.size() + aggregates.size());
    to.reserve(keys.size() + aggregates.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        const DeviceColumnView key = groups.key(i);
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const DeviceColumnView value = groups.value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of aggregate {} of {} groups", value.rows(), i, rows);
        from.push_back(value);
        to.push_back(aggregate_columns[i].get());
    }

    copyDeviceToHost(from, to, stream());

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

GPUAccumulator::GPUAccumulator(
    const DataTypes & argument_types, const DataTypes & result_types_, const std::vector<GPUAggregationKind> & aggregations)
    : result_types(result_types_)
    , reduce_stream(createStream())
{
    if (!canAggregateOnDevice({}, argument_types, result_types, aggregations))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reduce these aggregates on a GPU");

    reductions.reserve(aggregations.size());
    for (size_t i = 0; i < aggregations.size(); ++i)
    {
        const GPUElementType element_type = *columnTypeOf(*argument_types[i]);
        reductions.push_back(std::make_unique<CudfReduction>(
            element_type, resultTypeFor(element_type, aggregations[i]), aggregations[i], cuda::stream_ref{reduce_stream.get()}));
    }
}

void GPUAccumulator::add(GPUFuture<GPUDeviceBatch> future)
{
    const GPUDeviceBatch batch = takeOn(std::move(future), cuda::stream_ref{reduce_stream.get()});
    const std::vector<DeviceColumnView> & columns = batch.columns();
    if (columns.size() != reductions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch of {} columns for {} reductions", columns.size(), reductions.size());


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
