#include <Processors/Merges/Algorithms/PromQLRangeRateMergingAlgorithm.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <type_traits>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

bool hasReplicatedColumn(const Chunk & chunk)
{
    if (!chunk)
        return false;

    for (const auto & column : chunk.getColumns())
    {
        if (column->isReplicated())
            return true;
    }

    return false;
}

void validateSimpleAggregateSamplesType(const DataTypePtr & samples_type)
{
    const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(samples_type->getCustomName());
    if (!simple)
        return;

    const auto & argument_types = simple->getArgumentsDataTypes();
    if (simple->getFunctionName() != "timeSeriesGroupArray"
        || !simple->getParameters().empty()
        || argument_types.size() != 1
        || !argument_types.front()->equals(*samples_type)
        || !simple->getFunction()->getResultType()->equals(*samples_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native raw range rate merge expects {} to use SimpleAggregateFunction(timeSeriesGroupArray, Array(...)), got {}",
            TimeSeriesColumnNames::Samples,
            samples_type->getName());
}

}

PromQLRangeRateMergingAlgorithm::PromQLRangeRateMergingAlgorithm(
    SharedHeader header_,
    size_t num_inputs,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_,
    PromQLTwoRangeRatesSeriesMatcherPtr series_matcher_)
    : header(std::move(header_))
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_block_size(max_output_block_size_)
    , samples_position(0)
    , reads_raw_samples(header->has(TimeSeriesColumnNames::Samples))
    , rate_place(rate_function ? rate_function->sizeOfData() : 0, rate_function ? rate_function->alignOfData() : 1)
    , current_inputs(num_inputs)
    , source_states(num_inputs)
    , cursors(num_inputs)
    , output_groups(std::make_shared<PromQLRangeRateGroupSet>())
    , series_matcher(std::move(series_matcher_))
{
    if (!collector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate merge collector is null");
    if (!rate_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate merge aggregate function is null");
    if (rate_function->getName() != "timeSeriesRateToGrid")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate merge expects timeSeriesRateToGrid, got {}",
            rate_function->getName());
    if (rate_function->allocatesMemoryInArena())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate merge requires an arena-independent rate state");
    if (max_samples_per_series == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate merge requires a positive per-series sample limit");
    if (max_output_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate merge requires a positive output block size");
    if (!header->has(TimeSeriesColumnNames::ID) || !header->has(TimeSeriesColumnNames::Bucket))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate merge expects {} and {} columns, got {}",
            TimeSeriesColumnNames::ID,
            TimeSeriesColumnNames::Bucket,
            header->dumpStructure());

    description.emplace_back(TimeSeriesColumnNames::ID, 1, 1);
    description.emplace_back(TimeSeriesColumnNames::Bucket, 1, 1);
    queue_variants = SortQueueVariants(*header, description);
    id_position = header->getPositionByName(TimeSeriesColumnNames::ID);
    bucket_position = header->getPositionByName(TimeSeriesColumnNames::Bucket);

    const char * samples_column_name
        = reads_raw_samples ? TimeSeriesColumnNames::Samples : TimeSeriesColumnNames::TimeSeries;
    if (!header->has(samples_column_name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate merge expects exactly one of {} and {} columns, got {}",
            TimeSeriesColumnNames::Samples,
            TimeSeriesColumnNames::TimeSeries,
            header->dumpStructure());

    samples_position = header->getPositionByName(samples_column_name);
    const auto & samples_type = header->getByPosition(samples_position).type;
    const auto & rate_arguments = rate_function->getArgumentTypes();

    if (header->has(TimeSeriesColumnNames::Samples) == header->has(TimeSeriesColumnNames::TimeSeries))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate merge expects exactly one of {} and {} columns, got {}",
            TimeSeriesColumnNames::Samples,
            TimeSeriesColumnNames::TimeSeries,
            header->dumpStructure());

    if (reads_raw_samples)
    {
        validateSimpleAggregateSamplesType(samples_type);
        const auto * array_type = typeid_cast<const DataTypeArray *>(samples_type.get());
        const auto * tuple_type = array_type
            ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get())
            : nullptr;
        if (!tuple_type || tuple_type->getElements().size() != 2 || rate_arguments.size() != 2
            || !rate_arguments[0]->equals(*tuple_type->getElement(0))
            || !rate_arguments[1]->equals(*tuple_type->getElement(1)))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "PromQL native raw range rate merge expects {} to be Array(Tuple(timestamp, value)) matching two scalar rate arguments, got {}",
                TimeSeriesColumnNames::Samples,
                samples_type->getName());
        if (!raw_min_time_ || !raw_max_time_)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native raw range rate merge requires exact inclusive time bounds");

        streaming_rate = dynamic_cast<const ITimeSeriesRateToGridStreaming *>(rate_function.get());
        if (!streaming_rate)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "PromQL native raw range rate merge requires the exact ordered streaming interface of timeSeriesRateToGrid");

        raw_min_time_column = rate_arguments[0]->createColumn();
        raw_min_time_column->insert(*raw_min_time_);
        raw_max_time_column = rate_arguments[0]->createColumn();
        raw_max_time_column->insert(*raw_max_time_);
    }
    else if (rate_arguments.size() != 1 || !rate_arguments.front()->equals(*samples_type))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate merge expects {} to have the sole rate argument type {}, got {}",
            TimeSeriesColumnNames::TimeSeries,
            rate_arguments.empty() ? String{"<missing>"} : rate_arguments.front()->getName(),
            samples_type->getName());
    }

    current_id = header->getByPosition(id_position).type->createColumn();
    current_bucket = header->getByPosition(bucket_position).type->createColumn();
    last_input_id = header->getByPosition(id_position).type->createColumn();
    last_input_bucket = header->getByPosition(bucket_position).type->createColumn();
    if (series_matcher)
        rate_result = rate_function->getResultType()->createColumn();
    output_group_column = ColumnUInt64::create();
    output_values_column = rate_function->getResultType()->createColumn();
}

void PromQLRangeRateMergingAlgorithm::rejectUnsupportedInput(const Input & input) const
{
    rejectUnsupportedChunk(input.chunk);
    if (input.skip_last_row || input.permutation)
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native range rate merge does not support input row skipping or permutations");
}

void PromQLRangeRateMergingAlgorithm::rejectUnsupportedChunk(const Chunk & chunk) const
{
    if (isVirtualRow(chunk))
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native range rate merge does not support virtual input rows");
    if (hasReplicatedColumn(chunk))
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native range rate merge does not support replicated input columns");
}

void PromQLRangeRateMergingAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != current_inputs.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native range rate merge received {} inputs, expected {}",
            inputs.size(),
            current_inputs.size());

    for (const auto & input : inputs)
        rejectUnsupportedInput(input);

    removeConstAndSparse(inputs);
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        if (current_inputs[source_num].chunk)
        {
            initializeSource(source_num);
            cursors[source_num] = SortCursorImpl(
                *header,
                current_inputs[source_num].chunk.getColumns(),
                current_inputs[source_num].chunk.getNumRows(),
                description,
                source_num);
        }
    }

    queue_variants.callOnBatchVariant([&](auto & queue)
    {
        using QueueType = std::decay_t<decltype(queue)>;
        queue = QueueType(cursors);
    });
}

void PromQLRangeRateMergingAlgorithm::initializeSource(size_t source_num)
{
    auto & source = source_states[source_num];
    source.samples_column.reset();
    source.samples_array = nullptr;
    source.raw_samples_tuple = nullptr;
    source.full_groups.clear();

    const auto & columns = current_inputs[source_num].chunk.getColumns();
    source.samples_column = columns[samples_position]->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
    source.samples_array = typeid_cast<const ColumnArray *>(source.samples_column.get());
    if (!source.samples_array)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native range rate merge received incompatible {} column {}",
            reads_raw_samples ? TimeSeriesColumnNames::Samples : TimeSeriesColumnNames::TimeSeries,
            source.samples_column->getName());

    if (reads_raw_samples)
    {
        source.raw_samples_tuple = typeid_cast<const ColumnTuple *>(&source.samples_array->getData());
        if (!source.raw_samples_tuple || source.raw_samples_tuple->tupleSize() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native raw range rate merge received incompatible nested samples column");
    }

    collector->getGroupByID(columns[id_position], source.full_groups);
    ++merged_stats.blocks;
    merged_stats.bytes += current_inputs[source_num].chunk.bytes();
}

void PromQLRangeRateMergingAlgorithm::consume(Input & input, size_t source_num)
{
    if (source_num >= current_inputs.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native range rate merge received invalid source {}", source_num);

    rejectUnsupportedInput(input);
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    initializeSource(source_num);
    cursors[source_num].reset(
        current_inputs[source_num].chunk.getColumns(),
        *header,
        current_inputs[source_num].chunk.getNumRows());

    queue_variants.callOnBatchVariant([&](auto & queue)
    {
        queue.push(cursors[source_num]);
    });
}

IMergingAlgorithm::Status PromQLRangeRateMergingAlgorithm::merge()
{
    return queue_variants.callOnBatchVariant([&](auto & queue)
    {
        return mergeBatchImpl(queue);
    });
}

template <typename SortingQueue>
IMergingAlgorithm::Status PromQLRangeRateMergingAlgorithm::mergeBatchImpl(SortingQueue & queue)
{
    while (queue.isValid())
    {
        auto [current_ptr, batch_size] = queue.current();
        auto current = *current_ptr;
        const size_t source_num = current.impl->order;
        const size_t first_row = current.impl->getRow();

        size_t consumed_rows = 0;
        for (; consumed_rows < batch_size; ++consumed_rows)
        {
            if (consumeRow(source_num, first_row + consumed_rows, output_group_column, output_values_column))
            {
                if (consumed_rows)
                    queue.next(consumed_rows);

                auto output = Chunk(
                    Columns{std::move(output_group_column), std::move(output_values_column)}, max_output_block_size);
                output_group_column = ColumnUInt64::create();
                output_values_column = rate_function->getResultType()->createColumn();
                return Status(std::move(output), false);
            }
        }

        if (!current.impl->isLast(batch_size))
            queue.next(batch_size);
        else
        {
            queue.removeTop();
            return Status(source_num);
        }
    }

    if (has_current_series)
        finishSeries(output_group_column, output_values_column);

    if (!output_group_column->empty())
    {
        const size_t output_rows = output_group_column->size();
        return Status(
            Chunk(Columns{std::move(output_group_column), std::move(output_values_column)}, output_rows), true);
    }

    return Status(Chunk{}, true);
}

bool PromQLRangeRateMergingAlgorithm::consumeRow(
    size_t source_num,
    size_t row,
    MutableColumnPtr & group_column,
    MutableColumnPtr & values_column)
{
    const auto & columns = current_inputs[source_num].chunk.getColumns();
    const auto & id_column = *columns[id_position];
    const auto & bucket_column = *columns[bucket_position];
    const auto & source = source_states[source_num];
    const auto & samples_array = *source.samples_array;

    checkAndRememberInputOrder(id_column, bucket_column, row);

    if (has_current_series)
    {
        const int id_order = id_column.compareAt(row, 0, *current_id, 1);
        if (id_order > 0)
        {
            finishSeries(group_column, values_column);
            if (group_column->size() == max_output_block_size)
                return true;
        }
        else if (reads_raw_samples && has_current_external_bucket
            && bucket_column.compareAt(row, 0, *current_bucket, 1) > 0)
        {
            finishExternalBucket();
        }
    }

    const size_t row_begin = row == 0 ? 0 : samples_array.getOffsets()[row - 1];
    const size_t row_end = samples_array.getOffsets()[row];
    size_t slice_begin = row_begin;
    size_t slice_end = row_end;

    const IColumn * raw_rate_arguments[] = {
        source.raw_samples_tuple ? source.raw_samples_tuple->getColumnPtr(0).get() : nullptr,
        source.raw_samples_tuple ? source.raw_samples_tuple->getColumnPtr(1).get() : nullptr,
    };

    if (reads_raw_samples)
    {
        const auto & timestamp_column = *raw_rate_arguments[0];
        while (slice_begin < slice_end)
        {
            const size_t middle = slice_begin + (slice_end - slice_begin) / 2;
            if (timestamp_column.compareAt(middle, 0, *raw_min_time_column, 1) < 0)
                slice_begin = middle + 1;
            else
                slice_end = middle;
        }

        slice_end = slice_begin;
        size_t upper = row_end;
        while (slice_end < upper)
        {
            const size_t middle = slice_end + (upper - slice_end) / 2;
            if (timestamp_column.compareAt(middle, 0, *raw_max_time_column, 1) <= 0)
                slice_end = middle + 1;
            else
                upper = middle;
        }
    }

    const size_t row_samples = slice_end - slice_begin;
    if (row_samples == 0)
    {
        ++merged_stats.rows;
        return false;
    }

    if (!has_current_series)
        startSeries(id_column, row, source.full_groups[row]);

    if (row_samples > max_samples_per_series - current_series_samples)
        throw Exception(
            ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "PromQL native range rate merge exceeded its limit of {} samples for one physical series",
            max_samples_per_series);

    if (reads_raw_samples)
    {
        if (!has_current_external_bucket)
        {
            current_bucket->insertFrom(bucket_column, row);
            has_current_external_bucket = true;
        }
        PromQLRangeRateHelpers::forEachNonStaleRange(*raw_rate_arguments[1], slice_begin, slice_end, [&](size_t begin, size_t end)
        {
            streaming_rate->addRawSamples(
                *streaming_rate_state,
                *raw_rate_arguments[0],
                *raw_rate_arguments[1],
                begin,
                end);
        });
    }
    else
    {
        const IColumn * sliced_rate_arguments[] = {source.samples_column.get()};
        const auto * tuple_samples = typeid_cast<const ColumnTuple *>(&samples_array.getData());
        if (!tuple_samples || tuple_samples->tupleSize() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native sliced range rate merge expects Array(Tuple(timestamp, value))");

        const size_t value_begin = row == 0 ? 0 : samples_array.getOffsets()[row - 1];
        const size_t value_end = samples_array.getOffsets()[row];
        const auto & values = *tuple_samples->getColumnPtr(1);
        if (PromQLRangeRateHelpers::containsStaleMarker(values, value_begin, value_end))
        {
            auto filtered_samples = PromQLRangeRateHelpers::filterStaleMarkers(samples_array, row);
            const IColumn * filtered_rate_arguments[] = {filtered_samples.get()};
            rate_function->add(rate_place.data(), filtered_rate_arguments, 0, nullptr);
        }
        else
            rate_function->add(rate_place.data(), sliced_rate_arguments, row, nullptr);
    }

    current_series_samples += row_samples;
    ++merged_stats.rows;
    return false;
}

void PromQLRangeRateMergingAlgorithm::checkAndRememberInputOrder(
    const IColumn & id_column, const IColumn & bucket_column, size_t row)
{
    if (!last_input_id->empty())
    {
        const int id_order = id_column.compareAt(row, 0, *last_input_id, 1);
        if (id_order < 0)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native range rate merge input is not ordered by {}",
                TimeSeriesColumnNames::ID);
        if (id_order == 0 && bucket_column.compareAt(row, 0, *last_input_bucket, 1) < 0)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native range rate merge input is not ordered by ({}, {})",
                TimeSeriesColumnNames::ID,
                TimeSeriesColumnNames::Bucket);

        last_input_id->popBack(1);
        last_input_bucket->popBack(1);
    }

    last_input_id->insertFrom(id_column, row);
    last_input_bucket->insertFrom(bucket_column, row);
}

void PromQLRangeRateMergingAlgorithm::startSeries(
    const IColumn & id_column, size_t row, Group full_group)
{
    chassert(!has_current_series);
    chassert(!rate_state_created);
    chassert(!streaming_rate_state);
    chassert(current_id->empty());
    chassert(current_bucket->empty());

    current_id->insertFrom(id_column, row);
    try
    {
        if (series_matcher)
        {
            current_full_group = full_group;
        }
        else
        {
            current_output_group = collector->removeTag(full_group, TimeSeriesTagNames::MetricName);
            if (!output_groups->tryRegister(current_output_group))
                throw Exception(
                    ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                    "Multiple series have the same tags {}, duplicate series in the same result set are not allowed",
                    ContextTimeSeriesTagsCollector::toString(collector->getTagsByGroup(current_output_group)));
        }

        if (reads_raw_samples)
        {
            streaming_rate_state = streaming_rate->createStreamingState();
            if (!streaming_rate_state)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "timeSeriesRateToGrid returned a null streaming state");
        }
        else
        {
            rate_function->create(rate_place.data());
            rate_state_created = true;
        }
    }
    catch (...)
    {
        if (rate_state_created)
        {
            rate_function->destroy(rate_place.data());
            rate_state_created = false;
        }
        streaming_rate_state.reset();
        current_id->popBack(1);
        throw;
    }

    has_current_series = true;
    current_series_samples = 0;
}

void PromQLRangeRateMergingAlgorithm::finishExternalBucket()
{
    if (!has_current_external_bucket)
        return;

    chassert(reads_raw_samples);
    chassert(streaming_rate);
    chassert(streaming_rate_state);
    chassert(current_bucket->size() == 1);
    streaming_rate->finishExternalBucket(*streaming_rate_state);
    current_bucket->popBack(1);
    has_current_external_bucket = false;
}

void PromQLRangeRateMergingAlgorithm::finishSeries(MutableColumnPtr & group_column, MutableColumnPtr & values_column)
{
    if (!has_current_series)
        return;

    chassert(reads_raw_samples ? static_cast<bool>(streaming_rate_state) : rate_state_created);
    const size_t group_size_before = group_column->size();
    const size_t values_size_before = values_column->size();
    const size_t rate_result_size_before = rate_result ? rate_result->size() : 0;
    try
    {
        if (reads_raw_samples)
            finishExternalBucket();

        if (series_matcher)
        {
            if (reads_raw_samples)
                streaming_rate->insertStreamingResultInto(*streaming_rate_state, *rate_result);
            else
                rate_function->insertResultInto(rate_place.data(), *rate_result, nullptr);
            series_matcher->addFinishedSeries(current_full_group, rate_result, group_column, values_column);
        }
        else
        {
            group_column->insert(Field{current_output_group});
            if (reads_raw_samples)
                streaming_rate->insertStreamingResultInto(*streaming_rate_state, *values_column);
            else
                rate_function->insertResultInto(rate_place.data(), *values_column, nullptr);
        }
    }
    catch (...)
    {
        if (group_column->size() > group_size_before)
            group_column->popBack(group_column->size() - group_size_before);
        if (values_column->size() > values_size_before)
            values_column->popBack(values_column->size() - values_size_before);
        if (rate_result && rate_result->size() > rate_result_size_before)
            rate_result->popBack(rate_result->size() - rate_result_size_before);
        destroyRateState();
        current_id->popBack(1);
        current_series_samples = 0;
        throw;
    }

    if (rate_result && rate_result->size() > rate_result_size_before)
        rate_result->popBack(rate_result->size() - rate_result_size_before);
    destroyRateState();
    current_id->popBack(1);
    current_series_samples = 0;
    if (series_matcher && !rate_result)
        rate_result = rate_function->getResultType()->createColumn();
}

void PromQLRangeRateMergingAlgorithm::destroyRateState() noexcept
{
    if (rate_state_created)
    {
        rate_function->destroy(rate_place.data());
        rate_state_created = false;
    }
    streaming_rate_state.reset();
    if (current_bucket && !current_bucket->empty())
        current_bucket->popBack(current_bucket->size());
    has_current_external_bucket = false;
    has_current_series = false;
}

PromQLRangeRateMergingAlgorithm::~PromQLRangeRateMergingAlgorithm()
{
    destroyRateState();
}

}
