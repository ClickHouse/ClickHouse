#include <Processors/Transforms/PromQLRangeRateTransform.h>
#include <Processors/Transforms/PromQLColumnHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <bit>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace PromQLRangeRateHelpers
{

namespace
{

constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

}

bool isStaleMarker(const IColumn & values, size_t sample)
{
    return !values.isNullAt(sample) && std::bit_cast<UInt64>(values.getFloat64(sample)) == STALE_NAN_BITS;
}

bool containsStaleMarker(const IColumn & values, size_t begin, size_t end)
{
    for (size_t sample = begin; sample < end; ++sample)
    {
        if (isStaleMarker(values, sample))
            return true;
    }

    return false;
}

MutableColumnPtr filterStaleMarkers(const ColumnArray & samples, size_t row)
{
    const auto * tuples = typeid_cast<const ColumnTuple *>(&samples.getData());
    if (!tuples || tuples->tupleSize() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL stale-marker filter expects Array(Tuple(timestamp, value))");

    const auto & values = *tuples->getColumnPtr(1);
    const size_t begin = row == 0 ? 0 : samples.getOffsets()[row - 1];
    const size_t end = samples.getOffsets()[row];
    auto filtered = ColumnArray::create(samples.getData().cloneEmpty(), ColumnArray::ColumnOffsets::create());
    auto & filtered_array = assert_cast<ColumnArray &>(*filtered);
    for (size_t sample = begin; sample < end; ++sample)
    {
        if (!isStaleMarker(values, sample))
            filtered_array.getData().insertFrom(samples.getData(), sample);
    }
    filtered_array.getOffsets().push_back(filtered_array.getData().size());
    return filtered;
}

}

SharedHeader PromQLRangeRateTransform::transformHeader(const AggregateFunctionPtr & rate_function)
{
    if (!rate_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native rate aggregate function is null");

    auto group_type = std::make_shared<DataTypeUInt64>();
    auto values_type = rate_function->getResultType();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(group_type->createColumn(), group_type, TimeSeriesColumnNames::Group),
        ColumnWithTypeAndName(values_type->createColumn(), values_type, TimeSeriesColumnNames::Values),
    });
}

PromQLRangeRateTransform::PromQLRangeRateTransform(
    SharedHeader input_header,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    PromQLRangeRateGroupSetPtr output_groups_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_)
    : IProcessor({input_header}, {transformHeader(rate_function_)})
    , input(inputs.front())
    , output(outputs.front())
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_block_size(max_output_block_size_)
    , output_groups(output_groups_ ? std::move(output_groups_) : std::make_shared<PromQLRangeRateGroupSet>())
    , rate_place(rate_function ? rate_function->sizeOfData() : 0, rate_function ? rate_function->alignOfData() : 1)
{
    if (!collector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native tags collector is null");
    if (!rate_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native rate aggregate function is null");
    if (rate_function->getName() != "timeSeriesRateToGrid")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate expects timeSeriesRateToGrid, got {}", rate_function->getName());
    if (rate_function->allocatesMemoryInArena())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate requires a state independent of Arena memory");
    if (max_samples_per_series == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate requires a positive per-series sample limit");
    if (max_output_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range rate requires a positive output block size");
    if (!input_header->has(TimeSeriesColumnNames::ID) || !input_header->has(TimeSeriesColumnNames::Bucket))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate expects {} and {} columns, got {}",
            TimeSeriesColumnNames::ID,
            TimeSeriesColumnNames::Bucket,
            input_header->dumpStructure());

    id_position = input_header->getPositionByName(TimeSeriesColumnNames::ID);
    bucket_position = input_header->getPositionByName(TimeSeriesColumnNames::Bucket);

    const auto & rate_arguments = rate_function->getArgumentTypes();
    reads_raw_samples = input_header->has(TimeSeriesColumnNames::Samples);
    if (reads_raw_samples == input_header->has(TimeSeriesColumnNames::TimeSeries))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate expects exactly one of {} and {} columns, got {}",
            TimeSeriesColumnNames::Samples,
            TimeSeriesColumnNames::TimeSeries,
            input_header->dumpStructure());

    const char * samples_column_name
        = reads_raw_samples ? TimeSeriesColumnNames::Samples : TimeSeriesColumnNames::TimeSeries;
    samples_position = input_header->getPositionByName(samples_column_name);
    const auto & samples_type = input_header->getByPosition(samples_position).type;

    if (reads_raw_samples)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(samples_type.get());
        const auto * tuple_type = array_type
            ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get())
            : nullptr;
        if (!tuple_type || tuple_type->getElements().size() != 2 || rate_arguments.size() != 2
            || !rate_arguments[0]->equals(*tuple_type->getElement(0))
            || !rate_arguments[1]->equals(*tuple_type->getElement(1)))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "PromQL native raw range rate expects {} to be Array(Tuple(timestamp, value)) matching two scalar rate arguments, got {}",
                TimeSeriesColumnNames::Samples,
                samples_type->getName());
        if (!raw_min_time_ || !raw_max_time_)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native raw range rate requires exact inclusive time bounds");

        raw_min_time_column = rate_arguments[0]->createColumn();
        raw_min_time_column->insert(*raw_min_time_);
        raw_max_time_column = rate_arguments[0]->createColumn();
        raw_max_time_column->insert(*raw_max_time_);
    }
    else if (rate_arguments.size() != 1 || !rate_arguments.front()->equals(*samples_type))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range rate expects {} to have the sole rate argument type {}, got {}",
            TimeSeriesColumnNames::TimeSeries,
            rate_arguments.empty() ? String{"<missing>"} : rate_arguments.front()->getName(),
            samples_type->getName());
    }

    current_id = input_header->getByPosition(id_position).type->createColumn();
    current_bucket = input_header->getByPosition(bucket_position).type->createColumn();
    last_input_id = input_header->getByPosition(id_position).type->createColumn();
    last_input_bucket = input_header->getByPosition(bucket_position).type->createColumn();
}

PromQLRangeRateTransform::~PromQLRangeRateTransform()
{
    destroyRateState();
}

IProcessor::Status PromQLRangeRateTransform::prepare()
{
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (current_output_chunk)
    {
        output.push(std::move(current_output_chunk));
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        if (has_current_series)
        {
            finishing_input = true;
            return Status::Ready;
        }

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_input_chunk = input.pull();
    current_input_row = 0;
    full_groups.clear();
    full_groups_ready = false;
    has_input = true;
    return Status::Ready;
}

void PromQLRangeRateTransform::work()
{
    MutableColumnPtr group_column = ColumnUInt64::create();
    MutableColumnPtr values_column = rate_function->getResultType()->createColumn();

    if (finishing_input)
    {
        finishSeries(group_column, values_column);
        finishing_input = false;
        current_output_chunk = Chunk(Columns{std::move(group_column), std::move(values_column)}, 1);
        return;
    }

    const auto & columns = current_input_chunk.getColumns();
    const auto & id_column = *columns[id_position];
    const auto & bucket_column = *columns[bucket_position];
    const auto samples_column
        = columns[samples_position]->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
    const auto * samples_array = typeid_cast<const ColumnArray *>(samples_column.get());
    if (!samples_array)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native range rate received incompatible {} column {}",
            reads_raw_samples ? TimeSeriesColumnNames::Samples : TimeSeriesColumnNames::TimeSeries,
            samples_column->getName());

    const ColumnTuple * raw_samples_tuple = nullptr;
    if (reads_raw_samples)
    {
        raw_samples_tuple = typeid_cast<const ColumnTuple *>(&samples_array->getData());
        if (!raw_samples_tuple || raw_samples_tuple->tupleSize() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native raw range rate received incompatible nested samples column");
    }

    if (!full_groups_ready)
    {
        collector->getGroupByID(columns[id_position], full_groups);
        full_groups_ready = true;
    }

    const IColumn * sliced_rate_arguments[] = {samples_column.get()};
    const IColumn * raw_rate_arguments[] = {
        raw_samples_tuple ? raw_samples_tuple->getColumnPtr(0).get() : nullptr,
        raw_samples_tuple ? raw_samples_tuple->getColumnPtr(1).get() : nullptr,
    };
    while (current_input_row < current_input_chunk.getNumRows())
    {
        const size_t row = current_input_row;
        checkAndRememberInputOrder(id_column, bucket_column, row);

        const size_t row_begin = row == 0 ? 0 : samples_array->getOffsets()[row - 1];
        const size_t row_end = samples_array->getOffsets()[row];
        size_t slice_begin = row_begin;
        size_t slice_end = row_end;
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

        if (has_current_series)
        {
            const int id_order = id_column.compareAt(row, 0, *current_id, 1);
            if (id_order > 0)
            {
                finishSeries(group_column, values_column);
                if (group_column->size() == max_output_block_size)
                {
                    current_output_chunk = Chunk(
                        Columns{std::move(group_column), std::move(values_column)}, max_output_block_size);
                    return;
                }
            }
            else
                updateCurrentBucket(bucket_column, row);
        }

        const size_t row_samples = slice_end - slice_begin;
        if (row_samples == 0)
        {
            ++current_input_row;
            continue;
        }

        if (!has_current_series)
            startSeries(id_column, bucket_column, row, full_groups[row]);

        if (row_samples > max_samples_per_series - current_series_samples)
            throw Exception(
                ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                "PromQL native range rate exceeded its limit of {} samples for one physical series",
                max_samples_per_series);

        if (reads_raw_samples)
        {
            PromQLRangeRateHelpers::forEachNonStaleRange(*raw_rate_arguments[1], slice_begin, slice_end, [&](size_t begin, size_t end)
            {
                rate_function->addBatchSinglePlace(begin, end, rate_place.data(), raw_rate_arguments, nullptr);
            });
        }
        else
        {
            const auto * tuple_samples = typeid_cast<const ColumnTuple *>(&samples_array->getData());
            if (!tuple_samples || tuple_samples->tupleSize() != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native sliced range rate expects Array(Tuple(timestamp, value))");

            const size_t value_begin = row == 0 ? 0 : samples_array->getOffsets()[row - 1];
            const size_t value_end = samples_array->getOffsets()[row];
            const auto & values = *tuple_samples->getColumnPtr(1);
            if (PromQLRangeRateHelpers::containsStaleMarker(values, value_begin, value_end))
            {
                auto filtered_samples = PromQLRangeRateHelpers::filterStaleMarkers(*samples_array, row);
                const IColumn * filtered_rate_arguments[] = {filtered_samples.get()};
                rate_function->add(rate_place.data(), filtered_rate_arguments, 0, nullptr);
            }
            else
                rate_function->add(rate_place.data(), sliced_rate_arguments, row, nullptr);
        }
        current_series_samples += row_samples;
        ++current_input_row;
    }

    current_input_chunk.clear();
    full_groups.clear();
    full_groups_ready = false;
    has_input = false;

    if (!group_column->empty())
    {
        const size_t output_rows = group_column->size();
        current_output_chunk = Chunk(Columns{std::move(group_column), std::move(values_column)}, output_rows);
    }
}

void PromQLRangeRateTransform::checkAndRememberInputOrder(
    const IColumn & id_column, const IColumn & bucket_column, size_t row)
{
    if (!last_input_id->empty())
    {
        const int id_order = id_column.compareAt(row, 0, *last_input_id, 1);
        if (id_order < 0)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native range rate input is not ordered by {}",
                TimeSeriesColumnNames::ID);
        if (id_order == 0 && bucket_column.compareAt(row, 0, *last_input_bucket, 1) < 0)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native range rate input is not ordered by ({}, {})",
                TimeSeriesColumnNames::ID,
                TimeSeriesColumnNames::Bucket);

        last_input_id->popBack(1);
        last_input_bucket->popBack(1);
    }

    insertPromQLColumnValue(*last_input_id, id_column, row);
    insertPromQLColumnValue(*last_input_bucket, bucket_column, row);
}

void PromQLRangeRateTransform::startSeries(
    const IColumn & id_column, const IColumn & bucket_column, size_t row, Group full_group)
{
    chassert(!has_current_series);
    chassert(!rate_state_created);
    chassert(current_id->empty());
    chassert(current_bucket->empty());

    insertPromQLColumnValue(*current_id, id_column, row);
    insertPromQLColumnValue(*current_bucket, bucket_column, row);
    current_output_group = collector->removeTag(full_group, TimeSeriesTagNames::MetricName);
    if (!output_groups->tryRegister(current_output_group))
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Multiple series have the same tags {}, duplicate series in the same result set are not allowed",
            ContextTimeSeriesTagsCollector::toString(collector->getTagsByGroup(current_output_group)));

    rate_function->create(rate_place.data());
    rate_state_created = true;
    has_current_series = true;
    current_series_samples = 0;
}

void PromQLRangeRateTransform::finishSeries(MutableColumnPtr & group_column, MutableColumnPtr & values_column)
{
    if (!has_current_series)
        return;

    chassert(rate_state_created);
    group_column->insert(Field{current_output_group});
    rate_function->insertResultInto(rate_place.data(), *values_column, nullptr);
    destroyRateState();
    current_id->popBack(1);
    current_bucket->popBack(1);
    current_series_samples = 0;
}

void PromQLRangeRateTransform::updateCurrentBucket(const IColumn & bucket_column, size_t row)
{
    chassert(current_bucket->size() == 1);
    current_bucket->popBack(1);
    insertPromQLColumnValue(*current_bucket, bucket_column, row);
}

void PromQLRangeRateTransform::destroyRateState() noexcept
{
    if (!rate_state_created)
        return;

    rate_function->destroy(rate_place.data());
    rate_state_created = false;
    has_current_series = false;
}

}
