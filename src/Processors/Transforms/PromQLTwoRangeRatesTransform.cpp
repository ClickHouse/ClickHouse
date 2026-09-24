#include <Processors/Transforms/PromQLTwoRangeRatesTransform.h>
#include <Processors/Transforms/PromQLColumnHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>
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

PromQLTwoRangeRatesGroupState::PromQLTwoRangeRatesGroupState(size_t max_join_groups_, size_t max_grid_cells_)
    : max_join_groups(max_join_groups_)
    , max_grid_cells(max_grid_cells_)
{
    pending_groups.reserve(std::min(max_join_groups, size_t{1024}));
}

std::optional<PromQLTwoRangeRatesGroupState::Match> PromQLTwoRangeRatesGroupState::add(
    Group join_group,
    size_t side,
    const String & metric_name,
    MutableColumnPtr & rate_result,
    size_t grid_cells)
{
    std::lock_guard lock(mutex);

    auto * index_it = group_indices.find(join_group);
    if (!index_it)
    {
        if (pending_groups.size() >= max_join_groups)
            throw Exception(
                ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                "PromQL native two-rate transform exceeded its limit of {} vector-matching groups",
                max_join_groups);

        const UInt64 index = pending_groups.size();
        pending_groups.emplace_back();
        pending_groups.back().group = join_group;
        GroupIndexMap::LookupResult inserted_it = nullptr;
        bool inserted = false;
        group_indices.emplace(join_group, inserted_it, inserted);
        if (inserted)
            inserted_it->getMapped() = index;
        if (!inserted)
        {
            pending_groups.pop_back();
            index_it = inserted_it;
        }
        else
            index_it = inserted_it;
    }

    auto & pending = pending_groups[index_it->getMapped()];
    const UInt8 side_mask = static_cast<UInt8>(UInt8{1} << side);
    if (pending.seen_sides & side_mask)
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native two-rate transform found duplicate metric {} for one join group",
            metric_name);

    const bool has_opposite_side = pending.seen_sides & static_cast<UInt8>(side_mask ^ UInt8{3});
    if (has_opposite_side)
    {
        if (!pending.values || pending.grid_cells > buffered_grid_cells)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native two-rate transform lost a pending rate grid");

        const size_t retained_grid_cells = buffered_grid_cells - pending.grid_cells;
        if (grid_cells > max_grid_cells || retained_grid_cells > max_grid_cells - grid_cells)
            throw Exception(
                ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                "PromQL native two-rate transform exceeded its limit of {} buffered grid cells",
                max_grid_cells);

        Match match{join_group, std::move(pending.values)};
        buffered_grid_cells -= pending.grid_cells;
        pending.grid_cells = 0;
        pending.seen_sides = UInt8{3};
        return match;
    }

    if (grid_cells > max_grid_cells || buffered_grid_cells > max_grid_cells - grid_cells)
        throw Exception(
            ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "PromQL native two-rate transform exceeded its limit of {} buffered grid cells",
            max_grid_cells);

    pending.values = std::move(rate_result);
    pending.grid_cells = grid_cells;
    pending.seen_sides |= side_mask;
    buffered_grid_cells += grid_cells;
    return std::nullopt;
}

PromQLTwoRangeRatesFusionConfig::PromQLTwoRangeRatesFusionConfig(
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    String first_metric_name_,
    String second_metric_name_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    size_t max_join_groups_,
    size_t max_grid_cells_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_,
    SharedHeader output_header_)
    : collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , first_metric_name(std::move(first_metric_name_))
    , second_metric_name(std::move(second_metric_name_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_block_size(max_output_block_size_)
    , max_join_groups(max_join_groups_)
    , max_grid_cells(max_grid_cells_)
    , raw_min_time(std::move(raw_min_time_))
    , raw_max_time(std::move(raw_max_time_))
    , output_header(std::move(output_header_))
{
    if (!collector || !rate_function || !output_header)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL fused two-rate execution contract is incomplete");
    if (first_metric_name.empty() || second_metric_name.empty() || first_metric_name == second_metric_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL fused two-rate execution requires two different non-empty metric names");
    if (max_samples_per_series == 0 || max_output_block_size == 0 || max_join_groups == 0 || max_grid_cells == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL fused two-rate execution requires positive resource limits");
}

PromQLTwoRangeRatesSeriesMatcher::PromQLTwoRangeRatesSeriesMatcher(
    CollectorPtr collector_,
    String first_metric_name_,
    String second_metric_name_,
    PromQLTwoRangeRatesGroupStatePtr group_state_)
    : collector(std::move(collector_))
    , first_metric_name(std::move(first_metric_name_))
    , second_metric_name(std::move(second_metric_name_))
    , group_state(std::move(group_state_))
{
    if (!collector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate tags collector is null");
    if (first_metric_name.empty() || second_metric_name.empty() || first_metric_name == second_metric_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate transform requires two different non-empty metric names");
    if (!group_state)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate matching state is null");
}

void PromQLTwoRangeRatesSeriesMatcher::addFinishedSeries(
    Group full_group,
    MutableColumnPtr & rate_result,
    MutableColumnPtr & group_column,
    MutableColumnPtr & values_column) const
{
    const String metric_name = collector->extractTag(full_group, TimeSeriesTagNames::MetricName);
    size_t side = 0;
    if (metric_name == first_metric_name)
        side = 0;
    else if (metric_name == second_metric_name)
        side = 1;
    else
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native two-rate transform received unexpected metric name {}",
            metric_name.empty() ? String{"<missing>"} : metric_name);

    const Group join_group = collector->removeTag(full_group, TimeSeriesTagNames::MetricName);
    const auto * result_array = typeid_cast<const ColumnArray *>(rate_result.get());
    if (!result_array || result_array->size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native two-rate transform produced an invalid one-row rate result");
    const size_t grid_cells = result_array->getSize(0);
    auto match = group_state->add(join_group, side, metric_name, rate_result, grid_cells);
    if (!match)
        return;

    appendAddedGrid(*match->values, 0, *rate_result, 0, values_column);
    group_column->insert(Field{match->group});
}

void PromQLTwoRangeRatesSeriesMatcher::appendAddedGrid(
    const IColumn & first_column,
    size_t first_row,
    const IColumn & second_column,
    size_t second_row,
    MutableColumnPtr & output_column)
{
    const auto * first_array = typeid_cast<const ColumnArray *>(&first_column);
    const auto * second_array = typeid_cast<const ColumnArray *>(&second_column);
    auto * output_array = typeid_cast<ColumnArray *>(output_column.get());
    if (!first_array || !second_array || !output_array || first_row >= first_array->size() || second_row >= second_array->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native two-rate transform received an invalid rate grid column");

    const auto * first_nullable = typeid_cast<const ColumnNullable *>(&first_array->getData());
    const auto * second_nullable = typeid_cast<const ColumnNullable *>(&second_array->getData());
    auto * output_nullable = typeid_cast<ColumnNullable *>(&output_array->getData());
    if (!first_nullable || !second_nullable || !output_nullable)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native two-rate transform requires nullable rate grid values");

    const auto * first_values = typeid_cast<const ColumnFloat64 *>(&first_nullable->getNestedColumn());
    const auto * second_values = typeid_cast<const ColumnFloat64 *>(&second_nullable->getNestedColumn());
    auto * output_values = typeid_cast<ColumnFloat64 *>(&output_nullable->getNestedColumn());
    if (!first_values || !second_values || !output_values)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native two-rate transform requires Float64 rate grid values");

    const auto & first_offsets = first_array->getOffsets();
    const auto & second_offsets = second_array->getOffsets();
    const size_t first_begin = first_row == 0 ? 0 : first_offsets[first_row - 1];
    const size_t second_begin = second_row == 0 ? 0 : second_offsets[second_row - 1];
    const size_t first_size = first_offsets[first_row] - first_begin;
    const size_t second_size = second_offsets[second_row] - second_begin;
    if (first_size != second_size)
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native two-rate transform received rate grids with different sizes: {} and {}",
            first_size,
            second_size);

    const auto & first_null_map = first_nullable->getNullMapData();
    const auto & second_null_map = second_nullable->getNullMapData();
    auto & output_null_map = output_nullable->getNullMapData();
    auto & output_offsets = output_array->getOffsets();
    auto & output_data = output_values->getData();
    const auto & first_data = first_values->getData();
    const auto & second_data = second_values->getData();

    const size_t previous_offset = output_offsets.empty() ? 0 : output_offsets.back();
    output_data.resize(previous_offset + first_size);
    output_null_map.resize(previous_offset + first_size);

    for (size_t i = 0; i < first_size; ++i)
    {
        const size_t first_index = first_begin + i;
        const size_t second_index = second_begin + i;
        const size_t output_index = previous_offset + i;
        const bool is_null = first_null_map[first_index] || second_null_map[second_index];
        output_data[output_index] = is_null ? 0 : first_data[first_index] + second_data[second_index];
        output_null_map[output_index] = is_null;
    }

    output_offsets.push_back(previous_offset + first_size);
}

SharedHeader PromQLTwoRangeRatesTransform::transformHeader(const AggregateFunctionPtr & rate_function)
{
    if (!rate_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate aggregate function is null");

    auto group_type = std::make_shared<DataTypeUInt64>();
    auto values_type = rate_function->getResultType();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(group_type->createColumn(), group_type, TimeSeriesColumnNames::Group),
        ColumnWithTypeAndName(values_type->createColumn(), values_type, TimeSeriesColumnNames::Values),
    });
}

PromQLTwoRangeRatesTransform::PromQLTwoRangeRatesTransform(
    SharedHeader input_header,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    String first_metric_name_,
    String second_metric_name_,
    size_t max_samples_per_series_,
    size_t max_output_block_size_,
    size_t max_join_groups_,
    size_t max_grid_cells_,
    PromQLTwoRangeRatesGroupStatePtr group_state_,
    std::optional<Field> raw_min_time_,
    std::optional<Field> raw_max_time_)
    : IProcessor({input_header}, {transformHeader(rate_function_)})
    , input(inputs.front())
    , output(outputs.front())
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_block_size(max_output_block_size_)
    , series_matcher(std::make_shared<PromQLTwoRangeRatesSeriesMatcher>(
          collector,
          std::move(first_metric_name_),
          std::move(second_metric_name_),
          group_state_ ? std::move(group_state_) : std::make_shared<PromQLTwoRangeRatesGroupState>(max_join_groups_, max_grid_cells_)))
    , rate_place(rate_function ? rate_function->sizeOfData() : 0, rate_function ? rate_function->alignOfData() : 1)
{
    if (!collector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate tags collector is null");
    if (!rate_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate aggregate function is null");
    if (rate_function->getName() != "timeSeriesRateToGrid")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native two-rate transform expects timeSeriesRateToGrid, got {}",
            rate_function->getName());
    if (rate_function->allocatesMemoryInArena())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate transform requires an arena-independent rate state");
    if (max_samples_per_series == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate transform requires a positive per-series sample limit");
    if (max_output_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate transform requires a positive output block size");
    if (max_join_groups_ == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate transform requires a positive vector-matching group limit");
    if (max_grid_cells_ == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native two-rate transform requires a positive grid-cell limit");
    if (!input_header->has(TimeSeriesColumnNames::ID) || !input_header->has(TimeSeriesColumnNames::Bucket))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native two-rate transform expects {} and {} columns, got {}",
            TimeSeriesColumnNames::ID,
            TimeSeriesColumnNames::Bucket,
            input_header->dumpStructure());

    id_position = input_header->getPositionByName(TimeSeriesColumnNames::ID);
    bucket_position = input_header->getPositionByName(TimeSeriesColumnNames::Bucket);

    const auto & rate_result_type = rate_function->getResultType();
    const auto * result_array_type = typeid_cast<const DataTypeArray *>(rate_result_type.get());
    const auto * result_nullable_type = result_array_type
        ? typeid_cast<const DataTypeNullable *>(result_array_type->getNestedType().get())
        : nullptr;
    if (!result_nullable_type || !typeid_cast<const DataTypeFloat64 *>(result_nullable_type->getNestedType().get()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native two-rate transform expects rate result Array(Nullable(Float64)), got {}",
            rate_result_type->getName());

    const auto & rate_arguments = rate_function->getArgumentTypes();
    reads_raw_samples = input_header->has(TimeSeriesColumnNames::Samples);
    if (reads_raw_samples == input_header->has(TimeSeriesColumnNames::TimeSeries))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native two-rate transform expects exactly one of {} and {} columns, got {}",
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
                "PromQL native raw two-rate transform expects {} to be Array(Tuple(timestamp, value)) matching two scalar rate arguments, got {}",
                TimeSeriesColumnNames::Samples,
                samples_type->getName());
        if (!raw_min_time_ || !raw_max_time_)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native raw two-rate transform requires exact inclusive time bounds");

        raw_min_time_column = rate_arguments[0]->createColumn();
        raw_min_time_column->insert(*raw_min_time_);
        raw_max_time_column = rate_arguments[0]->createColumn();
        raw_max_time_column->insert(*raw_max_time_);
    }
    else if (rate_arguments.size() != 1 || !rate_arguments.front()->equals(*samples_type))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native two-rate transform expects {} to have the sole rate argument type {}, got {}",
            TimeSeriesColumnNames::TimeSeries,
            rate_arguments.empty() ? String{"<missing>"} : rate_arguments.front()->getName(),
            samples_type->getName());
    }

    current_id = input_header->getByPosition(id_position).type->createColumn();
    last_input_id = input_header->getByPosition(id_position).type->createColumn();
    last_input_bucket = input_header->getByPosition(bucket_position).type->createColumn();
    rate_result = rate_function->getResultType()->createColumn();
}

PromQLTwoRangeRatesTransform::~PromQLTwoRangeRatesTransform()
{
    destroyRateState();
}

IProcessor::Status PromQLTwoRangeRatesTransform::prepare()
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

void PromQLTwoRangeRatesTransform::work()
{
    MutableColumnPtr group_column = ColumnUInt64::create();
    MutableColumnPtr values_column = rate_function->getResultType()->createColumn();

    if (finishing_input)
    {
        finishSeries(group_column, values_column);
        finishing_input = false;
        if (!group_column->empty())
        {
            const size_t output_rows = group_column->size();
            current_output_chunk = Chunk(Columns{std::move(group_column), std::move(values_column)}, output_rows);
        }
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
            "PromQL native two-rate transform received incompatible {} column {}",
            reads_raw_samples ? TimeSeriesColumnNames::Samples : TimeSeriesColumnNames::TimeSeries,
            samples_column->getName());

    const ColumnTuple * raw_samples_tuple = nullptr;
    if (reads_raw_samples)
    {
        raw_samples_tuple = typeid_cast<const ColumnTuple *>(&samples_array->getData());
        if (!raw_samples_tuple || raw_samples_tuple->tupleSize() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native raw two-rate transform received incompatible samples column");
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
                    const size_t output_rows = group_column->size();
                    current_output_chunk = Chunk(Columns{std::move(group_column), std::move(values_column)}, output_rows);
                    return;
                }
            }
        }

        const size_t row_samples = slice_end - slice_begin;
        if (row_samples == 0)
        {
            ++current_input_row;
            continue;
        }

        if (!has_current_series)
            startSeries(id_column, row, full_groups[row]);

        if (row_samples > max_samples_per_series - current_series_samples)
            throw Exception(
                ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                "PromQL native two-rate transform exceeded its limit of {} samples for one physical series",
                max_samples_per_series);

        if (reads_raw_samples)
            rate_function->addBatchSinglePlace(slice_begin, slice_end, rate_place.data(), raw_rate_arguments, nullptr);
        else
            rate_function->add(rate_place.data(), sliced_rate_arguments, row, nullptr);
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

void PromQLTwoRangeRatesTransform::checkAndRememberInputOrder(
    const IColumn & id_column, const IColumn & bucket_column, size_t row)
{
    if (!last_input_id->empty())
    {
        const int id_order = id_column.compareAt(row, 0, *last_input_id, 1);
        if (id_order < 0)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native two-rate transform input is not ordered by {}",
                TimeSeriesColumnNames::ID);
        if (id_order == 0 && bucket_column.compareAt(row, 0, *last_input_bucket, 1) < 0)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native two-rate transform input is not ordered by ({}, {})",
                TimeSeriesColumnNames::ID,
                TimeSeriesColumnNames::Bucket);

        last_input_id->popBack(1);
        last_input_bucket->popBack(1);
    }

    insertPromQLColumnValue(*last_input_id, id_column, row);
    insertPromQLColumnValue(*last_input_bucket, bucket_column, row);
}

void PromQLTwoRangeRatesTransform::startSeries(const IColumn & id_column, size_t row, Group full_group)
{
    chassert(!has_current_series);
    chassert(!rate_state_created);
    chassert(current_id->empty());

    insertPromQLColumnValue(*current_id, id_column, row);
    current_full_group = full_group;
    rate_function->create(rate_place.data());
    rate_state_created = true;
    has_current_series = true;
    current_series_samples = 0;
}

void PromQLTwoRangeRatesTransform::finishSeries(MutableColumnPtr & group_column, MutableColumnPtr & values_column)
{
    if (!has_current_series)
        return;

    chassert(rate_state_created);
    try
    {
        rate_function->insertResultInto(rate_place.data(), *rate_result, nullptr);
        series_matcher->addFinishedSeries(current_full_group, rate_result, group_column, values_column);
    }
    catch (...)
    {
        if (rate_result && !rate_result->empty())
            rate_result->popBack(1);
        destroyRateState();
        current_id->popBack(1);
        current_series_samples = 0;
        throw;
    }

    if (rate_result && !rate_result->empty())
        rate_result->popBack(1);
    destroyRateState();
    current_id->popBack(1);
    current_series_samples = 0;
    if (!rate_result)
        rate_result = rate_function->getResultType()->createColumn();
}

void PromQLTwoRangeRatesTransform::destroyRateState() noexcept
{
    if (!rate_state_created)
        return;

    rate_function->destroy(rate_place.data());
    rate_state_created = false;
    has_current_series = false;
}

}
