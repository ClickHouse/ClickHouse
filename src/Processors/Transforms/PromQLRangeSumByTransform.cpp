#include <Processors/Transforms/PromQLRangeSumByTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Transforms/PromQLColumnHelpers.h>
#include <Processors/Transforms/PromQLRangeRateTransform.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

SharedHeader PromQLRangeSumByTransform::transformHeader(const AggregateFunctionPtr & sum_function, bool emit_partial_state)
{
    if (!sum_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native sum aggregate function is null");

    auto group_type = std::make_shared<DataTypeUInt64>();
    DataTypePtr values_type = emit_partial_state ? sum_function->getStateType() : sum_function->getResultType();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(group_type->createColumn(), group_type, TimeSeriesColumnNames::Group),
        ColumnWithTypeAndName(values_type->createColumn(), values_type, TimeSeriesColumnNames::Values),
    });
}

PromQLRangeSumByTransform::PromQLRangeSumByTransform(
    SharedHeader input_header,
    CollectorPtr collector_,
    AggregateFunctionPtr rate_function_,
    AggregateFunctionPtr sum_function_,
    Strings labels_to_keep_,
    size_t max_samples_per_series_,
    size_t max_output_groups_,
    size_t max_output_block_size_,
    PromQLGroupLimitPtr group_limit_,
    bool emit_partial_state_)
    : IAccumulatingTransform(input_header, transformHeader(sum_function_, emit_partial_state_))
    , collector(std::move(collector_))
    , rate_function(std::move(rate_function_))
    , sum_function(std::move(sum_function_))
    , labels_to_keep(std::move(labels_to_keep_))
    , max_samples_per_series(max_samples_per_series_)
    , max_output_groups(max_output_groups_)
    , max_output_block_size(max_output_block_size_)
    , group_limit(std::move(group_limit_))
    , emit_partial_state(emit_partial_state_)
    , rate_place(rate_function ? rate_function->sizeOfData() : 0, rate_function ? rate_function->alignOfData() : 1)
    , group_arena(std::make_shared<Arena>())
{
    if (!collector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native tags collector is null");
    if (!rate_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native rate aggregate function is null");
    if (rate_function->getName() != "timeSeriesRateToGrid")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "PromQL native range sum expects timeSeriesRateToGrid, got {}", rate_function->getName());
    if (sum_function->getName() != "sumForEach" && sum_function->getName() != "sumPromQLForEach")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range sum expects sumForEach or sumPromQLForEach, got {}",
            sum_function->getName());
    if (rate_function->allocatesMemoryInArena())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range sum requires a rate state independent of Arena memory");
    if (max_samples_per_series == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range sum requires a positive per-series sample limit");
    if (max_output_groups == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range sum requires a positive output group limit");
    if (max_output_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range sum requires a positive output block size");

    id_position = input_header->getPositionByName(TimeSeriesColumnNames::ID);
    time_series_position = input_header->getPositionByName(TimeSeriesColumnNames::TimeSeries);

    const auto & rate_arguments = rate_function->getArgumentTypes();
    const auto & time_series_type = input_header->getByPosition(time_series_position).type;
    if (rate_arguments.size() != 1 || !rate_arguments.front()->equals(*time_series_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range sum expects {} to have the sole rate argument type {}, got {}",
            TimeSeriesColumnNames::TimeSeries,
            rate_arguments.empty() ? String{"<missing>"} : rate_arguments.front()->getName(),
            time_series_type->getName());

    const auto & sum_arguments = sum_function->getArgumentTypes();
    if (sum_arguments.size() != 1 || !sum_arguments.front()->equals(*rate_function->getResultType()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range sum expects {}({}), got {}",
            sum_function->getName(),
            rate_function->getResultType()->getName(),
            sum_arguments.empty() ? String{"<missing>"} : sum_arguments.front()->getName());

    labels_to_keep.erase(
        std::remove(labels_to_keep.begin(), labels_to_keep.end(), TimeSeriesTagNames::MetricName), labels_to_keep.end());
    std::sort(labels_to_keep.begin(), labels_to_keep.end());
    labels_to_keep.erase(std::unique(labels_to_keep.begin(), labels_to_keep.end()), labels_to_keep.end());

    current_id = input_header->getByPosition(id_position).type->createColumn();
    rate_result = rate_function->getResultType()->createColumn();
}

PromQLRangeSumByTransform::~PromQLRangeSumByTransform()
{
    destroyStates();
}

void PromQLRangeSumByTransform::consume(Chunk chunk)
{
    const auto & columns = chunk.getColumns();
    const auto & id_column = *columns[id_position];
    const auto time_series_column
        = columns[time_series_position]->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
    const auto * time_series_array = typeid_cast<const ColumnArray *>(time_series_column.get());
    if (!time_series_array)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native range sum received incompatible {} column {}",
            TimeSeriesColumnNames::TimeSeries,
            time_series_column->getName());
    const auto * tuple_samples = typeid_cast<const ColumnTuple *>(&time_series_array->getData());
    if (!tuple_samples || tuple_samples->tupleSize() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL native range sum expects Array(Tuple(timestamp, value))");
    const auto & values = *tuple_samples->getColumnPtr(1);
    const IColumn * rate_arguments[] = {time_series_column.get()};

    collector->getGroupByID(columns[id_position], full_groups);
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        if (!has_current_series)
        {
            startSeries(id_column, row, full_groups[row]);
        }
        else
        {
            const int order = id_column.compareAt(row, 0, *current_id, 1);
            if (order < 0)
                throw Exception(
                    ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                    "PromQL native range sum input is not ordered by {}",
                    TimeSeriesColumnNames::ID);
            if (order > 0)
            {
                finishSeries();
                startSeries(id_column, row, full_groups[row]);
            }
        }

        const size_t row_begin = row == 0 ? 0 : time_series_array->getOffsets()[row - 1];
        const size_t row_samples = time_series_array->getOffsets()[row] - row_begin;
        if (row_samples > max_samples_per_series - current_series_samples)
            throw Exception(
                ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                "PromQL native range sum exceeded its limit of {} samples for one physical series",
                max_samples_per_series);

        if (PromQLRangeRateHelpers::containsStaleMarker(values, row_begin, row_begin + row_samples))
        {
            auto filtered_samples = PromQLRangeRateHelpers::filterStaleMarkers(*time_series_array, row);
            const IColumn * filtered_rate_arguments[] = {filtered_samples.get()};
            rate_function->add(rate_place.data(), filtered_rate_arguments, 0, nullptr);
        }
        else
            rate_function->add(rate_place.data(), rate_arguments, row, nullptr);
        current_series_samples += row_samples;
    }
}

void PromQLRangeSumByTransform::startSeries(const IColumn & id_column, size_t row, Group full_group)
{
    chassert(!has_current_series);
    chassert(!rate_state_created);
    chassert(current_id->empty());

    insertPromQLColumnValue(*current_id, id_column, row);
    current_output_group = projectGroup(full_group);
    rate_function->create(rate_place.data());
    rate_state_created = true;
    has_current_series = true;
}

void PromQLRangeSumByTransform::finishSeries()
{
    if (!has_current_series)
        return;

    chassert(rate_state_created);
    chassert(rate_result->empty());
    rate_function->insertResultInto(rate_place.data(), *rate_result, nullptr);

    try
    {
        AggregateDataPtr group_place = getOrCreateGroupState(current_output_group);
        const IColumn * sum_arguments[] = {rate_result.get()};
        sum_function->add(group_place, sum_arguments, 0, group_arena.get());
    }
    catch (...)
    {
        rate_result->popBack(1);
        throw;
    }

    rate_result->popBack(1);
    rate_function->destroy(rate_place.data());
    rate_state_created = false;
    has_current_series = false;
    current_series_samples = 0;
    current_id->popBack(1);
}

PromQLRangeSumByTransform::Group PromQLRangeSumByTransform::projectGroup(Group full_group)
{
    return collector->removeAllTagsExcept(full_group, labels_to_keep);
}

AggregateDataPtr PromQLRangeSumByTransform::getOrCreateGroupState(Group group)
{
    if (auto * it = group_states.find(group); it != group_states.end())
        return it->getMapped();

    if (!group_limit && group_states.size() >= max_output_groups)
        throw Exception(
            ErrorCodes::TOO_MANY_ROWS_OR_BYTES, "PromQL native range sum exceeded its limit of {} output groups", max_output_groups);

    decltype(group_states)::LookupResult it = nullptr;
    bool inserted = false;
    group_states.emplace(group, it, inserted);
    chassert(inserted);

    if (group_limit && !group_limit->tryRegister(group))
    {
        group_states.erase(group);
        throw Exception(
            ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "PromQL native range sum exceeded its query-wide limit of {} output groups",
            max_output_groups);
    }

    try
    {
        AggregateDataPtr new_place = group_arena->alignedAlloc(sum_function->sizeOfData(), sum_function->alignOfData());
        sum_function->create(new_place);
        it->getMapped() = new_place;
    }
    catch (...)
    {
        group_states.erase(group);
        throw;
    }
    return it->getMapped();
}

Chunk PromQLRangeSumByTransform::generate()
{
    if (!generation_started)
    {
        finishSeries();
        generation_started = true;
        generated_groups.reserve(group_states.size());
        for (const auto & entry : group_states)
            generated_groups.push_back(entry.getKey());
        std::sort(generated_groups.begin(), generated_groups.end());
    }

    if (next_generated_group == generated_groups.size())
        return {};

    const size_t output_rows = std::min(max_output_block_size, generated_groups.size() - next_generated_group);

    auto group_column = ColumnUInt64::create();
    DataTypePtr values_type = emit_partial_state ? sum_function->getStateType() : sum_function->getResultType();
    auto values_column = values_type->createColumn();
    group_column->reserve(output_rows);
    values_column->reserve(output_rows);

    /// Transfer only a complete lane output, so a retained chunk cannot pin states
    /// belonging to other output blocks. Multi-block output keeps the owned-copy path.
    const bool transfer_partial_states = emit_partial_state && generated_groups.size() <= max_output_block_size;
    if (transfer_partial_states)
        assert_cast<ColumnAggregateFunction &>(*values_column).addArena(group_arena);

    const size_t output_end = next_generated_group + output_rows;
    for (; next_generated_group < output_end; ++next_generated_group)
    {
        const Group group = generated_groups[next_generated_group];
        group_column->insertValue(group);
        auto * group_state = group_states.find(group);
        auto * const group_place = group_state->getMapped();
        if (emit_partial_state)
        {
            auto & state_column = assert_cast<ColumnAggregateFunction &>(*values_column);
            if (transfer_partial_states)
            {
                /// The column retains the arena before taking state ownership. Its reserved
                /// pointer array makes append non-throwing, followed immediately by disowning.
                state_column.getData().push_back(group_place);
                group_state->getMapped() = nullptr;
            }
            else
            {
                state_column.insertFrom(group_place);
            }
        }
        else
        {
            sum_function->insertResultInto(group_place, *values_column, group_arena.get());
        }
    }

    auto result = Chunk(Columns{std::move(group_column), std::move(values_column)}, output_rows);
    if (next_generated_group == generated_groups.size())
    {
        destroyStates();
        group_arena = std::make_shared<Arena>();
    }
    return result;
}

void PromQLRangeSumByTransform::destroyStates() noexcept
{
    if (rate_state_created)
    {
        rate_function->destroy(rate_place.data());
        rate_state_created = false;
        has_current_series = false;
        current_series_samples = 0;
    }

    if (sum_function)
    {
        for (auto & entry : group_states)
        {
            if (entry.getMapped())
                sum_function->destroy(entry.getMapped());
        }
    }
    group_states.clear();
}

}
