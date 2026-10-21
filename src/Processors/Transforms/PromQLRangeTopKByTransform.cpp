#include <Processors/Transforms/PromQLRangeTopKByTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cmath>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int LOGICAL_ERROR;
}

SharedHeader PromQLRangeTopKByTransform::transformHeader(const SharedHeader & input_header)
{
    if (!input_header)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range topk input header is null");

    return input_header;
}

PromQLRangeTopKByTransform::PromQLRangeTopKByTransform(
    SharedHeader input_header_, UInt64 k_, bool bottomk_, size_t max_output_block_size_)
    : IAccumulatingTransform(input_header_, transformHeader(input_header_))
    , k(k_)
    , bottomk(bottomk_)
    , max_output_block_size(max_output_block_size_)
{
    if (max_output_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL native range topk requires a positive output block size");

    if (!input_header_->has(TimeSeriesColumnNames::Group) || !input_header_->has(TimeSeriesColumnNames::Values))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range topk expects {} and {} columns, got {}",
            TimeSeriesColumnNames::Group,
            TimeSeriesColumnNames::Values,
            input_header_->dumpStructure());

    group_position = input_header_->getPositionByName(TimeSeriesColumnNames::Group);
    values_position = input_header_->getPositionByName(TimeSeriesColumnNames::Values);

    const auto & group_type = input_header_->getByPosition(group_position).type;
    if (!isUInt64(group_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range topk expects {} to have type UInt64, got {}",
            TimeSeriesColumnNames::Group,
            group_type->getName());

    const auto & values_type = input_header_->getByPosition(values_position).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(values_type.get());
    if (!array_type || removeNullable(array_type->getNestedType())->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL native range topk expects {} to have an Array(Float64) or Array(Nullable(Float64)) type, got {}",
            TimeSeriesColumnNames::Values,
            values_type->getName());
}

bool PromQLRangeTopKByTransform::isBetter(
    const Field & lhs, UInt64 lhs_group, const Field & rhs, UInt64 rhs_group, bool bottomk)
{
    const Float64 lhs_value = lhs.safeGet<Float64>();
    const Float64 rhs_value = rhs.safeGet<Float64>();
    const bool lhs_nan = std::isnan(lhs_value);
    const bool rhs_nan = std::isnan(rhs_value);

    /// NaN is ranked after every non-NaN value for both operators, matching
    /// the `timeSeries{TopK,BottomK}Masks` aggregate semantics.
    if (lhs_nan != rhs_nan)
        return !lhs_nan;
    if (!lhs_nan && lhs_value != rhs_value)
        return bottomk ? lhs_value < rhs_value : lhs_value > rhs_value;

    /// PromQL leaves ties unspecified; the native path makes them stable by
    /// preferring the smaller group key.
    return lhs_group < rhs_group;
}

void PromQLRangeTopKByTransform::validateValues(const Array & values) const
{
    if (has_num_steps && values.size() != num_steps)
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "PromQL native range topk requires all values arrays to have {} steps, got {}",
            num_steps,
            values.size());
}

void PromQLRangeTopKByTransform::consume(Chunk chunk)
{
    if (!chunk.getNumRows())
        return;

    const auto & columns = chunk.getColumns();
    const auto * group_column = typeid_cast<const ColumnUInt64 *>(columns[group_position].get());
    const auto * values_column = typeid_cast<const ColumnArray *>(columns[values_position].get());
    if (!group_column || !values_column)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL native range topk received incompatible group/values columns");

    rows.reserve(rows.size() + chunk.getNumRows());
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        const UInt64 group = group_column->getElement(row);
        if (!seen_groups.insert(group).second)
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "PromQL native range topk received duplicate merged group {}",
                group);

        Field values_field;
        values_column->get(row, values_field);
        if (values_field.getType() != Field::Types::Array)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PromQL native range topk received a non-array values row");

        const auto & values = values_field.safeGet<Array>();
        if (!has_num_steps)
        {
            num_steps = values.size();
            has_num_steps = true;
        }
        validateValues(values);
        rows.push_back(Row{group, values});
    }
}

Chunk PromQLRangeTopKByTransform::generate()
{
    if (rows.empty() || k == 0)
        return {};

    if (!generation_started)
    {
        generation_started = true;
        selected.assign(rows.size(), std::vector<UInt8>(num_steps, 0));
        for (size_t step = 0; step < num_steps; ++step)
        {
            std::vector<size_t> candidates;
            candidates.reserve(rows.size());
            for (size_t row = 0; row < rows.size(); ++row)
            {
                if (!rows[row].values[step].isNull())
                    candidates.push_back(row);
            }

            const size_t selected_count = std::min<UInt64>(k, candidates.size());
            if (selected_count)
            {
                std::partial_sort(
                    candidates.begin(),
                    candidates.begin() + selected_count,
                    candidates.end(),
                    [&](size_t lhs, size_t rhs)
                    {
                        return isBetter(rows[lhs].values[step], rows[lhs].group, rows[rhs].values[step], rows[rhs].group, bottomk);
                    });
            }
            for (size_t index = 0; index < selected_count; ++index)
                selected[candidates[index]][step] = 1;
        }

        generated_rows.reserve(rows.size());
        for (size_t row = 0; row < rows.size(); ++row)
        {
            if (std::any_of(selected[row].begin(), selected[row].end(), [](UInt8 value) { return value != 0; }))
                generated_rows.push_back(row);
        }
        std::sort(
            generated_rows.begin(),
            generated_rows.end(),
            [&](size_t lhs, size_t rhs) { return rows[lhs].group < rows[rhs].group; });
    }

    if (next_generated_row == generated_rows.size())
        return {};

    auto group_column = ColumnUInt64::create();
    auto values_column = input.getSharedHeader()->getByPosition(values_position).type->createColumn();
    const size_t output_rows = std::min(max_output_block_size, generated_rows.size() - next_generated_row);
    group_column->reserve(output_rows);
    values_column->reserve(output_rows);
    const size_t output_end = next_generated_row + output_rows;
    for (; next_generated_row < output_end; ++next_generated_row)
    {
        const size_t row = generated_rows[next_generated_row];

        Array output_values;
        output_values.reserve(num_steps);
        for (size_t step = 0; step < num_steps; ++step)
            output_values.emplace_back(selected[row][step] ? rows[row].values[step] : Field{});

        group_column->insertValue(rows[row].group);
        values_column->insert(output_values);
    }

    return Chunk(Columns{std::move(group_column), std::move(values_column)}, output_rows);
}

}
