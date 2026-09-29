#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/groupby.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/unary.hpp>

#include <rmm/exec_policy.hpp>

#include <thrust/transform.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace DB::GPU
{

namespace
{

std::unique_ptr<cudf::groupby_aggregation> groupByAggregationFor(GPUAggregationKind aggregation)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Sum: return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
        case GPUAggregationKind::Min: return cudf::make_min_aggregation<cudf::groupby_aggregation>();
        case GPUAggregationKind::Max: return cudf::make_max_aggregation<cudf::groupby_aggregation>();
    }
    throwGPUError(("unknown aggregation " + std::to_string(static_cast<int>(aggregation))).c_str());
}

GPUElementType leftInOf(const GPUGroupByValue & value)
{
    return value.aggregation == GPUAggregationKind::Sum ? value.result_type : value.element_type;
}

struct SubtractFrom
{
    uint64_t minus;

    __device__ uint64_t operator()(uint64_t offset) const { return offset - minus; }
};

}

struct CudfGroupBy::State
{
    const std::vector<GPUElementType> keys;
    const std::vector<GPUGroupByValue> values;
    const rmm::cuda_stream_view stream;

    std::unique_ptr<cudf::table> merged;
    std::vector<std::unique_ptr<cudf::table>> partials;
    size_t partial_rows = 0;

    /// Of the keys and of the values of varying width: their offsets widened for a view of the device's.
    std::vector<std::unique_ptr<cudf::column>> variable_offsets;
    std::vector<std::unique_ptr<cudf::column>> value_offsets;
    bool finalized = false;

    State(GPUSpan<GPUElementType> keys_, GPUSpan<GPUGroupByValue> values_, rmm::cuda_stream_view stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_.begin(), values_.end())
        , stream(stream_)
    {
    }

    std::unique_ptr<cudf::table> group(const cudf::table_view & table, const std::vector<GPUAggregationKind> & kinds) const
    {
        std::vector<cudf::size_type> key_indices(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            key_indices[i] = static_cast<cudf::size_type>(i);

        cudf::groupby::groupby grouping(table.select(key_indices), cudf::null_policy::INCLUDE);

        std::vector<cudf::groupby::aggregation_request> requests(values.size());
        for (size_t i = 0; i < values.size(); ++i)
        {
            requests[i].values = table.column(static_cast<cudf::size_type>(keys.size() + i));
            requests[i].aggregations.push_back(groupByAggregationFor(kinds[i]));
        }

        auto [group_keys, results] = guarded(
            "grouping " + std::to_string(table.num_rows()) + " rows by " + std::to_string(keys.size()) + " keys",
            [&] { return grouping.aggregate(requests, stream); });

        std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();
        for (size_t i = 0; i < values.size(); ++i)
        {
            std::unique_ptr<cudf::column> & result = results[i].results.front();
            const GPUElementType left_in = leftInOf(values[i]);
            /// cuDF sums unsigned integers into signed ones, whose bits are the same.
            const bool as_wide = columnKindOf(left_in) == GPUColumnKind::Variable
                ? result->type().id() == cudf::type_id::STRING
                : cudf::size_of(result->type()) == sizeOf(left_in);
            if (!as_wide)
                throwGPUError(
                    ("the device reduced value " + std::to_string(i) + " into cuDF type "
                    + std::to_string(static_cast<int32_t>(result->type().id())) + ", which is not as wide as element type "
                    + std::to_string(static_cast<int>(left_in))).c_str());
            columns.push_back(std::move(result));
        }

        return std::make_unique<cudf::table>(std::move(columns));
    }

    void mergeAll()
    {
        if (partials.empty())
            return;

        std::vector<cudf::table_view> tables;
        tables.reserve(partials.size() + 1);
        if (merged)
            tables.push_back(merged->view());
        for (const auto & partial : partials)
            tables.push_back(partial->view());

        std::vector<GPUAggregationKind> kinds;
        kinds.reserve(values.size());
        for (const auto & value : values)
            kinds.push_back(value.aggregation);

        if (tables.size() == 1)
        {
            merged = std::move(partials.front());
        }
        else
        {
            const std::unique_ptr<cudf::table> gathered
                = guarded("gathering " + std::to_string(tables.size()) + " tables of groups", [&] { return cudf::concatenate(tables, stream); });
            merged = group(gathered->view(), kinds);
        }

        partials.clear();
        partial_rows = 0;
    }
};

CudfGroupBy::CudfGroupBy(GPUSpan<GPUElementType> keys, GPUSpan<GPUGroupByValue> values, rmm::cuda_stream_view stream)
{
    if (stream.is_default())
        throwGPUError("a `GROUP BY` by variable-width keys on the device's default stream, where cuco cannot copy its counts back");

    if (keys.empty() || values.empty())
        throwGPUError("a `GROUP BY` without keys or without values");

    for (const auto & key : keys)
    {
        if (key != GPUElementType::String && !isInteger(key))
            throwGPUError(("a key of element type " + std::to_string(static_cast<int>(key)) + " is neither an integer nor a string").c_str());
    }

    for (const auto & value : values)
    {
        const bool variable = columnKindOf(value.element_type) == GPUColumnKind::Variable;
        if (variable && value.aggregation == GPUAggregationKind::Sum)
            throwGPUError("a sum of values of varying width");
        if (!variable && !isInteger(value.element_type))
            throwGPUError(
                ("a value of element type " + std::to_string(static_cast<int>(value.element_type))
                + " is neither an integer nor of varying width, which a `GROUP BY` through cuDF reduces only").c_str());
        if (value.aggregation == GPUAggregationKind::Sum && sizeOf(value.result_type) != 8)
            throwGPUError(("a sum into element type " + std::to_string(static_cast<int>(value.result_type)) + ", which is not eight bytes wide").c_str());
    }

    state = new State(keys, values, stream);
}

CudfGroupBy::~CudfGroupBy()
{
    delete state;
}

void CudfGroupBy::addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values)
{
    if (state->finalized)
        throwGPUError("a batch after the groups were closed");

    if (keys.size() != state->keys.size() || values.size() != state->values.size())
        throwGPUError(
            ("a batch of " + std::to_string(keys.size()) + " keys and " + std::to_string(values.size()) + " values, expected "
            + std::to_string(state->keys.size()) + " and " + std::to_string(state->values.size())).c_str());

    const size_t rows = values[0].rows();
    if (rows == 0)
        return;

    std::vector<cudf::column_view> columns;
    columns.reserve(state->keys.size() + state->values.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        const std::string what = "key " + std::to_string(i);
        if (keys[i].rows() != rows)
            throwGPUError((what + " of " + std::to_string(keys[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
        columns.push_back(columnViewOf(keys[i], state->keys[i], what));
    }

    for (size_t i = 0; i < values.size(); ++i)
    {
        const std::string what = "value " + std::to_string(i);
        if (values[i].rows() != rows)
            throwGPUError((what + " of " + std::to_string(values[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
        columns.push_back(columnViewOf(values[i], state->values[i].element_type, what));
    }

    std::vector<GPUAggregationKind> kinds;
    kinds.reserve(state->values.size());
    for (const auto & value : state->values)
        kinds.push_back(value.aggregation);

    std::unique_ptr<cudf::table> groups = state->group(cudf::table_view(columns), kinds);
    state->partial_rows += static_cast<size_t>(groups->num_rows());
    state->partials.push_back(std::move(groups));

    const size_t merged_rows = state->merged ? static_cast<size_t>(state->merged->num_rows()) : 0;
    if (state->partial_rows > merged_rows)
        state->mergeAll();
}

size_t CudfGroupBy::finalize()
{
    if (state->finalized)
        throwGPUError("the groups were closed twice");

    state->mergeAll();
    state->finalized = true;

    if (!state->merged)
        return 0;

    state->variable_offsets.resize(state->keys.size());
    state->value_offsets.resize(state->values.size());
    return static_cast<size_t>(state->merged->num_rows());
}

DeviceColumnView CudfGroupBy::key(size_t index) const
{
    if (!state->finalized)
        throwGPUError("the groups were asked for before they were closed");

    if (index >= state->keys.size())
        throwGPUError(("key " + std::to_string(index) + " of " + std::to_string(state->keys.size())).c_str());

    const GPUElementType type = state->keys[index];
    switch (columnKindOf(type))
    {
        case GPUColumnKind::Fixed:
            if (!state->merged)
                return DeviceFixedColumn{.element_type = type};
            return deviceViewOf(state->merged->view().column(static_cast<cudf::size_type>(index)), type, "a key of the groups");
        case GPUColumnKind::Variable:
            if (!state->merged)
                return DeviceVariableColumn{};
            return deviceViewOfVariable(
                state->merged->view().column(static_cast<cudf::size_type>(index)),
                state->variable_offsets[index],
                "a key of the groups",
                state->stream.value());
    }
    throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(type))).c_str());
}

DeviceColumnView CudfGroupBy::value(size_t index) const
{
    if (!state->finalized)
        throwGPUError("the groups were asked for before they were closed");

    if (index >= state->values.size())
        throwGPUError(("value " + std::to_string(index) + " of " + std::to_string(state->values.size())).c_str());

    const GPUElementType left_in = leftInOf(state->values[index]);
    const auto column_index = static_cast<cudf::size_type>(state->keys.size() + index);
    switch (columnKindOf(left_in))
    {
        case GPUColumnKind::Fixed:
        {
            if (!state->merged)
                return DeviceFixedColumn{.element_type = left_in};

            /// Taken by its bits, as `group` checked it is as wide as `left_in`: cuDF sums unsigned integers into signed ones.
            const cudf::column_view column = state->merged->view().column(column_index);
            checkNoNulls(column, "a value of the groups");
            if (column.offset() != 0)
                throwGPUError("the device returned a value of the groups as a slice");
            return DeviceFixedColumn{.element_type = left_in, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
        }
        case GPUColumnKind::Variable:
            if (!state->merged)
                return DeviceVariableColumn{};
            return deviceViewOfVariable(
                state->merged->view().column(column_index), state->value_offsets[index], "a value of the groups", state->stream.value());
    }
    throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(left_in))).c_str());
}

void subtractFromOffsets(const uint64_t * from, size_t count, uint64_t minus, uint64_t * to, rmm::cuda_stream_view stream)
{
    if (count == 0 || (minus == 0 && from == to))
        return;

    guarded("rebasing " + std::to_string(count) + " string offsets", [&]
    {
        thrust::transform(rmm::exec_policy_nosync(stream), from, from + count, to, SubtractFrom{minus});
    });
}

}
