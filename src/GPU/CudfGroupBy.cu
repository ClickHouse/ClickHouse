#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>
#include <GPU/GPUFilter.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/copying.hpp>
#include <cudf/groupby.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/unary.hpp>

#include <rmm/device_buffer.hpp>

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB::GPU
{

namespace
{

constexpr int filter_threads_per_block = 256;

/// `streaming_groupby` encodes a row of a batch as its capacity plus the row's index, in a `cudf::size_type`.
constexpr size_t max_capacity = static_cast<size_t>(std::numeric_limits<cudf::size_type>::max());
constexpr size_t max_slice_rows = 64UL << 20;
constexpr size_t min_capacity = 1UL << 20;

/// Sets the bit of every row that passes `program`, a word of the null mask per 32 rows.
__global__ void filterToNullMask(GPUFilterProgram program, Filtering::FilterLayouts filters, size_t rows, cudf::bitmask_type * mask)
{
    const size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const bool passes = row < rows && Filtering::passesFilter(program, filters, row);
    const unsigned word = __ballot_sync(0xffffffffu, passes);
    if (row < rows && (threadIdx.x % 32) == 0)
        mask[row / 32] = word;
}

/// Whether each row passes `program`, as a column of `BOOL8`.
__global__ void filterToBooleans(GPUFilterProgram program, Filtering::FilterLayouts columns, size_t rows, bool * mask)
{
    const size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    if (row < rows)
        mask[row] = Filtering::passesFilter(program, columns, row);
}

__host__ std::unique_ptr<cudf::groupby_aggregation> groupByAggregationFor(GPUAggregationKind aggregation)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Sum: return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
        case GPUAggregationKind::Min: return cudf::make_min_aggregation<cudf::groupby_aggregation>();
        case GPUAggregationKind::Max: return cudf::make_max_aggregation<cudf::groupby_aggregation>();
        case GPUAggregationKind::Count: return cudf::make_count_aggregation<cudf::groupby_aggregation>(cudf::null_policy::INCLUDE);
        case GPUAggregationKind::Mean: return cudf::make_mean_aggregation<cudf::groupby_aggregation>();
    }
    throwGPUError(("unknown aggregation " + std::to_string(static_cast<int>(aggregation))).c_str());
}

__host__ cudf::column_view withNullMask(const cudf::column_view & column, const cudf::bitmask_type * mask, cudf::size_type null_count)
{
    std::vector<cudf::column_view> children(column.child_begin(), column.child_end());
    return cudf::column_view(column.type(), column.size(), column.head(), mask, null_count, column.offset(), children);
}

}

struct CudfGroupBy::State
{
    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const cuda::stream_ref stream;

    std::vector<cudf::size_type> key_indices;

    /// Sized for `capacity` groups, and made again for more of them by `grow`.
    std::optional<cudf::groupby::streaming_groupby> grouping;
    size_t capacity = 0;

    /// The keys, then the values, once the groups are closed.
    std::unique_ptr<cudf::table> groups;

    /// What a batch's `WHERE` leaves out, as the null mask of its first key.
    rmm::device_buffer filter_mask;

    /// Of the keys and of the values of varying width: their offsets widened for a view of the device's.
    std::vector<std::unique_ptr<cudf::column>> variable_offsets;
    std::vector<std::unique_ptr<cudf::column>> value_offsets;
    bool finalized = false;

    __host__ State(GPUSpan<GPUElementType> keys_, GPUSpan<GPUElementType> values_, GPUSpan<GPUGroupByAggregate> aggregates_, cuda::stream_ref stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_.begin(), values_.end())
        , aggregates(aggregates_.begin(), aggregates_.end())
        , stream(stream_)
        , filter_mask(0, stream_)
    {
        key_indices.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            key_indices[i] = static_cast<cudf::size_type>(i);
    }

    __host__ cudf::groupby::streaming_groupby make(size_t new_capacity) const
    {
        /// A `count` counts the rows of the first key, which has no nulls but what a `WHERE` leaves out.
        std::vector<cudf::groupby::streaming_aggregation_request> requests;
        requests.reserve(aggregates.size());
        for (const auto & aggregate : aggregates)
            requests.push_back({
                .column_index = aggregate.column == GPUGroupByAggregate::no_column ? 0 : static_cast<cudf::size_type>(keys.size() + aggregate.column),
                .aggregation = groupByAggregationFor(aggregate.aggregation),
            });

        /// The keys have no nulls but what a `WHERE` leaves out.
        return guarded("making a `streaming_groupby` of at most " + std::to_string(new_capacity) + " groups", [&]
        {
            return cudf::groupby::streaming_groupby(
                key_indices, requests, static_cast<cudf::size_type>(new_capacity), cudf::null_policy::EXCLUDE);
        });
    }

    /// The keys, then the values, as cuDF returned them, checked to be as wide as what they are taken as.
    __host__ std::unique_ptr<cudf::table> tableOf(std::unique_ptr<cudf::table> group_keys, std::vector<cudf::groupby::aggregation_result> results) const
    {
        std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            std::unique_ptr<cudf::column> & result = results[i].results.front();
            const GPUElementType result_type = aggregates[i].result_type;
            /// cuDF counts into a `cudf::size_type`, where ClickHouse counts into a `UInt64`.
            if (aggregates[i].aggregation == GPUAggregationKind::Count && cudf::size_of(result->type()) != sizeOf(result_type))
                result = guarded("widening a count", [&] { return cudf::cast(result->view(), cudf::data_type{cudf::type_id::INT64}, stream); });
            /// cuDF sums unsigned integers into signed ones, whose bits are the same.
            const bool as_wide = columnKindOf(result_type) == GPUColumnKind::Variable
                ? result->type().id() == cudf::type_id::STRING
                : cudf::size_of(result->type()) == sizeOf(result_type);
            if (!as_wide)
                throwGPUError(
                    ("the device reduced aggregate " + std::to_string(i) + " into cuDF type "
                    + std::to_string(static_cast<int32_t>(result->type().id())) + ", which is not as wide as element type "
                    + std::to_string(static_cast<int>(result_type))).c_str());
            columns.push_back(std::move(result));
        }

        return std::make_unique<cudf::table>(std::move(columns));
    }

    /// `streaming_groupby` holds as many groups as it was made for, and takes no more rows at a time. Before a slice that
    /// could bring more groups than fit, it is made again for half as many more and the groups so far are merged into it,
    /// so that its hash table follows the groups seen, at an amortized cost of a few merges per group.
    __host__ void group(const cudf::table_view & table)
    {
        const size_t rows = static_cast<size_t>(table.num_rows());
        size_t offset = 0;
        while (offset < rows)
        {
            const size_t distinct = grouping ? static_cast<size_t>(grouping->distinct_keys()) : 0;
            const size_t slice_rows = std::min(rows - offset, max_slice_rows);
            const auto begin = static_cast<cudf::size_type>(offset);
            const auto end = static_cast<cudf::size_type>(offset + slice_rows);
            const cudf::table_view slice = begin == 0 && end == table.num_rows() ? table : cudf::slice(table, {begin, end}).front();

            if (!grouping || distinct + slice_rows > capacity)
            {
                /// Grown by half, with a quarter to spare: its hash table holds twice as many slots as groups.
                const size_t new_capacity = std::max({capacity + capacity / 2, (distinct + slice_rows) + (distinct + slice_rows) / 4, min_capacity});
                if (new_capacity + slice_rows > max_capacity)
                    throwGPUError(("a `GROUP BY` of more than " + std::to_string(max_capacity - slice_rows) + " groups").c_str());

                cudf::groupby::streaming_groupby grown = make(new_capacity);
                guarded("grouping " + std::to_string(slice_rows) + " rows into a grown `streaming_groupby`", [&]
                {
                    grown.aggregate(slice, stream);
                    if (grouping)
                        grown.merge(*grouping, stream);
                    return 0;
                });
                grouping.emplace(std::move(grown));
                capacity = new_capacity;
            }
            else
            {
                guarded(
                    "grouping " + std::to_string(slice_rows) + " rows by " + std::to_string(keys.size()) + " keys",
                    [&] { grouping->aggregate(slice, stream); return 0; });
            }
            offset += slice_rows;
        }
    }

    /// Leaves the rows that `filter` is false for out of the batch, by making them nulls of its first key.
    __host__ cudf::column_view filterKey(
        const cudf::column_view & key, size_t rows, GPUSpan<DeviceFixedColumn> filter_columns, const GPUFilterProgram & filter)
    {
        if (filter_columns.size() > max_filter_columns)
            throwGPUError(("a `WHERE` over " + std::to_string(filter_columns.size()) + " columns").c_str());

        Filtering::FilterLayouts layouts;
        layouts.present = true;
        layouts.count = static_cast<uint32_t>(filter_columns.size());
        for (size_t i = 0; i < filter_columns.size(); ++i)
        {
            if (filter_columns[i].rows != rows)
                throwGPUError(("a column of a `WHERE` of " + std::to_string(filter_columns[i].rows) + " rows in a batch of " + std::to_string(rows)).c_str());
            layouts.columns[i] = {.data = filter_columns[i].data, .type = filter_columns[i].element_type};
        }

        const auto num_rows = static_cast<cudf::size_type>(rows);
        filter_mask.resize(cudf::bitmask_allocation_size_bytes(num_rows), stream);
        auto * mask = static_cast<cudf::bitmask_type *>(filter_mask.data());

        const auto blocks = static_cast<unsigned>((rows + filter_threads_per_block - 1) / filter_threads_per_block);
        filterToNullMask<<<blocks, filter_threads_per_block, 0, stream.get()>>>(filter, layouts, rows, mask);
        checkCuda(cudaGetLastError(), "Cannot evaluate a `WHERE` into a null mask");

        const cudf::size_type null_count = guarded("counting the rows a `WHERE` leaves out", [&]
        {
            return cudf::null_count(mask, 0, num_rows, stream);
        });
        return withNullMask(key, mask, null_count);
    }
};

__host__ CudfGroupBy::CudfGroupBy(
    GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, cuda::stream_ref stream)
{
    if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
        throwGPUError("a `GROUP BY` through cuDF on the device's default stream, where cuco cannot copy its counts back");

    if (keys.empty() || values.empty())
        throwGPUError("a `GROUP BY` without keys or without values");

    for (const auto & key : keys)
    {
        if (key != GPUElementType::String && !isInteger(key))
            throwGPUError(("a key of element type " + std::to_string(static_cast<int>(key)) + " is neither an integer nor a string").c_str());
    }

    for (const auto & aggregate : aggregates)
    {
        const bool counts = aggregate.aggregation == GPUAggregationKind::Count;
        if (counts != (aggregate.column == GPUGroupByAggregate::no_column))
            throwGPUError("a `count` of a column, or another aggregate of none");
        if (counts)
        {
            if (aggregate.result_type != GPUElementType::UInt64)
                throwGPUError("a `count` into other than a `UInt64`");
            continue;
        }
        if (aggregate.column >= values.size())
            throwGPUError(("an aggregate of value " + std::to_string(aggregate.column) + " of " + std::to_string(values.size())).c_str());
        const GPUElementType element_type = values[aggregate.column];
        const bool variable = columnKindOf(element_type) == GPUColumnKind::Variable;
        switch (aggregate.aggregation)
        {
            case GPUAggregationKind::Sum:
                if (variable || sizeOf(aggregate.result_type) != 8)
                    throwGPUError("a sum of values of varying width, or into other than eight bytes");
                break;
            case GPUAggregationKind::Mean:
                if (variable || aggregate.result_type != GPUElementType::Float64)
                    throwGPUError("an `avg` of values of varying width, or into other than a `Float64`");
                break;
            case GPUAggregationKind::Min:
            case GPUAggregationKind::Max:
                if (aggregate.result_type != element_type)
                    throwGPUError("a `min` or a `max` into another type than its values'");
                break;
            case GPUAggregationKind::Count:
                break;
        }
    }

    if (aggregates.empty())
        throwGPUError("a `GROUP BY` without aggregates");

    state = new State(keys, values, aggregates, stream);
}

__host__ CudfGroupBy::~CudfGroupBy()
{
    delete state;
}

__host__ void CudfGroupBy::addBatch(
    GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUFilterProgram * filter)
{
    if (state->finalized)
        throwGPUError("a batch after the groups were closed");

    if (keys.size() != state->keys.size() || values.size() != state->values.size())
        throwGPUError(
            ("a batch of " + std::to_string(keys.size()) + " keys and " + std::to_string(values.size()) + " values, expected "
            + std::to_string(state->keys.size()) + " and " + std::to_string(state->values.size())).c_str());

    const size_t rows = keys[0].rows();
    if (rows == 0)
        return;

    std::vector<cudf::column_view> columns;
    columns.reserve(state->keys.size() + state->values.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (keys[i].rows() != rows)
            throwGPUError(("key " + std::to_string(i) + " of " + std::to_string(keys[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
        columns.push_back(columnViewOf(keys[i], state->keys[i]));
    }

    for (size_t i = 0; i < values.size(); ++i)
    {
        if (values[i].rows() != rows)
            throwGPUError(("value " + std::to_string(i) + " of " + std::to_string(values[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
        columns.push_back(columnViewOf(values[i], state->values[i]));
    }

    if (filter)
        columns[0] = state->filterKey(columns[0], rows, filter_columns, *filter);

    state->group(cudf::table_view(columns));
}

__host__ size_t CudfGroupBy::finalize()
{
    if (state->finalized)
        throwGPUError("the groups were closed twice");

    if (state->grouping)
    {
        auto [group_keys, results] = guarded("closing the groups", [&]
        {
            return std::move(*state->grouping).finalize_and_release(state->stream);
        });
        state->groups = state->tableOf(std::move(group_keys), std::move(results));
    }
    state->grouping.reset();
    state->finalized = true;

    if (!state->groups)
        return 0;

    state->variable_offsets.resize(state->keys.size());
    state->value_offsets.resize(state->aggregates.size());
    return static_cast<size_t>(state->groups->num_rows());
}

__host__ size_t CudfGroupBy::filterGroups(const GPUFilterProgram & program, GPUSpan<uint32_t> columns)
{
    if (!state->finalized)
        throwGPUError("the groups were filtered before they were closed");

    if (!state->groups || state->groups->num_rows() == 0)
        return 0;

    if (columns.size() > max_filter_columns)
        throwGPUError(("a filter of the groups over " + std::to_string(columns.size()) + " columns").c_str());

    const cudf::table_view groups = state->groups->view();
    const auto rows = static_cast<size_t>(groups.num_rows());

    Filtering::FilterLayouts layouts;
    layouts.present = true;
    layouts.count = static_cast<uint32_t>(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        const uint32_t column = columns[i];
        const GPUElementType element_type = column < state->keys.size()
            ? state->keys[column]
            : (column - state->keys.size() < state->aggregates.size() ? state->aggregates[column - state->keys.size()].result_type
                                                                       : GPUElementType::String);
        if (column >= static_cast<uint32_t>(groups.num_columns()) || columnKindOf(element_type) != GPUColumnKind::Fixed)
            throwGPUError(("a filter of the groups over column " + std::to_string(column) + ", which is not of a fixed width").c_str());
        const cudf::column_view view = groups.column(static_cast<cudf::size_type>(column));
        checkNoNulls(view);
        layouts.columns[i] = {.data = view.head<char>(), .type = element_type};
    }

    rmm::device_buffer mask(rows * sizeof(bool), state->stream);
    const auto blocks = static_cast<unsigned>((rows + filter_threads_per_block - 1) / filter_threads_per_block);
    filterToBooleans<<<blocks, filter_threads_per_block, 0, state->stream.get()>>>(program, layouts, rows, static_cast<bool *>(mask.data()));
    checkCuda(cudaGetLastError(), "Cannot filter the groups on the device");

    const cudf::column_view mask_view(cudf::data_type{cudf::type_id::BOOL8}, static_cast<cudf::size_type>(rows), mask.data(), nullptr, 0);
    state->groups = guarded("filtering " + std::to_string(rows) + " groups", [&] { return cudf::apply_boolean_mask(groups, mask_view, state->stream); });
    return static_cast<size_t>(state->groups->num_rows());
}

__host__ DeviceColumnView CudfGroupBy::key(size_t index) const
{
    if (!state->finalized)
        throwGPUError("the groups were asked for before they were closed");

    if (index >= state->keys.size())
        throwGPUError(("key " + std::to_string(index) + " of " + std::to_string(state->keys.size())).c_str());

    const GPUElementType type = state->keys[index];
    switch (columnKindOf(type))
    {
        case GPUColumnKind::Fixed:
            if (!state->groups)
                return DeviceFixedColumn{.element_type = type};
            return deviceViewOf(state->groups->view().column(static_cast<cudf::size_type>(index)), type);
        case GPUColumnKind::Variable:
        {
            if (!state->groups)
                return DeviceVariableColumn{};
            DeviceVariableColumnOfCudf of_cudf
                = deviceViewOfVariable(state->groups->view().column(static_cast<cudf::size_type>(index)), state->stream);
            state->variable_offsets[index] = std::move(of_cudf.widened_offsets);
            return of_cudf.view;
        }
    }
    throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(type))).c_str());
}

__host__ DeviceColumnView CudfGroupBy::value(size_t index) const
{
    if (!state->finalized)
        throwGPUError("the groups were asked for before they were closed");

    if (index >= state->aggregates.size())
        throwGPUError(("aggregate " + std::to_string(index) + " of " + std::to_string(state->aggregates.size())).c_str());

    const GPUElementType result_type = state->aggregates[index].result_type;
    const auto column_index = static_cast<cudf::size_type>(state->keys.size() + index);
    switch (columnKindOf(result_type))
    {
        case GPUColumnKind::Fixed:
        {
            if (!state->groups)
                return DeviceFixedColumn{.element_type = result_type};

            /// Taken by its bits, as `tableOf` checked it is as wide as `result_type`: cuDF sums unsigned integers into
            /// signed ones.
            const cudf::column_view column = state->groups->view().column(column_index);
            checkNoNulls(column);
            if (column.offset() != 0)
                throwGPUError("the device returned a value of the groups as a slice");
            return DeviceFixedColumn{.element_type = result_type, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
        }
        case GPUColumnKind::Variable:
        {
            if (!state->groups)
                return DeviceVariableColumn{};
            DeviceVariableColumnOfCudf of_cudf = deviceViewOfVariable(state->groups->view().column(column_index), state->stream);
            state->value_offsets[index] = std::move(of_cudf.widened_offsets);
            return of_cudf.view;
        }
    }
    throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(result_type))).c_str());
}

}
