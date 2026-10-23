#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>
#include <GPU/GPUExpression.cuh>
#include <GPU/GPUFilter.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
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

/// A batch of which a `WHERE` leaves out at least this share of the rows is compacted to the rows it keeps before they
/// are grouped and their derived values computed. `streaming_groupby` still goes over a row left out as a null of the
/// first key, and the derived values are computed for it, while compacting reads every row once and writes those kept.
constexpr double min_left_out_share_to_compact = 0.25;

/// The batches are reduced over their runs of equal keys before they are grouped where the first batch has at most this
/// many runs per row, see `CudfGroupBy::State::reduce_runs`.
constexpr double max_runs_per_row_to_reduce = 0.5;

/// The reduced runs gathered are combined into their groups once they take more than this, or twice what they took
/// after they were combined last, whichever is more, or once they reach this many rows, below what cuDF concatenates.
constexpr size_t min_bytes_to_combine_runs = 4UL << 30;
constexpr size_t max_rows_to_combine_runs = 1UL << 30;

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
    /// The values of a batch, then those derived from them.
    const std::vector<GPUElementType> values;
    const std::vector<GPUDerivedValue> derived;
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
    /// As many `true` bytes as the largest batch compacted had rows: the values of the `BOOL8` column whose nulls are
    /// what a `WHERE` leaves out.
    rmm::device_buffer all_true;

    /// Of each derived value, its values over the rows of the batch.
    std::vector<rmm::device_buffer> derived_values;

    /// Whether the partial results of every aggregate over some of the rows of a group combine by an aggregate: those of
    /// a `sum`, a `min` and a `max` by the same one and those of a `count` by a `sum`, but not those of an `avg`.
    bool runs_combine = false;

    /// Set by the first batch: whether every batch is reduced over its runs of equal keys, which is so where the first
    /// has few runs per row, as the rows of a part sorted by the keys have. Then the runs are gathered as they are, not
    /// grouped through `streaming_groupby`, and combined into their groups at once by a groupby of cuDF: the rows of a
    /// sorted part bring keys the groups so far do not have, and `streaming_groupby` takes much longer to add a group
    /// than a groupby made for all of them at once.
    std::optional<bool> reduce_runs;

    /// The runs reduced from the batches, each the keys and then the aggregates, how many rows and bytes they take,
    /// and the bytes they took after they were combined last.
    std::vector<std::unique_ptr<cudf::table>> reduced_runs;
    size_t reduced_rows = 0;
    size_t reduced_bytes = 0;
    size_t combined_bytes = 0;

    /// Without keys and aggregates, the rows a `WHERE` keeps are collected rather than grouped: the values of each batch's.
    const bool collect;
    std::vector<std::unique_ptr<cudf::table>> collected;

    /// Of the keys and of the values of varying width: their offsets widened for a view of the device's.
    std::vector<std::unique_ptr<cudf::column>> variable_offsets;
    std::vector<std::unique_ptr<cudf::column>> value_offsets;
    bool finalized = false;

    __host__ State(
        GPUSpan<GPUElementType> keys_,
        GPUSpan<GPUElementType> values_,
        GPUSpan<GPUDerivedValue> derived_,
        GPUSpan<GPUGroupByAggregate> aggregates_,
        cuda::stream_ref stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_.begin(), values_.end())
        , derived(derived_.begin(), derived_.end())
        , aggregates(aggregates_.begin(), aggregates_.end())
        , stream(stream_)
        , collect(keys_.empty() && aggregates_.empty())
        , filter_mask(0, stream_)
        , all_true(0, stream_)
    {
        key_indices.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            key_indices[i] = static_cast<cudf::size_type>(i);
        for (size_t i = 0; i < derived.size(); ++i)
            derived_values.emplace_back(0, stream_);
        runs_combine = std::all_of(
            aggregates.begin(), aggregates.end(), [](const GPUGroupByAggregate & aggregate) { return aggregate.aggregation != GPUAggregationKind::Mean; });
    }

    __host__ size_t numBatchValues() const { return values.size() - derived.size(); }

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

    /// Sets the bits of `filter_mask` of the rows `filter` holds for, and returns how many of the rows it leaves out.
    __host__ cudf::size_type evaluateFilter(size_t rows, GPUSpan<DeviceFixedColumn> filter_columns, const GPUFilterProgram & filter)
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

        return guarded("counting the rows a `WHERE` leaves out", [&]
        {
            return cudf::null_count(mask, 0, num_rows, stream);
        });
    }

    /// The rows of `columns` that the `WHERE` whose mask is in `filter_mask` keeps.
    __host__ std::unique_ptr<cudf::table> compact(const std::vector<cudf::column_view> & columns, size_t rows, cudf::size_type left_out)
    {
        if (all_true.size() < rows)
        {
            all_true.resize(rows, stream);
            checkCuda(cudaMemsetAsync(all_true.data(), 1, rows, stream.get()), "Cannot fill a mask of the device");
        }

        const cudf::column_view kept(
            cudf::data_type{cudf::type_id::BOOL8},
            static_cast<cudf::size_type>(rows),
            all_true.data(),
            static_cast<const cudf::bitmask_type *>(filter_mask.data()),
            left_out);
        return guarded("compacting " + std::to_string(rows) + " rows to those a `WHERE` keeps", [&]
        {
            return cudf::apply_retention_mask(cudf::table_view(columns), kept, stream);
        });
    }

    /// The keys of each run of equal keys of `columns`, the keys, the values and the derived values of a batch without
    /// nulls, and the aggregates over the run. cuDF's sort-based groupby, told the keys are sorted, starts a group
    /// wherever a row's keys differ from the row's before, so that a key in several runs comes once for each.
    __host__ std::unique_ptr<cudf::table> reduceRuns(const std::vector<cudf::column_view> & columns) const
    {
        const cudf::table_view batch_keys(std::vector<cudf::column_view>(columns.begin(), columns.begin() + keys.size()));
        cudf::groupby::groupby by_runs(batch_keys, cudf::null_policy::EXCLUDE, cudf::sorted::YES);

        std::vector<cudf::groupby::aggregation_request> requests(aggregates.size());
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const GPUGroupByAggregate & aggregate = aggregates[i];
            requests[i].values = aggregate.column == GPUGroupByAggregate::no_column ? columns[0] : columns[keys.size() + aggregate.column];
            requests[i].aggregations.push_back(groupByAggregationFor(aggregate.aggregation));
        }

        auto [run_keys, results] = guarded("reducing " + std::to_string(batch_keys.num_rows()) + " rows over their runs of equal keys", [&]
        {
            return by_runs.aggregate(requests, stream);
        });

        std::vector<std::unique_ptr<cudf::column>> reduced = run_keys->release();
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            std::unique_ptr<cudf::column> & result = results[i].results.front();
            /// As wide as the sums of counts that `combineRuns` makes of them, so that the runs gathered concatenate.
            if (aggregates[i].aggregation == GPUAggregationKind::Count && result->type().id() != cudf::type_id::INT64)
                result = guarded("widening a count", [&] { return cudf::cast(result->view(), cudf::data_type{cudf::type_id::INT64}, stream); });
            reduced.push_back(std::move(result));
        }
        return std::make_unique<cudf::table>(std::move(reduced));
    }

    /// The groups of the runs gathered: the keys, and the results of the aggregates. The runs are gone afterwards.
    __host__ std::pair<std::unique_ptr<cudf::table>, std::vector<cudf::groupby::aggregation_result>> combineRuns()
    {
        std::vector<cudf::table_view> views;
        views.reserve(reduced_runs.size());
        for (const auto & runs : reduced_runs)
            views.push_back(runs->view());

        std::unique_ptr<cudf::table> concatenated;
        if (views.size() > 1)
            concatenated = guarded("concatenating " + std::to_string(reduced_rows) + " runs of equal keys", [&]
            {
                return cudf::concatenate(views, stream);
            });
        const cudf::table_view all = concatenated ? concatenated->view() : views.front();

        std::vector<cudf::size_type> key_columns(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            key_columns[i] = static_cast<cudf::size_type>(i);
        cudf::groupby::groupby by_keys(all.select(key_columns), cudf::null_policy::EXCLUDE);

        /// The partial results of a `count` are summed, and those of the others taken by the same aggregate.
        std::vector<cudf::groupby::aggregation_request> requests(aggregates.size());
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const GPUAggregationKind kind = aggregates[i].aggregation;
            requests[i].values = all.column(static_cast<cudf::size_type>(keys.size() + i));
            requests[i].aggregations.push_back(groupByAggregationFor(kind == GPUAggregationKind::Count ? GPUAggregationKind::Sum : kind));
        }

        auto combined = guarded("combining " + std::to_string(reduced_rows) + " runs of equal keys into their groups", [&]
        {
            return by_keys.aggregate(requests, stream);
        });

        /// Freed in the order of the stream, after the groupby has read them.
        reduced_runs.clear();
        reduced_rows = 0;
        reduced_bytes = 0;
        return combined;
    }

    __host__ void gatherRuns(std::unique_ptr<cudf::table> runs)
    {
        reduced_rows += static_cast<size_t>(runs->num_rows());
        reduced_bytes += runs->alloc_size();
        reduced_runs.push_back(std::move(runs));

        if (reduced_runs.size() < 2
            || (reduced_bytes <= std::max(min_bytes_to_combine_runs, 2 * combined_bytes) && reduced_rows < max_rows_to_combine_runs))
            return;

        auto [group_keys, results] = combineRuns();
        std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();
        for (auto & result : results)
            columns.push_back(std::move(result.results.front()));
        auto groups = std::make_unique<cudf::table>(std::move(columns));
        combined_bytes = groups->alloc_size();
        reduced_rows = static_cast<size_t>(groups->num_rows());
        reduced_bytes = combined_bytes;
        reduced_runs.push_back(std::move(groups));
    }

    /// Keeps the rows of a batch of `values` that `filter`, when given, holds for, as a table of its own.
    __host__ void collectBatch(GPUSpan<DeviceColumnView> batch, GPUSpan<DeviceFixedColumn> filter_columns, const GPUFilterProgram * filter)
    {
        if (batch.size() != values.size())
            throwGPUError(("a batch of " + std::to_string(batch.size()) + " columns to collect, expected " + std::to_string(values.size())).c_str());

        const size_t rows = batch[0].rows();
        if (rows == 0)
            return;

        std::vector<cudf::column_view> columns;
        columns.reserve(values.size());
        for (size_t i = 0; i < batch.size(); ++i)
        {
            if (batch[i].rows() != rows)
                throwGPUError(("column " + std::to_string(i) + " of " + std::to_string(batch[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
            columns.push_back(columnViewOf(batch[i], values[i]));
        }

        if (filter)
        {
            const cudf::size_type left_out = evaluateFilter(rows, filter_columns, *filter);
            if (static_cast<size_t>(left_out) == rows)
                return;
            if (left_out != 0)
            {
                collected.push_back(compact(columns, rows, left_out));
                return;
            }
        }

        /// A copy, as the batch's buffers are freed once it is added.
        collected.push_back(guarded("keeping " + std::to_string(rows) + " rows", [&]
        {
            return std::make_unique<cudf::table>(cudf::table_view(columns), stream);
        }));
    }

    /// Appends to `columns`, the keys and the values of a batch of `rows` rows, the values derived from them.
    __host__ void derive(std::vector<cudf::column_view> & columns, size_t rows)
    {
        if (derived.empty())
            return;

        std::vector<DeviceFixedColumn> batch_values;
        batch_values.reserve(numBatchValues());
        for (size_t i = 0; i < numBatchValues(); ++i)
        {
            const GPUElementType type = values[i];
            batch_values.push_back(
                columnKindOf(type) == GPUColumnKind::Fixed ? deviceViewOf(columns[keys.size() + i], type) : DeviceFixedColumn{});
        }

        for (size_t j = 0; j < derived.size(); ++j)
        {
            rmm::device_buffer & out = derived_values[j];
            if (out.size() < rows * sizeof(double))
                out.resize(rows * sizeof(double), stream);
            evaluateDerivedValue(derived[j], batch_values, rows, static_cast<double *>(out.data()), stream);
            columns.emplace_back(cudf::data_type{cudf::type_id::FLOAT64}, static_cast<cudf::size_type>(rows), out.data(), nullptr, 0);
        }
    }
};

__host__ CudfGroupBy::CudfGroupBy(
    GPUSpan<GPUElementType> keys,
    GPUSpan<GPUElementType> values,
    GPUSpan<GPUDerivedValue> derived,
    GPUSpan<GPUGroupByAggregate> aggregates,
    cuda::stream_ref stream)
{
    if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
        throwGPUError("a `GROUP BY` through cuDF on the device's default stream, where cuco cannot copy its counts back");

    if (values.empty())
        throwGPUError("a `GROUP BY` without values");

    /// Collects the rows a `WHERE` keeps, of values of any type.
    if (keys.empty() && aggregates.empty())
    {
        if (!derived.empty())
            throwGPUError("a collection of rows with derived values");
        state = new State(keys, values, derived, aggregates, stream);
        return;
    }

    if (keys.empty())
        throwGPUError("a `GROUP BY` without keys");

    if (derived.size() > values.size())
        throwGPUError(("a `GROUP BY` of " + std::to_string(derived.size()) + " derived values among " + std::to_string(values.size())).c_str());

    for (size_t j = 0; j < derived.size(); ++j)
    {
        if (values[values.size() - derived.size() + j] != GPUElementType::Float64)
            throwGPUError("a derived value of other than a `Float64`");
        for (uint32_t i = 0; i < derived[j].num_columns; ++i)
        {
            if (derived[j].columns[i] >= values.size() - derived.size())
                throwGPUError(("a derived value of value " + std::to_string(derived[j].columns[i]) + ", which is not of a batch").c_str());
        }
    }

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

    state = new State(keys, values, derived, aggregates, stream);
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

    if (state->collect)
    {
        state->collectBatch(values, filter_columns, filter);
        return;
    }

    if (keys.size() != state->keys.size() || values.size() != state->numBatchValues())
        throwGPUError(
            ("a batch of " + std::to_string(keys.size()) + " keys and " + std::to_string(values.size()) + " values, expected "
            + std::to_string(state->keys.size()) + " and " + std::to_string(state->numBatchValues())).c_str());

    size_t rows = keys[0].rows();
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

    /// Lives until the groups are made of its rows.
    std::unique_ptr<cudf::table> compacted;
    if (filter)
    {
        const cudf::size_type left_out = state->evaluateFilter(rows, filter_columns, *filter);
        if (static_cast<size_t>(left_out) == rows)
            return;

        /// Runs are reduced over batches without nulls only, as cuDF sorts keys with nulls whatever it is told.
        if (left_out != 0
            && (state->reduce_runs.value_or(state->runs_combine)
                || static_cast<double>(left_out) >= min_left_out_share_to_compact * static_cast<double>(rows)))
        {
            compacted = state->compact(columns, rows, left_out);
            rows = static_cast<size_t>(compacted->num_rows());
            const cudf::table_view kept = compacted->view();
            columns.assign(kept.begin(), kept.end());
        }
        else if (left_out != 0)
        {
            columns[0] = withNullMask(columns[0], static_cast<const cudf::bitmask_type *>(state->filter_mask.data()), left_out);
        }
    }

    state->derive(columns, rows);

    if (!state->reduce_runs.has_value() && state->runs_combine && !columns[0].has_nulls())
    {
        std::unique_ptr<cudf::table> runs = state->reduceRuns(columns);
        state->reduce_runs = static_cast<double>(runs->num_rows()) <= max_runs_per_row_to_reduce * static_cast<double>(rows);
        if (*state->reduce_runs)
        {
            state->gatherRuns(std::move(runs));
            return;
        }
    }
    else if (!state->reduce_runs.has_value())
    {
        state->reduce_runs = false;
    }
    else if (*state->reduce_runs)
    {
        state->gatherRuns(state->reduceRuns(columns));
        return;
    }

    state->group(cudf::table_view(columns));
}

__host__ size_t CudfGroupBy::finalize()
{
    if (state->finalized)
        throwGPUError("the groups were closed twice");

    if (state->collect && state->collected.size() == 1)
    {
        state->groups = std::move(state->collected.front());
        state->collected.clear();
    }
    else if (state->collect && !state->collected.empty())
    {
        std::vector<cudf::table_view> views;
        views.reserve(state->collected.size());
        for (const auto & table : state->collected)
            views.push_back(table->view());
        state->groups = guarded("concatenating " + std::to_string(views.size()) + " batches of collected rows", [&]
        {
            return cudf::concatenate(views, state->stream);
        });
        state->collected.clear();
    }
    else if (state->grouping)
    {
        auto [group_keys, results] = guarded("closing the groups", [&]
        {
            return std::move(*state->grouping).finalize_and_release(state->stream);
        });
        state->groups = state->tableOf(std::move(group_keys), std::move(results));
    }
    else if (!state->reduced_runs.empty())
    {
        auto [group_keys, results] = state->combineRuns();
        state->groups = state->tableOf(std::move(group_keys), std::move(results));
    }
    state->grouping.reset();
    state->finalized = true;

    if (!state->groups)
        return 0;

    state->variable_offsets.resize(state->keys.size());
    state->value_offsets.resize(state->collect ? state->values.size() : state->aggregates.size());
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

    const size_t num_values = state->collect ? state->values.size() : state->aggregates.size();
    if (index >= num_values)
        throwGPUError(("aggregate " + std::to_string(index) + " of " + std::to_string(num_values)).c_str());

    const GPUElementType result_type = state->collect ? state->values[index] : state->aggregates[index].result_type;
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
