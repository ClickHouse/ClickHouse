#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>

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

/// `streaming_groupby` encodes a row of a batch as its capacity plus the row's index, in a `cudf::size_type`.
constexpr size_t max_capacity = static_cast<size_t>(std::numeric_limits<cudf::size_type>::max());
constexpr size_t max_slice_rows = 64UL << 20;
/// The first hash table is for this many groups, and grows by half with the groups seen: a query of a few groups does not
/// pay for a table of a million, and one of many pays a few merges.
constexpr size_t min_capacity = 1UL << 16;

/// A batch of which a `WHERE` leaves out at least this share of the rows is compacted to the rows it keeps before they
/// are grouped. `streaming_groupby` still goes over a row left out as a null of the first key, while compacting reads
/// every row once and writes those kept.
constexpr double min_left_out_share_to_compact = 0.25;

/// The batches are reduced over their runs of equal keys before they are grouped where the first batch has at most this
/// many runs per row, see `StreamingGroupBy::reduce_runs`.
constexpr double max_runs_per_row_to_reduce = 0.5;

/// The reduced runs gathered are combined into their groups once they take more than this, or twice what they took
/// after they were combined last, whichever is more, or once they reach this many rows, below what cuDF concatenates.
/// The runs gathered stay on the device until then, and combining them concatenates them into a copy as large: with
/// 4 GiB, TPC-H Q1 over SF100, of 4 groups but runs of a fourth of its rows, took 10 GiB of the device with one reader.
constexpr size_t min_bytes_to_combine_runs = 256UL << 20;
constexpr size_t max_rows_to_combine_runs = 1UL << 30;

__host__ cudf::column_view withNullMask(const cudf::column_view & column, const cudf::bitmask_type * mask, cudf::size_type null_count)
{
    std::vector<cudf::column_view> children(column.child_begin(), column.child_end());
    return cudf::column_view(column.type(), column.size(), column.head(), mask, null_count, column.offset(), children);
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

/// The `CudfGroupBy` of `createCudfGroupBy`.
class StreamingGroupBy final : public CudfGroupBy
{
public:
    __host__ StreamingGroupBy(
        GPUSpan<GPUElementType> keys_,
        GPUSpan<GPUElementType> values_,
        GPUSpan<GPUGroupByAggregate> aggregates_,
        cuda::stream_ref stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_.begin(), values_.end())
        , aggregates(aggregates_.begin(), aggregates_.end())
        , stream(stream_)
        , keyless(keys_.empty())
        , keyless_key(0, stream_)
        , all_true(0, stream_)
    {
        if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
            throwGPUError("a `GROUP BY` through cuDF on the device's default stream, where cuco cannot copy its counts back");

        if (values.empty())
            throwGPUError("a `GROUP BY` without values");

        if (aggregates.empty())
            throwGPUError("a `GROUP BY` without aggregates");

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

        key_indices.resize(numKeyColumns());
        for (size_t i = 0; i < key_indices.size(); ++i)
            key_indices[i] = static_cast<cudf::size_type>(i);
        runs_combine = std::all_of(
            aggregates.begin(), aggregates.end(), [](const GPUGroupByAggregate & aggregate) { return aggregate.aggregation != GPUAggregationKind::Mean; });
        streams = std::none_of(aggregates.begin(), aggregates.end(), [&](const GPUGroupByAggregate & aggregate)
        {
            return (aggregate.aggregation == GPUAggregationKind::Min || aggregate.aggregation == GPUAggregationKind::Max)
                && columnKindOf(values[aggregate.column]) == GPUColumnKind::Variable;
        });
        if (!streams && !runs_combine)
            throwGPUError("a `GROUP BY` with an `avg` and a `min` or a `max` of values of varying width, which combine from partial results that an `avg`'s do not");
    }

    __host__ void addBatch(GPUSpan<DeviceColumnView> batch_keys, GPUSpan<DeviceColumnView> batch_values, const uint32_t * kept) override
    {
        if (finalized)
            throwGPUError("a batch after the groups were closed");

        if (batch_keys.size() != keys.size() || batch_values.size() != values.size())
            throwGPUError(
                ("a batch of " + std::to_string(batch_keys.size()) + " keys and " + std::to_string(batch_values.size()) + " values, expected "
                + std::to_string(keys.size()) + " and " + std::to_string(values.size())).c_str());

        size_t rows = batch_keys.empty() ? batch_values[0].rows() : batch_keys[0].rows();
        if (rows == 0)
            return;

        std::vector<cudf::column_view> columns;
        columns.reserve(numKeyColumns() + values.size());

        if (keyless)
            columns.push_back(keylessKey(rows));

        for (size_t i = 0; i < batch_keys.size(); ++i)
        {
            if (batch_keys[i].rows() != rows)
                throwGPUError(("key " + std::to_string(i) + " of " + std::to_string(batch_keys[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
            columns.push_back(columnViewOf(batch_keys[i], keys[i]));
        }

        for (size_t i = 0; i < batch_values.size(); ++i)
        {
            if (batch_values[i].rows() != rows)
                throwGPUError(("value " + std::to_string(i) + " of " + std::to_string(batch_values[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
            columns.push_back(columnViewOf(batch_values[i], values[i]));
        }

        /// Lives until the groups are made of its rows.
        std::unique_ptr<cudf::table> compacted;
        if (kept)
        {
            const auto * mask = reinterpret_cast<const cudf::bitmask_type *>(kept);
            const cudf::size_type left_out = leftOut(mask, rows);
            if (static_cast<size_t>(left_out) == rows)
                return;

            /// Runs are reduced over batches without nulls only, as cuDF sorts keys with nulls whatever it is told.
            if (left_out != 0
                && (reduce_runs.value_or(runs_combine) || static_cast<double>(left_out) >= min_left_out_share_to_compact * static_cast<double>(rows)))
            {
                compacted = compact(columns, mask, rows, left_out);
                rows = static_cast<size_t>(compacted->num_rows());
                const cudf::table_view kept_rows = compacted->view();
                columns.assign(kept_rows.begin(), kept_rows.end());
            }
            else if (left_out != 0)
            {
                columns[0] = withNullMask(columns[0], mask, left_out);
            }
        }

        if (!reduce_runs.has_value() && runs_combine && !columns[0].has_nulls())
        {
            std::unique_ptr<cudf::table> runs = reduceRuns(columns);
            reduce_runs = static_cast<double>(runs->num_rows()) <= max_runs_per_row_to_reduce * static_cast<double>(rows);
            /// Partial results either way, which are many of an unsorted batch, but only the first's.
            if (*reduce_runs || !streams)
            {
                gatherRuns(std::move(runs));
                return;
            }
        }
        else if (!reduce_runs.has_value())
        {
            reduce_runs = false;
        }
        else if (*reduce_runs)
        {
            gatherRuns(reduceRuns(columns));
            return;
        }

        if (!streams)
        {
            gatherRuns(reduceRuns(columns, cudf::sorted::NO));
            return;
        }

        group(cudf::table_view(columns));
    }

    __host__ size_t finalize() override
    {
        if (finalized)
            throwGPUError("the groups were closed twice");

        if (grouping)
        {
            auto [group_keys, results] = guarded("closing the groups", [&]
            {
                return std::move(*grouping).finalize_and_release(stream);
            });
            groups = withoutKeylessKey(tableOf(std::move(group_keys), std::move(results)));
        }
        else if (!reduced_runs.empty())
        {
            auto [group_keys, results] = combineRuns();
            groups = withoutKeylessKey(tableOf(std::move(group_keys), std::move(results)));
        }
        grouping.reset();
        finalized = true;

        if (!groups)
            return 0;

        variable_offsets.resize(keys.size());
        value_offsets.resize(aggregates.size());
        return static_cast<size_t>(groups->num_rows());
    }

    __host__ size_t keepGroups(const uint32_t * kept) override
    {
        if (!finalized)
            throwGPUError("the groups were filtered before they were closed");

        if (!groups || groups->num_rows() == 0)
            return 0;

        const cudf::table_view all = groups->view();
        const auto rows = static_cast<size_t>(all.num_rows());
        const auto * mask = reinterpret_cast<const cudf::bitmask_type *>(kept);
        const cudf::size_type left_out = leftOut(mask, rows);
        if (left_out != 0)
            groups = compact(std::vector<cudf::column_view>(all.begin(), all.end()), mask, rows, left_out);
        return static_cast<size_t>(groups->num_rows());
    }

    __host__ DeviceColumnView key(size_t index) const override
    {
        if (!finalized)
            throwGPUError("the groups were asked for before they were closed");

        if (index >= keys.size())
            throwGPUError(("key " + std::to_string(index) + " of " + std::to_string(keys.size())).c_str());

        const GPUElementType type = keys[index];
        switch (columnKindOf(type))
        {
            case GPUColumnKind::Fixed:
                if (!groups)
                    return DeviceFixedColumn{.element_type = type};
                return deviceViewOf(groups->view().column(static_cast<cudf::size_type>(index)), type);
            case GPUColumnKind::Variable:
            {
                if (!groups)
                    return DeviceVariableColumn{};
                DeviceVariableColumnOfCudf of_cudf = deviceViewOfVariable(groups->view().column(static_cast<cudf::size_type>(index)), stream);
                variable_offsets[index] = std::move(of_cudf.widened_offsets);
                return of_cudf.view;
            }
        }
        throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(type))).c_str());
    }

    __host__ DeviceColumnView value(size_t index) const override
    {
        if (!finalized)
            throwGPUError("the groups were asked for before they were closed");

        if (index >= aggregates.size())
            throwGPUError(("aggregate " + std::to_string(index) + " of " + std::to_string(aggregates.size())).c_str());

        const GPUElementType result_type = aggregates[index].result_type;
        const auto column_index = static_cast<cudf::size_type>(keys.size() + index);
        switch (columnKindOf(result_type))
        {
            case GPUColumnKind::Fixed:
            {
                if (!groups)
                    return DeviceFixedColumn{.element_type = result_type};

                /// Taken by its bits, as `tableOf` checked it is as wide as `result_type`: cuDF sums unsigned integers into
                /// signed ones.
                const cudf::column_view column = groups->view().column(column_index);
                checkNoNulls(column);
                if (column.offset() != 0)
                    throwGPUError("the device returned a value of the groups as a slice");
                return DeviceFixedColumn{.element_type = result_type, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
            }
            case GPUColumnKind::Variable:
            {
                if (!groups)
                    return DeviceVariableColumn{};
                DeviceVariableColumnOfCudf of_cudf = deviceViewOfVariable(groups->view().column(column_index), stream);
                value_offsets[index] = std::move(of_cudf.widened_offsets);
                return of_cudf.view;
            }
        }
        throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(result_type))).c_str());
    }

private:
    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const cuda::stream_ref stream;

    std::vector<cudf::size_type> key_indices;

    /// Sized for `capacity` groups, and made again for more of them by `grow`.
    std::optional<cudf::groupby::streaming_groupby> grouping;
    size_t capacity = 0;
    /// At least as many as the groups of `grouping`: the groups it had when it was asked last, which waits for the
    /// device, and the rows grouped since.
    size_t max_groups = 0;

    /// The keys, then the values, once the groups are closed.
    std::unique_ptr<cudf::table> groups;

    /// Whether the partial results of every aggregate over some of the rows of a group combine by an aggregate: those of
    /// a `sum`, a `min` and a `max` by the same one and those of a `count` by a `sum`, but not those of an `avg`.
    bool runs_combine = false;

    /// Whether `streaming_groupby` takes the aggregates, which it does not of a `min` or a `max` of values of varying
    /// width. Without it, each batch is reduced into partial results of its groups by a groupby of cuDF, which are
    /// gathered and combined as the runs are.
    bool streams = true;

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

    /// Without keys but with aggregates, the rows are grouped by a key of their own, the same for every row, which comes
    /// first among the columns grouped and is gone from the groups once they are closed: every batch is then one run.
    const bool keyless;
    /// As many zero bytes as the largest batch had rows: the values of that key.
    rmm::device_buffer keyless_key;

    /// As many `true` bytes as the most rows compacted: the values of the `BOOL8` column whose nulls are what a mask
    /// leaves out.
    rmm::device_buffer all_true;

    /// Of the keys and of the values of varying width: their offsets widened for a view of the device's.
    mutable std::vector<std::unique_ptr<cudf::column>> variable_offsets;
    mutable std::vector<std::unique_ptr<cudf::column>> value_offsets;
    bool finalized = false;


    /// The columns grouped by: the keys, or the key of its own of a `GROUP BY` without keys.
    __host__ size_t numKeyColumns() const { return keys.size() + (keyless ? 1 : 0); }

    /// The key of a `GROUP BY` without keys, of `rows` rows.
    __host__ cudf::column_view keylessKey(size_t rows)
    {
        if (keyless_key.size() < rows)
        {
            keyless_key.resize(rows, stream);
            checkCuda(cudaMemsetAsync(keyless_key.data(), 0, rows, stream.get()), "Cannot fill the key of a `GROUP BY` without keys");
        }
        return cudf::column_view(cudf::data_type{cudf::type_id::INT8}, static_cast<cudf::size_type>(rows), keyless_key.data(), nullptr, 0);
    }

    /// The rows of the first `rows` that `mask` leaves out.
    __host__ cudf::size_type leftOut(const cudf::bitmask_type * mask, size_t rows) const
    {
        return guarded("counting the rows a filter leaves out", [&]
        {
            return cudf::null_count(mask, 0, static_cast<cudf::size_type>(rows), stream);
        });
    }

    /// The rows of `columns` that `mask` keeps.
    __host__ std::unique_ptr<cudf::table> compact(
        const std::vector<cudf::column_view> & columns, const cudf::bitmask_type * mask, size_t rows, cudf::size_type left_out)
    {
        if (all_true.size() < rows)
        {
            all_true.resize(rows, stream);
            checkCuda(cudaMemsetAsync(all_true.data(), 1, rows, stream.get()), "Cannot fill a mask of the device");
        }

        const cudf::column_view kept(cudf::data_type{cudf::type_id::BOOL8}, static_cast<cudf::size_type>(rows), all_true.data(), mask, left_out);
        return guarded("compacting " + std::to_string(rows) + " rows to those a filter keeps", [&]
        {
            return cudf::apply_retention_mask(cudf::table_view(columns), kept, stream);
        });
    }

    /// The groups of `table`, the keys and then the values, without the key of a `GROUP BY` without keys.
    __host__ std::unique_ptr<cudf::table> withoutKeylessKey(std::unique_ptr<cudf::table> table) const
    {
        if (!keyless)
            return table;
        std::vector<std::unique_ptr<cudf::column>> columns = table->release();
        columns.erase(columns.begin());
        return std::make_unique<cudf::table>(std::move(columns));
    }

    __host__ cudf::groupby::streaming_groupby make(size_t new_capacity) const
    {
        /// A `count` counts the rows of the first key, which has no nulls.
        std::vector<cudf::groupby::streaming_aggregation_request> requests;
        requests.reserve(aggregates.size());
        for (const auto & aggregate : aggregates)
            requests.push_back({
                .column_index = aggregate.column == GPUGroupByAggregate::no_column ? 0 : static_cast<cudf::size_type>(numKeyColumns() + aggregate.column),
                .aggregation = groupByAggregationFor(aggregate.aggregation),
            });

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
            const size_t slice_rows = std::min(rows - offset, max_slice_rows);
            const auto begin = static_cast<cudf::size_type>(offset);
            const auto end = static_cast<cudf::size_type>(offset + slice_rows);
            const cudf::table_view slice = begin == 0 && end == table.num_rows() ? table : cudf::slice(table, {begin, end}).front();

            if (grouping && max_groups + slice_rows > capacity)
                max_groups = static_cast<size_t>(grouping->distinct_keys());

            if (!grouping || max_groups + slice_rows > capacity)
            {
                /// Grown by half, with a quarter to spare: its hash table holds twice as many slots as groups.
                const size_t new_capacity = std::max({capacity + capacity / 2, (max_groups + slice_rows) + (max_groups + slice_rows) / 4, min_capacity});
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
                    "grouping " + std::to_string(slice_rows) + " rows by " + std::to_string(numKeyColumns()) + " keys",
                    [&] { grouping->aggregate(slice, stream); return 0; });
            }
            max_groups += slice_rows;
            offset += slice_rows;
        }
    }

    /// The keys of each run of equal keys of `columns`, the keys and the values of a batch, and the aggregates over the run. cuDF's sort-based groupby, told the keys are sorted, starts a group
    /// wherever a row's keys differ from the row's before, so that a key in several runs comes once for each. Told they
    /// are not, it groups them by a hash table, and a run is all the rows of a key in the batch, of a key that is not null.
    __host__ std::unique_ptr<cudf::table> reduceRuns(const std::vector<cudf::column_view> & columns, cudf::sorted sorted = cudf::sorted::YES) const
    {
        const cudf::table_view batch_keys(std::vector<cudf::column_view>(columns.begin(), columns.begin() + numKeyColumns()));
        cudf::groupby::groupby by_runs(batch_keys, cudf::null_policy::EXCLUDE, sorted);

        std::vector<cudf::groupby::aggregation_request> requests(aggregates.size());
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const GPUGroupByAggregate & aggregate = aggregates[i];
            requests[i].values = aggregate.column == GPUGroupByAggregate::no_column ? columns[0] : columns[numKeyColumns() + aggregate.column];
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

        std::vector<cudf::size_type> key_columns(numKeyColumns());
        for (size_t i = 0; i < key_columns.size(); ++i)
            key_columns[i] = static_cast<cudf::size_type>(i);
        cudf::groupby::groupby by_keys(all.select(key_columns), cudf::null_policy::EXCLUDE);

        /// The partial results of a `count` are summed, and those of the others taken by the same aggregate.
        std::vector<cudf::groupby::aggregation_request> requests(aggregates.size());
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const GPUAggregationKind kind = aggregates[i].aggregation;
            requests[i].values = all.column(static_cast<cudf::size_type>(numKeyColumns() + i));
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
};

}

__host__ CudfGroupBy::~CudfGroupBy() = default;

__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, cuda::stream_ref stream)
{
    return new StreamingGroupBy(keys, values, aggregates, stream);
}

}
