#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>

#include <cudf/binaryop.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/groupby.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/unary.hpp>

#include <rmm/device_buffer.hpp>

#include <algorithm>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace DB::GPU
{

namespace
{

/// Below this share of rows left out by `WHERE`, they become null keys rather than being compacted.
constexpr double min_left_out_share_to_compact = 0.25;

constexpr double max_runs_per_row_to_reduce = 0.5;
constexpr double min_groups_per_run_to_reduce = 0.25;

/// `streaming_groupby` warps aggregate their rows before updating the few groups.
constexpr size_t max_groups_to_stream = 1UL << 16;

/// `streaming_groupby` keeps its capacity plus a row index, and counts, in a `cudf::size_type`.
constexpr size_t max_streaming_rows = std::numeric_limits<cudf::size_type>::max();

/// With 4 GiB, TPC-H Q1 took 10 GiB of the device.
constexpr size_t min_bytes_to_combine_runs = 256UL << 20;
constexpr size_t max_rows_to_combine_runs = 1UL << 30;

static_assert(std::is_same_v<cudf::bitmask_type, uint32_t>);

__host__ cudf::column_view withNullMask(const cudf::column_view & column, const cudf::bitmask_type * mask, cudf::size_type null_count)
{
    std::vector<cudf::column_view> children(column.child_begin(), column.child_end());
    return cudf::column_view(column.type(), column.size(), column.head(), mask, null_count, column.offset(), children);
}

__host__ void waitFor(cuda::stream_ref waiting, cuda::stream_ref stream)
{
    cudaEvent_t event = nullptr;
    checkCuda(cudaEventCreateWithFlags(&event, cudaEventDisableTiming), "Cannot create a CUDA event");
    const cudaError_t recorded = cudaEventRecord(event, stream.get());
    const cudaError_t waited = recorded == cudaSuccess ? cudaStreamWaitEvent(waiting.get(), event, 0) : recorded;
    cudaEventDestroy(event);
    checkCuda(waited, "Cannot make a stream wait for another");
}

/// Widened offsets go into `offsets`, which must outlive the view.
__host__ DeviceColumnView viewOf(
    const cudf::table * table, size_t index, GPUElementType type, std::unique_ptr<cudf::column> & offsets, cuda::stream_ref stream)
{
    switch (columnKindOf(type))
    {
        case GPUColumnKind::Fixed:
            if (!table)
                return DeviceFixedColumn{.element_type = type};
            return deviceViewByBits(table->view().column(static_cast<cudf::size_type>(index)), type);
        case GPUColumnKind::Variable:
        {
            if (!table)
                return DeviceVariableColumn{};
            DeviceVariableColumnOfCudf of_cudf = deviceViewOfVariable(table->view().column(static_cast<cudf::size_type>(index)), stream);
            offsets = std::move(of_cudf.widened_offsets);
            return of_cudf.view;
        }
    }
    throwGPUError(("unknown column kind of element type " + std::to_string(static_cast<int>(type))).c_str());
}

__host__ std::vector<cudf::column_view> columnsOf(const GPUSpan<DeviceColumnView> & batch, const std::vector<GPUElementType> & types, size_t rows)
{
    std::vector<cudf::column_view> columns;
    columns.reserve(batch.size());
    for (size_t i = 0; i < batch.size(); ++i)
    {
        if (batch[i].rows() != rows)
            throwGPUError(("column " + std::to_string(i) + " of " + std::to_string(batch[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
        columns.push_back(columnViewOf(batch[i], types[i]));
    }
    return columns;
}

}

namespace cudf_group_by
{

/// `avg` is a `sum` and a `count`; a table of partial results is the keys, then each partial.
class PartialAggregates
{
public:
    __host__ PartialAggregates(GPUSpan<GPUGroupByAggregate> aggregates_, GPUSpan<GPUElementType> values_, size_t num_key_columns_, cuda::stream_ref stream_)
        : aggregates(aggregates_.begin(), aggregates_.end()), values(values_.begin(), values_.end()), num_key_columns(num_key_columns_), stream(stream_)
    {
        for (size_t i = 0; i < num_key_columns; ++i)
            key_indices.push_back(static_cast<cudf::size_type>(i));

        for (const auto & aggregate : aggregates)
        {
            if (aggregate.aggregation == GPUAggregationKind::Mean)
                of.emplace_back(indexOf(GPUAggregationKind::Sum, aggregate.column), indexOf(GPUAggregationKind::Count, GPUGroupByAggregate::no_column));
            else
                of.emplace_back(indexOf(aggregate.aggregation, aggregate.column), std::nullopt);
        }
    }

    __host__ bool operator==(const PartialAggregates & other) const
    {
        return num_key_columns == other.num_key_columns && partials == other.partials && aggregates.size() == other.aggregates.size();
    }

    __host__ const std::vector<cudf::size_type> & keyIndices() const { return key_indices; }

    /// cuDF does not stream `min` or `max` of variable-width values.
    __host__ bool streams() const
    {
        return std::none_of(partials.begin(), partials.end(), [&](const Partial & partial)
        {
            return (partial.aggregation == GPUAggregationKind::Min || partial.aggregation == GPUAggregationKind::Max)
                && columnKindOf(values[partial.column]) == GPUColumnKind::Variable;
        });
    }

    __host__ std::vector<cudf::groupby::streaming_aggregation_request> streamingRequests() const
    {
        std::vector<cudf::groupby::streaming_aggregation_request> requests;
        requests.reserve(partials.size());
        for (const auto & partial : partials)
            requests.push_back({.column_index = columnOf(partial), .aggregation = aggregationOf(partial.aggregation)});
        return requests;
    }

    /// Rows with null keys, which a `WHERE` left out, are excluded.
    __host__ std::unique_ptr<cudf::table> reduce(const std::vector<cudf::column_view> & columns, cudf::sorted sorted) const
    {
        const cudf::table_view batch_keys(std::vector<cudf::column_view>(columns.begin(), columns.begin() + num_key_columns));
        cudf::groupby::groupby by_keys(batch_keys, cudf::null_policy::EXCLUDE, sorted);

        std::vector<cudf::groupby::aggregation_request> requests(partials.size());
        for (size_t i = 0; i < partials.size(); ++i)
        {
            requests[i].values = columns[static_cast<size_t>(columnOf(partials[i]))];
            requests[i].aggregations.push_back(aggregationOf(partials[i].aggregation));
        }

        auto [group_keys, results] = guarded("reducing " + std::to_string(batch_keys.num_rows()) + " rows into partial results of their groups", [&]
        {
            return by_keys.aggregate(requests, stream);
        });
        return tableOf(std::move(group_keys), std::move(results));
    }

    __host__ std::unique_ptr<cudf::table> combine(const std::vector<cudf::table_view> & tables, size_t rows) const
    {
        std::unique_ptr<cudf::table> concatenated;
        if (tables.size() > 1)
            concatenated = guarded("concatenating " + std::to_string(rows) + " partial results of groups", [&]
            {
                return cudf::concatenate(tables, stream);
            });
        const cudf::table_view all = concatenated ? concatenated->view() : tables.front();

        cudf::groupby::groupby by_keys(all.select(key_indices), cudf::null_policy::EXCLUDE);
        std::vector<cudf::groupby::aggregation_request> requests(partials.size());
        for (size_t i = 0; i < partials.size(); ++i)
        {
            const GPUAggregationKind kind = partials[i].aggregation;
            requests[i].values = all.column(static_cast<cudf::size_type>(num_key_columns + i));
            requests[i].aggregations.push_back(aggregationOf(kind == GPUAggregationKind::Count ? GPUAggregationKind::Sum : kind));
        }

        auto [group_keys, results] = guarded("combining " + std::to_string(rows) + " partial results into their groups", [&]
        {
            return by_keys.aggregate(requests, stream);
        });
        return tableOf(std::move(group_keys), std::move(results));
    }

    __host__ std::unique_ptr<cudf::table> tableOf(std::unique_ptr<cudf::table> group_keys, std::vector<cudf::groupby::aggregation_result> results) const
    {
        std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();
        for (size_t i = 0; i < partials.size(); ++i)
        {
            std::unique_ptr<cudf::column> & result = results[i].results.front();
            /// Widened to the type of the sums, so that the partial results concatenate.
            if (partials[i].aggregation == GPUAggregationKind::Count && result->type().id() != cudf::type_id::INT64)
                result = guarded("widening a count", [&] { return cudf::cast(result->view(), cudf::data_type{cudf::type_id::INT64}, stream); });
            columns.push_back(std::move(result));
        }
        return std::make_unique<cudf::table>(std::move(columns));
    }

    __host__ std::unique_ptr<cudf::table> resultsOf(std::unique_ptr<cudf::table> combined, size_t dropped_keys) const
    {
        std::vector<std::unique_ptr<cudf::column>> partial_columns = combined->release();
        const auto partial = [&](size_t index) -> cudf::column_view { return partial_columns[num_key_columns + index]->view(); };

        /// `avg`s first, as they only read the partials that the other aggregates then take.
        std::vector<std::unique_ptr<cudf::column>> results(aggregates.size());
        std::vector<size_t> uses(partials.size(), 0);
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const auto [first, second] = of[i];
            if (!second)
            {
                ++uses[first];
                continue;
            }
            results[i] = guarded("dividing the sums of an `avg` by its counts", [&]
            {
                return cudf::binary_operation(partial(first), partial(*second), cudf::binary_operator::TRUE_DIV, cudf::data_type{cudf::type_id::FLOAT64}, stream);
            });
        }
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const auto [first, second] = of[i];
            if (second)
                continue;
            results[i] = --uses[first] == 0 ? std::move(partial_columns[num_key_columns + first])
                                            : std::make_unique<cudf::column>(partial(first), stream);
        }

        std::vector<std::unique_ptr<cudf::column>> columns;
        columns.reserve(num_key_columns + aggregates.size());
        for (size_t i = dropped_keys; i < num_key_columns; ++i)
            columns.push_back(std::move(partial_columns[i]));
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const GPUElementType result_type = aggregates[i].result_type;
            const bool as_wide = columnKindOf(result_type) == GPUColumnKind::Variable
                ? results[i]->type().id() == cudf::type_id::STRING
                : cudf::size_of(results[i]->type()) == sizeOf(result_type);
            if (!as_wide)
                throwGPUError(
                    ("the device reduced aggregate " + std::to_string(i) + " into cuDF type "
                    + std::to_string(static_cast<int32_t>(results[i]->type().id())) + ", which is not as wide as element type "
                    + std::to_string(static_cast<int>(result_type))).c_str());
            columns.push_back(std::move(results[i]));
        }
        return std::make_unique<cudf::table>(std::move(columns));
    }

private:
    struct Partial
    {
        GPUAggregationKind aggregation;
        uint32_t column;

        bool operator==(const Partial &) const = default;
    };

    __host__ size_t indexOf(GPUAggregationKind aggregation, uint32_t column)
    {
        const Partial partial{.aggregation = aggregation, .column = column};
        const auto it = std::find(partials.begin(), partials.end(), partial);
        if (it != partials.end())
            return static_cast<size_t>(it - partials.begin());
        partials.push_back(partial);
        return partials.size() - 1;
    }

    /// A `count` counts the first key, which has nulls only where a `WHERE` left rows out.
    __host__ cudf::size_type columnOf(const Partial & partial) const
    {
        return partial.column == GPUGroupByAggregate::no_column ? 0 : static_cast<cudf::size_type>(num_key_columns + partial.column);
    }

    __host__ static std::unique_ptr<cudf::groupby_aggregation> aggregationOf(GPUAggregationKind aggregation)
    {
        switch (aggregation)
        {
            case GPUAggregationKind::Sum: return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
            case GPUAggregationKind::Min: return cudf::make_min_aggregation<cudf::groupby_aggregation>();
            case GPUAggregationKind::Max: return cudf::make_max_aggregation<cudf::groupby_aggregation>();
            case GPUAggregationKind::Count: return cudf::make_count_aggregation<cudf::groupby_aggregation>(cudf::null_policy::INCLUDE);
            case GPUAggregationKind::Mean: break;
        }
        throwGPUError(("no partial aggregation " + std::to_string(static_cast<int>(aggregation))).c_str());
    }

    const std::vector<GPUGroupByAggregate> aggregates;
    const std::vector<GPUElementType> values;
    const size_t num_key_columns;
    const cuda::stream_ref stream;
    std::vector<cudf::size_type> key_indices;
    std::vector<Partial> partials;
    /// Per aggregate: its partial, or for an `avg`, its `sum` and its `count`.
    std::vector<std::pair<size_t, std::optional<size_t>>> of;
};

class Parts
{
public:
    __host__ Parts(const PartialAggregates & aggregates_, size_t num_parts, cuda::stream_ref stream_)
        : aggregates(aggregates_), parts(num_parts), stream(stream_)
    {
    }

    __host__ size_t size() const { return parts.size(); }

    __host__ void add(std::unique_ptr<cudf::table> partial)
    {
        if (partial->num_rows() == 0)
            return;

        if (parts.size() == 1)
        {
            gather(parts.front(), std::move(partial));
            return;
        }

        auto [pieces, offsets] = guarded(
            "splitting " + std::to_string(partial->num_rows()) + " partial results into " + std::to_string(parts.size()) + " parts by their keys", [&]
            {
                return cudf::hash_partition(
                    partial->view(), aggregates.keyIndices(), static_cast<int>(parts.size()), cudf::hash_id::HASH_MURMUR3, cudf::DEFAULT_HASH_SEED, stream);
            });
        offsets.push_back(pieces->num_rows());

        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (offsets[i] == offsets[i + 1])
                continue;
            gather(parts[i], guarded("copying a part of partial results", [&]
            {
                return std::make_unique<cudf::table>(cudf::slice(pieces->view(), {offsets[i], offsets[i + 1]}).front(), stream);
            }));
        }
    }

    __host__ std::vector<std::unique_ptr<cudf::table>> copiesOf(size_t part, cuda::stream_ref to_stream) const
    {
        if (part >= parts.size())
            throwGPUError(("part " + std::to_string(part) + " of partial results split into " + std::to_string(parts.size()) + " parts").c_str());

        std::vector<std::unique_ptr<cudf::table>> copies;
        copies.reserve(parts[part].tables.size());
        for (const auto & table : parts[part].tables)
            copies.push_back(guarded("copying a part of partial results", [&] { return std::make_unique<cudf::table>(table->view(), to_stream); }));
        return copies;
    }

    __host__ std::unique_ptr<cudf::table> combineAll()
    {
        std::vector<cudf::table_view> views;
        size_t rows = 0;
        for (const auto & part : parts)
        {
            for (const auto & table : part.tables)
            {
                views.push_back(table->view());
                rows += static_cast<size_t>(table->num_rows());
            }
        }
        if (views.empty())
            return nullptr;

        auto groups = aggregates.combine(views, rows);
        /// Freed in the order of the stream, after the groupby has read them.
        for (auto & part : parts)
            part = {};
        return groups;
    }

private:
    struct Runs
    {
        std::vector<std::unique_ptr<cudf::table>> tables;
        size_t rows = 0;
        size_t bytes = 0;
        size_t combined_bytes = 0;
    };

    /// Combining at twice the size after the last combine keeps memory within about twice the groups.
    __host__ void gather(Runs & runs, std::unique_ptr<cudf::table> table)
    {
        runs.rows += static_cast<size_t>(table->num_rows());
        runs.bytes += table->alloc_size();
        runs.tables.push_back(std::move(table));

        const size_t min_bytes = min_bytes_to_combine_runs / parts.size();
        if (runs.tables.size() < 2 || (runs.bytes <= std::max(min_bytes, 2 * runs.combined_bytes) && runs.rows < max_rows_to_combine_runs))
            return;

        std::vector<cudf::table_view> views;
        for (const auto & t : runs.tables)
            views.push_back(t->view());
        auto groups = aggregates.combine(views, runs.rows);
        runs.tables.clear();
        runs.combined_bytes = groups->alloc_size();
        runs.rows = static_cast<size_t>(groups->num_rows());
        runs.bytes = runs.combined_bytes;
        runs.tables.push_back(std::move(groups));
    }

    const PartialAggregates & aggregates;
    std::vector<Runs> parts;
    const cuda::stream_ref stream;
};

/// Moved into the parts before it might overflow, and made again: `streaming_groupby` never grows.
class StreamingPartials
{
public:
    __host__ StreamingPartials(const PartialAggregates & aggregates_, Parts & parts_, cuda::stream_ref stream_)
        : aggregates(aggregates_), parts(parts_), stream(stream_)
    {
    }

    __host__ void add(const cudf::table_view & table)
    {
        const size_t rows = static_cast<size_t>(table.num_rows());
        if (grouping && rows_grouped + rows > max_streaming_rows)
            close();
        if (grouping && max_groups + rows > capacity)
            max_groups = static_cast<size_t>(grouping->distinct_keys());
        if (grouping && max_groups + rows > capacity)
            close();
        if (!grouping)
        {
            capacity = rows + rows / 4 + max_groups_to_stream;
            if (capacity + rows > max_streaming_rows)
                throwGPUError(("a batch of " + std::to_string(rows) + " rows for a `streaming_groupby`").c_str());
            grouping.emplace(guarded("making a `streaming_groupby`", [&]
            {
                return cudf::groupby::streaming_groupby(
                    aggregates.keyIndices(), aggregates.streamingRequests(), static_cast<cudf::size_type>(capacity), cudf::null_policy::EXCLUDE);
            }));
        }

        guarded("grouping into a `streaming_groupby`", [&] { grouping->aggregate(table, stream); return 0; });
        max_groups += rows;
        rows_grouped += rows;
    }

    __host__ void close()
    {
        if (!grouping)
            return;

        auto [group_keys, results] = guarded("closing a `streaming_groupby` into partial results", [&]
        {
            return std::move(*grouping).finalize_and_release(stream);
        });
        grouping.reset();
        capacity = 0;
        max_groups = 0;
        rows_grouped = 0;
        parts.add(aggregates.tableOf(std::move(group_keys), std::move(results)));
    }

private:
    const PartialAggregates & aggregates;
    Parts & parts;
    const cuda::stream_ref stream;

    std::optional<cudf::groupby::streaming_groupby> grouping;
    size_t capacity = 0;
    size_t max_groups = 0;
    size_t rows_grouped = 0;
};

}

using namespace cudf_group_by;

class StreamingGroupBy final : public CudfGroupBy
{
public:
    __host__ StreamingGroupBy(
        GPUSpan<GPUElementType> keys_, GPUSpan<GPUElementType> values_, GPUSpan<GPUGroupByAggregate> aggregates_, size_t num_parts, cuda::stream_ref stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_.begin(), values_.end())
        , aggregates(aggregates_.begin(), aggregates_.end())
        , stream(stream_)
        , keyless(keys_.empty())
        , keyless_key(0, stream_)
        , all_true(0, stream_)
        , partials(aggregates_, values_, numKeyColumns(), stream_)
        , parts(partials, num_parts, stream_)
        , streaming(partials, parts, stream_)
        , widened_offsets(keys_.size() + aggregates_.size())
    {
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
        for (auto & column : columnsOf(batch_keys, keys, rows))
            columns.push_back(column);
        for (auto & column : columnsOf(batch_values, values, rows))
            columns.push_back(column);

        /// Must live until the partial results are made of its rows.
        std::unique_ptr<cudf::table> compacted;
        if (kept)
        {
            const auto * mask = reinterpret_cast<const cudf::bitmask_type *>(kept);
            const cudf::size_type left_out = guarded("counting the rows a filter leaves out", [&]
            {
                return cudf::null_count(mask, 0, static_cast<cudf::size_type>(rows), stream);
            });
            if (static_cast<size_t>(left_out) == rows)
                return;

            /// Only batches without nulls, as cuDF sorts keys with nulls whatever it is told.
            const bool runs = reduction == Reduction::Undecided || reduction == Reduction::Runs;
            if (left_out != 0 && (runs || static_cast<double>(left_out) >= min_left_out_share_to_compact * static_cast<double>(rows)))
            {
                compacted = keptRows(cudf::table_view(columns), mask, left_out, all_true, stream);
                rows = static_cast<size_t>(compacted->num_rows());
                const cudf::table_view kept_rows = compacted->view();
                columns.assign(kept_rows.begin(), kept_rows.end());
            }
            else if (left_out != 0)
            {
                columns[0] = withNullMask(columns[0], mask, left_out);
            }
        }

        if (reduction == Reduction::Undecided)
            decide(columns, rows);
        else
            reduce(columns);
    }

    __host__ void absorbPart(const CudfGroupBy & other_group_by, size_t part) override
    {
        const auto * other = dynamic_cast<const StreamingGroupBy *>(&other_group_by);
        if (!other)
            throwGPUError("a part of a `GROUP BY` of another kind than `createCudfGroupBy` makes");
        if (finalized || parts.size() != 1)
            throwGPUError("a part of the groups of a `GROUP BY` absorbed into one that is not of one part");
        if (keyless != other->keyless || keys.size() != other->keys.size() || !(partials == other->partials))
            throwGPUError("a part of the groups of a `GROUP BY` absorbed into one of other keys or aggregates");

        {
            std::lock_guard lock(other->closing);
            other->streaming.close();
        }

        /// The originals are freed in the order of `other`'s stream.
        waitFor(stream, other->stream);
        auto copies = other->parts.copiesOf(part, stream);
        waitFor(other->stream, stream);
        for (auto & copy : copies)
            parts.add(std::move(copy));
    }

    __host__ size_t finalize() override
    {
        if (finalized)
            throwGPUError("the groups were closed twice");
        streaming.close();
        if (auto combined = parts.combineAll())
            groups = partials.resultsOf(std::move(combined), keyless ? 1 : 0);
        finalized = true;
        return groups ? static_cast<size_t>(groups->num_rows()) : 0;
    }

    __host__ size_t keepGroups(const uint32_t * kept) override
    {
        if (!finalized)
            throwGPUError("the groups were filtered before they were closed");
        if (!groups || groups->num_rows() == 0)
            return 0;

        const auto * mask = reinterpret_cast<const cudf::bitmask_type *>(kept);
        const cudf::size_type left_out = guarded("counting the groups a filter leaves out", [&]
        {
            return cudf::null_count(mask, 0, groups->num_rows(), stream);
        });
        if (left_out != 0)
            groups = keptRows(groups->view(), mask, left_out, all_true, stream);
        return static_cast<size_t>(groups->num_rows());
    }

    __host__ DeviceColumnView key(size_t index) const override
    {
        if (!finalized)
            throwGPUError("the groups were asked for before they were closed");
        if (index >= keys.size())
            throwGPUError(("key " + std::to_string(index) + " of " + std::to_string(keys.size())).c_str());
        return viewOf(groups.get(), index, keys[index], widened_offsets[index], stream);
    }

    __host__ DeviceColumnView value(size_t index) const override
    {
        if (!finalized)
            throwGPUError("the groups were asked for before they were closed");
        if (index >= aggregates.size())
            throwGPUError(("aggregate " + std::to_string(index) + " of " + std::to_string(aggregates.size())).c_str());
        const size_t column = keys.size() + index;
        return viewOf(groups.get(), column, aggregates[index].result_type, widened_offsets[column], stream);
    }

private:
    enum class Reduction
    {
        Undecided,
        Runs,
        HashTable,
        Streaming,
    };

    __host__ size_t numKeyColumns() const { return keys.size() + (keyless ? 1 : 0); }

    __host__ cudf::column_view keylessKey(size_t rows)
    {
        if (keyless_key.size() < rows)
        {
            keyless_key.resize(rows, stream);
            checkCuda(cudaMemsetAsync(keyless_key.data(), 0, rows, stream.get()), "Cannot fill the key of a `GROUP BY` without keys");
        }
        return cudf::column_view(cudf::data_type{cudf::type_id::INT8}, static_cast<cudf::size_type>(rows), keyless_key.data(), nullptr, 0);
    }

    __host__ void decide(const std::vector<cudf::column_view> & columns, size_t rows)
    {
        if (columns[0].has_nulls())
        {
            reduction = partials.streams() ? Reduction::Streaming : Reduction::HashTable;
            reduce(columns);
            return;
        }

        const auto runs = partials.reduce(columns, cudf::sorted::YES);
        const auto num_runs = static_cast<size_t>(runs->num_rows());
        auto batch_groups = partials.combine({runs->view()}, num_runs);
        const auto num_groups = static_cast<size_t>(batch_groups->num_rows());
        if (static_cast<double>(num_runs) <= max_runs_per_row_to_reduce * static_cast<double>(rows)
            && static_cast<double>(num_groups) > min_groups_per_run_to_reduce * static_cast<double>(num_runs))
            reduction = Reduction::Runs;
        else if (num_groups <= max_groups_to_stream && partials.streams())
            reduction = Reduction::Streaming;
        else
            reduction = Reduction::HashTable;
        parts.add(std::move(batch_groups));
    }

    __host__ void reduce(const std::vector<cudf::column_view> & columns)
    {
        switch (reduction)
        {
            case Reduction::Runs:
                parts.add(partials.reduce(columns, cudf::sorted::YES));
                return;
            case Reduction::HashTable:
                parts.add(partials.reduce(columns, cudf::sorted::NO));
                return;
            case Reduction::Streaming:
                streaming.add(cudf::table_view(columns));
                return;
            case Reduction::Undecided:
                break;
        }
        throwGPUError("a batch of a `GROUP BY` that did not decide how to reduce it");
    }

    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const cuda::stream_ref stream;
    const bool keyless;
    rmm::device_buffer keyless_key;
    rmm::device_buffer all_true;

    PartialAggregates partials;
    Reduction reduction = Reduction::Undecided;
    Parts parts;
    mutable StreamingPartials streaming;
    /// Taken by the first of those that take the parts, which closes `streaming` into them.
    mutable std::mutex closing;

    /// Of those of varying width, their offsets widened, see `viewOf`.
    std::unique_ptr<cudf::table> groups;
    mutable std::vector<std::unique_ptr<cudf::column>> widened_offsets;
    bool finalized = false;
};

__host__ CudfGroupBy::~CudfGroupBy() = default;

__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, size_t num_parts, cuda::stream_ref stream)
{
    if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
        throwGPUError("a `GROUP BY` through cuDF on the device's default stream, where cuco cannot copy its counts back");
    if (values.empty())
        throwGPUError("a `GROUP BY` without values");
    if (aggregates.empty())
        throwGPUError("a `GROUP BY` without aggregates");
    if (num_parts == 0 || num_parts > static_cast<size_t>(std::numeric_limits<int>::max()))
        throwGPUError(("a `GROUP BY` split into " + std::to_string(num_parts) + " parts").c_str());

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

    return new StreamingGroupBy(keys, values, aggregates, num_parts, stream);
}

}
