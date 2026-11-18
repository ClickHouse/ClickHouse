#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>
#include <GPU/GPUExpression.cuh>

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

/// A batch whose `WHERE` leaves out at least this share of rows is compacted; otherwise the rows left out are nulls of the
/// first key.
constexpr double min_left_out_share_to_compact = 0.25;

/// Batches are reduced over runs of equal keys (sorted groupby) where the first batch has at most this many runs per row
/// and at least this many groups per run, i.e. the part is sorted by many keys.
constexpr double max_runs_per_row_to_reduce = 0.5;
constexpr double min_groups_per_run_to_reduce = 0.25;

/// Where the first batch has at most this many groups, the batches are grouped into one `streaming_groupby`, whose
/// warps aggregate their rows before they update the few groups, rather than each by a groupby of its own.
constexpr size_t max_groups_to_stream = 1UL << 16;

/// `streaming_groupby` encodes a row of a batch as its capacity plus the row's index, in a `cudf::size_type`, and takes
/// at most as many rows at a time as it holds groups.
constexpr size_t max_capacity = static_cast<size_t>(std::numeric_limits<cudf::size_type>::max());
constexpr size_t max_slice_rows = 64UL << 20;

/// Partial results of a part are combined once they exceed this (divided by the number of parts) and twice their size
/// after the last combine, or this many rows. With 4 GiB, TPC-H Q1 took 10 GiB of the device.
constexpr size_t min_bytes_to_combine_runs = 256UL << 20;
constexpr size_t max_rows_to_combine_runs = 1UL << 30;

static_assert(std::is_same_v<cudf::bitmask_type, uint32_t>);

__host__ cudf::column_view withNullMask(const cudf::column_view & column, const cudf::bitmask_type * mask, cudf::size_type null_count)
{
    std::vector<cudf::column_view> children(column.child_begin(), column.child_end());
    return cudf::column_view(column.type(), column.size(), column.head(), mask, null_count, column.offset(), children);
}

/// Makes `waiting` wait for what `stream` has so far.
__host__ void waitFor(cuda::stream_ref waiting, cuda::stream_ref stream)
{
    cudaEvent_t event = nullptr;
    checkCuda(cudaEventCreateWithFlags(&event, cudaEventDisableTiming), "Cannot create a CUDA event");
    const cudaError_t recorded = cudaEventRecord(event, stream.get());
    const cudaError_t waited = recorded == cudaSuccess ? cudaStreamWaitEvent(waiting.get(), event, 0) : recorded;
    cudaEventDestroy(event);
    checkCuda(waited, "Cannot make a stream wait for another");
}

/// The inputs of a `WHERE` of a batch of `rows` rows.
__host__ std::vector<GPUExpressionInput> inputsOf(GPUSpan<DeviceFixedColumn> columns, size_t rows)
{
    std::vector<GPUExpressionInput> inputs;
    inputs.reserve(columns.size());
    for (const auto & column : columns)
    {
        if (column.rows != rows)
            throwGPUError(("a column of a `WHERE` of " + std::to_string(column.rows) + " rows in a batch of " + std::to_string(rows)).c_str());
        inputs.push_back({.data = column.data, .type = column.element_type});
    }
    return inputs;
}

/// Of a table of the device, its `index`-th column, as `type`: of one of varying width, its offsets widened go into
/// `offsets`, which keeps them while the view is used.
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

/// The pieces of `CudfGroupBy`, outside the anonymous namespace as its `State` holds them.
namespace cudf_group_by
{

/// The values the device computes for a batch from its other values, see `GPUDerivedValue`.
class DerivedValues
{
public:
    __host__ DerivedValues(GPUSpan<GPUDerivedValue> derived_, cuda::stream_ref stream_) : derived(derived_.begin(), derived_.end()), stream(stream_)
    {
        for (size_t i = 0; i < derived.size(); ++i)
            buffers.emplace_back(0, stream_);
    }

    __host__ size_t size() const { return derived.size(); }

    /// Appends to `columns`, of `rows` rows, the values derived from `batch_values`, the values of the batch.
    __host__ void derive(std::vector<cudf::column_view> & columns, const std::vector<DeviceFixedColumn> & batch_values, size_t rows)
    {
        for (size_t j = 0; j < derived.size(); ++j)
        {
            const GPUDerivedValue & value = derived[j];
            if (value.expression.result != GPUExpressionResult::Value)
                throwGPUError("a derived value of an expression that is a predicate");

            std::vector<GPUExpressionInput> inputs;
            inputs.reserve(value.columns.size());
            for (const uint32_t column : value.columns)
            {
                if (column >= batch_values.size() || batch_values[column].rows < rows)
                    throwGPUError(("a derived value of value column " + std::to_string(column) + ", which is not of the batch").c_str());
                inputs.push_back({.data = batch_values[column].data, .type = batch_values[column].element_type});
            }

            rmm::device_buffer & out = buffers[j];
            const GPUElementType type = value.expression.value_type;
            if (out.size() < rows * sizeOf(type))
                out.resize(rows * sizeOf(type), stream);
            evaluateGPUExpression(value.expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, out.data(), stream);
            columns.emplace_back(cudfTypeOf(type), static_cast<cudf::size_type>(rows), out.data(), nullptr, 0);
        }
    }

private:
    const std::vector<GPUDerivedValue> derived;
    const cuda::stream_ref stream;
    std::vector<rmm::device_buffer> buffers;
};

/// The aggregates as partials that combine: `sum`, `min`, `max` by themselves, `count` by `sum`, `avg` as `sum` and
/// `count`. A table of partial results is the keys, then each partial.
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

    /// Whether `streaming_groupby` takes them, which it does not of a `min` or a `max` of values of varying width.
    __host__ bool streams() const
    {
        return std::none_of(partials.begin(), partials.end(), [&](const Partial & partial)
        {
            return (partial.aggregation == GPUAggregationKind::Min || partial.aggregation == GPUAggregationKind::Max)
                && columnKindOf(values[partial.column]) == GPUColumnKind::Variable;
        });
    }

    /// The requests of `streaming_groupby` of the partials.
    __host__ std::vector<cudf::groupby::streaming_aggregation_request> streamingRequests() const
    {
        std::vector<cudf::groupby::streaming_aggregation_request> requests;
        requests.reserve(partials.size());
        for (const auto & partial : partials)
            requests.push_back({.column_index = columnOf(partial), .aggregation = aggregationOf(partial.aggregation)});
        return requests;
    }

    /// The partial results of a batch: of each group (hash), or of each run of equal keys (sorted, no null keys). Rows
    /// with null keys, which a `WHERE` left out, are excluded.
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

    /// The groups of `tables` of partial results, as partial results.
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

    /// The keys and the partial results of `streaming_groupby`.
    __host__ std::unique_ptr<cudf::table> tableOf(std::unique_ptr<cudf::table> group_keys, std::vector<cudf::groupby::aggregation_result> results) const
    {
        std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();
        for (size_t i = 0; i < partials.size(); ++i)
        {
            std::unique_ptr<cudf::column> & result = results[i].results.front();
            /// Of each count, as wide as their sums, so that the partial results concatenate.
            if (partials[i].aggregation == GPUAggregationKind::Count && result->type().id() != cudf::type_id::INT64)
                result = guarded("widening a count", [&] { return cudf::cast(result->view(), cudf::data_type{cudf::type_id::INT64}, stream); });
            columns.push_back(std::move(result));
        }
        return std::make_unique<cudf::table>(std::move(columns));
    }

    /// The groups of `combined`, the keys and the partials of every group: the keys, without `dropped_keys` first, then
    /// the result of each aggregate, checked to be as wide as what it is taken as.
    __host__ std::unique_ptr<cudf::table> resultsOf(std::unique_ptr<cudf::table> combined, size_t dropped_keys) const
    {
        std::vector<std::unique_ptr<cudf::column>> partial_columns = combined->release();
        const auto partial = [&](size_t index) -> cudf::column_view { return partial_columns[num_key_columns + index]->view(); };

        /// The `avg`s first, which only read their partials; then each other aggregate takes its partial, the last of
        /// those of one partial taking it as it is and the others a copy.
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

    /// The column of the partial among the keys and the values: of a `count`, the first key, which has no nulls but what
    /// a `WHERE` leaves out.
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
    /// Of each aggregate, its partial, or of an `avg`, its `sum` and its `count`.
    std::vector<std::pair<size_t, std::optional<size_t>>> of;
};

/// Partial results split by a hash of the keys into parts, which others merge each on its own.
class Parts
{
public:
    __host__ Parts(const PartialAggregates & aggregates_, size_t num_parts, cuda::stream_ref stream_)
        : aggregates(aggregates_), parts(num_parts), stream(stream_)
    {
    }

    __host__ size_t size() const { return parts.size(); }

    /// Splits `partial` by a hash of the keys into the parts. Of the bytes after which a part is combined, a share as of
    /// all of them, so that the parts hold about as much as one list of runs would.
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

    /// Copies, in the order of `stream`, of the partial results of part `part`.
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

    /// The groups of every part, as partial results; nothing without any. The parts are empty afterwards.
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
    /// Partial results of one part, how many rows and bytes they take, and the bytes they took after they were combined last.
    struct Runs
    {
        std::vector<std::unique_ptr<cudf::table>> tables;
        size_t rows = 0;
        size_t bytes = 0;
        size_t combined_bytes = 0;
    };

    /// Adds `table` to `runs`, and combines them once they take more than their share of `min_bytes_to_combine_runs` and
    /// twice what they took after they were combined last, so that what they hold stays within about twice the groups.
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

/// One `streaming_groupby` of few groups, which goes into the parts as partial results once it might not hold the groups
/// of the next slice, or once the parts are taken, and is made again: it never grows.
class StreamingPartials
{
public:
    __host__ StreamingPartials(const PartialAggregates & aggregates_, Parts & parts_, cuda::stream_ref stream_)
        : aggregates(aggregates_), parts(parts_), stream(stream_)
    {
    }

    /// Groups `table`, the keys and the values of a batch, a slice at a time.
    __host__ void add(const cudf::table_view & table)
    {
        const size_t rows = static_cast<size_t>(table.num_rows());
        for (size_t offset = 0; offset < rows;)
        {
            const size_t slice_rows = std::min(rows - offset, max_slice_rows);
            const auto begin = static_cast<cudf::size_type>(offset);
            const auto end = static_cast<cudf::size_type>(offset + slice_rows);
            const cudf::table_view slice = begin == 0 && end == table.num_rows() ? table : cudf::slice(table, {begin, end}).front();

            if (grouping && max_groups + slice_rows > capacity)
                max_groups = static_cast<size_t>(grouping->distinct_keys());
            if (grouping && max_groups + slice_rows > capacity)
                close();
            if (!grouping)
            {
                capacity = std::min(max_capacity - slice_rows, slice_rows + slice_rows / 4 + max_groups_to_stream);
                grouping.emplace(guarded("making a `streaming_groupby` of at most " + std::to_string(capacity) + " groups", [&]
                {
                    return cudf::groupby::streaming_groupby(
                        aggregates.keyIndices(), aggregates.streamingRequests(), static_cast<cudf::size_type>(capacity), cudf::null_policy::EXCLUDE);
                }));
            }

            guarded("grouping " + std::to_string(slice_rows) + " rows into a `streaming_groupby`", [&] { grouping->aggregate(slice, stream); return 0; });
            max_groups += slice_rows;
            offset += slice_rows;
        }
    }

    /// Its groups into the parts as partial results.
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
        parts.add(aggregates.tableOf(std::move(group_keys), std::move(results)));
    }

private:
    const PartialAggregates & aggregates;
    Parts & parts;
    const cuda::stream_ref stream;

    /// Made for `capacity` groups, and at least as many as its groups, `max_groups`: those it had when it was asked last,
    /// which waits for the device, and the rows grouped since.
    std::optional<cudf::groupby::streaming_groupby> grouping;
    size_t capacity = 0;
    size_t max_groups = 0;
};

}

using namespace cudf_group_by;

struct CudfGroupBy::State
{
    /// How the batches are reduced into partial results, which the first decides.
    enum class Reduction
    {
        Undecided,
        /// Over their runs of equal keys, see `max_runs_per_row_to_reduce`.
        Runs,
        /// Each by a hash table of its own.
        HashTable,
        /// Into one `streaming_groupby` of few groups, see `max_groups_to_stream`.
        Streaming,
    };

    const std::vector<GPUElementType> keys;
    /// The values of a batch, then those derived from them.
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const cuda::stream_ref stream;
    /// Without keys, the rows are grouped by a key of their own, the same for every row, which comes first among the
    /// columns grouped and is gone from the groups once they are closed: every batch is then one run.
    const bool keyless;
    /// As many zero bytes as the largest batch had rows: the values of that key.
    rmm::device_buffer keyless_key;

    /// What the `WHERE` of a batch, or the `HAVING`, leaves out, as a bit per row, and as many `true` bytes as the most
    /// rows compacted by it, see `keptRows`.
    rmm::device_buffer filter_mask;
    rmm::device_buffer all_true;
    DerivedValues derived;
    PartialAggregates partials;
    Reduction reduction = Reduction::Undecided;
    Parts parts;
    StreamingPartials streaming;
    /// Taken by the first of those that take the parts, which closes `streaming` into them.
    std::mutex closing;

    /// The keys, then the results of the aggregates, once the groups are closed, and of those of varying width, their
    /// offsets widened, see `viewOf`.
    std::unique_ptr<cudf::table> groups;
    mutable std::vector<std::unique_ptr<cudf::column>> widened_offsets;
    bool finalized = false;

    __host__ State(
        GPUSpan<GPUElementType> keys_,
        GPUSpan<GPUElementType> values_,
        GPUSpan<GPUDerivedValue> derived_,
        GPUSpan<GPUGroupByAggregate> aggregates_,
        size_t num_parts,
        cuda::stream_ref stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_.begin(), values_.end())
        , aggregates(aggregates_.begin(), aggregates_.end())
        , stream(stream_)
        , keyless(keys_.empty())
        , keyless_key(0, stream_)
        , filter_mask(0, stream_)
        , all_true(0, stream_)
        , derived(derived_, stream_)
        , partials(aggregates_, values_, numKeyColumns(), stream_)
        , parts(partials, num_parts, stream_)
        , streaming(partials, parts, stream_)
        , widened_offsets(keys_.size() + aggregates_.size())
    {
    }

    __host__ size_t numBatchValues() const { return values.size() - derived.size(); }

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

    /// The first batch decides: runs of mostly distinct keys -> runs; few groups -> `streaming_groupby`; otherwise a hash
    /// table. Its runs, combined, are its groups and go into the parts.
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
};

__host__ CudfGroupBy::CudfGroupBy(
    GPUSpan<GPUElementType> keys,
    GPUSpan<GPUElementType> values,
    GPUSpan<GPUDerivedValue> derived,
    GPUSpan<GPUGroupByAggregate> aggregates,
    size_t num_parts,
    cuda::stream_ref stream)
{
    if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
        throwGPUError("a `GROUP BY` through cuDF on the device's default stream, where cuco cannot copy its counts back");
    if (values.empty())
        throwGPUError("a `GROUP BY` without values");
    if (aggregates.empty())
        throwGPUError("a `GROUP BY` without aggregates");
    if (num_parts == 0 || num_parts > static_cast<size_t>(std::numeric_limits<int>::max()))
        throwGPUError(("a `GROUP BY` split into " + std::to_string(num_parts) + " parts").c_str());
    if (derived.size() > values.size())
        throwGPUError(("a `GROUP BY` of " + std::to_string(derived.size()) + " derived values among " + std::to_string(values.size())).c_str());

    for (size_t j = 0; j < derived.size(); ++j)
    {
        if (values[values.size() - derived.size() + j] != derived[j].expression.value_type)
            throwGPUError("a derived value of another type than its expression's");
        for (const uint32_t column : derived[j].columns)
        {
            if (column >= values.size() - derived.size())
                throwGPUError(("a derived value of value " + std::to_string(column) + ", which is not of a batch").c_str());
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

    state = new State(keys, values, derived, aggregates, num_parts, stream);
}

__host__ CudfGroupBy::~CudfGroupBy()
{
    delete state;
}

__host__ void CudfGroupBy::addBatch(
    GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUExpression * filter)
{
    if (state->finalized)
        throwGPUError("a batch after the groups were closed");

    if (keys.size() != state->keys.size() || values.size() != state->numBatchValues())
        throwGPUError(
            ("a batch of " + std::to_string(keys.size()) + " keys and " + std::to_string(values.size()) + " values, expected "
            + std::to_string(state->keys.size()) + " and " + std::to_string(state->numBatchValues())).c_str());

    size_t rows = keys.empty() ? values[0].rows() : keys[0].rows();
    if (rows == 0)
        return;

    std::vector<cudf::column_view> columns;
    columns.reserve(state->numKeyColumns() + state->values.size());
    if (state->keyless)
        columns.push_back(state->keylessKey(rows));
    for (auto & column : columnsOf(keys, state->keys, rows))
        columns.push_back(column);
    for (auto & column : columnsOf(values, state->values, rows))
        columns.push_back(column);

    /// Lives until the partial results are made of its rows.
    std::unique_ptr<cudf::table> compacted;
    if (filter)
    {
        const std::vector<GPUExpressionInput> inputs = inputsOf(filter_columns, rows);
        const cudf::size_type left_out = evaluateMask(
            *filter, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, state->filter_mask, state->stream);
        if (static_cast<size_t>(left_out) == rows)
            return;

        /// Runs are reduced over batches without nulls only, as cuDF sorts keys with nulls whatever it is told.
        using Reduction = State::Reduction;
        const bool runs = state->reduction == Reduction::Undecided || state->reduction == Reduction::Runs;
        if (left_out != 0 && (runs || static_cast<double>(left_out) >= min_left_out_share_to_compact * static_cast<double>(rows)))
        {
            compacted = keptRows(cudf::table_view(columns), static_cast<const cudf::bitmask_type *>(state->filter_mask.data()), left_out, state->all_true, state->stream);
            rows = static_cast<size_t>(compacted->num_rows());
            const cudf::table_view kept = compacted->view();
            columns.assign(kept.begin(), kept.end());
        }
        else if (left_out != 0)
        {
            columns[0] = withNullMask(columns[0], static_cast<const cudf::bitmask_type *>(state->filter_mask.data()), left_out);
        }
    }

    if (state->derived.size() != 0)
    {
        std::vector<DeviceFixedColumn> batch_values;
        batch_values.reserve(state->numBatchValues());
        for (size_t i = 0; i < state->numBatchValues(); ++i)
        {
            const GPUElementType type = state->values[i];
            batch_values.push_back(
                columnKindOf(type) == GPUColumnKind::Fixed ? deviceViewOf(columns[state->numKeyColumns() + i], type) : DeviceFixedColumn{});
        }
        state->derived.derive(columns, batch_values, rows);
    }

    if (state->reduction == State::Reduction::Undecided)
        state->decide(columns, rows);
    else
        state->reduce(columns);
}

__host__ void CudfGroupBy::absorbPart(const CudfGroupBy & other, size_t part)
{
    if (state->finalized || state->parts.size() != 1)
        throwGPUError("a part of the groups of a `GROUP BY` absorbed into one that is not of one part");
    if (state->keyless != other.state->keyless || state->keys.size() != other.state->keys.size() || !(state->partials == other.state->partials))
        throwGPUError("a part of the groups of a `GROUP BY` absorbed into one of other keys or aggregates");

    /// The first of those that take the parts closes `streaming_groupby` of `other` into them, which takes no more batches.
    {
        std::lock_guard lock(other.state->closing);
        other.state->streaming.close();
    }

    /// Copies in the order of this stream, as the originals are freed in the order of `other`'s, once this one has made them.
    waitFor(state->stream, other.state->stream);
    auto copies = other.state->parts.copiesOf(part, state->stream);
    waitFor(other.state->stream, state->stream);
    for (auto & copy : copies)
        state->parts.add(std::move(copy));
}

__host__ size_t CudfGroupBy::finalize()
{
    if (state->finalized)
        throwGPUError("the groups were closed twice");

    state->streaming.close();
    if (auto combined = state->parts.combineAll())
        state->groups = state->partials.resultsOf(std::move(combined), state->keyless ? 1 : 0);
    state->finalized = true;
    return state->groups ? static_cast<size_t>(state->groups->num_rows()) : 0;
}

__host__ size_t CudfGroupBy::filterGroups(const GPUExpression & expression, GPUSpan<uint32_t> columns)
{
    if (!state->finalized)
        throwGPUError("the groups were filtered before they were closed");

    if (!state->groups || state->groups->num_rows() == 0)
        return 0;

    const cudf::table_view groups = state->groups->view();
    const auto rows = static_cast<size_t>(groups.num_rows());

    std::vector<GPUExpressionInput> inputs;
    inputs.reserve(columns.size());
    for (const uint32_t column : columns)
    {
        const GPUElementType element_type = column < state->keys.size()
            ? state->keys[column]
            : (column - state->keys.size() < state->aggregates.size() ? state->aggregates[column - state->keys.size()].result_type
                                                                       : GPUElementType::String);
        if (column >= static_cast<uint32_t>(groups.num_columns()) || columnKindOf(element_type) != GPUColumnKind::Fixed)
            throwGPUError(("a filter of the groups over column " + std::to_string(column) + ", which is not of a fixed width").c_str());
        const cudf::column_view view = groups.column(static_cast<cudf::size_type>(column));
        checkNoNulls(view);
        inputs.push_back({.data = view.head<char>(), .type = element_type});
    }

    const cudf::size_type left_out = evaluateMask(
        expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, state->filter_mask, state->stream);
    if (left_out == 0)
        return rows;
    state->groups = keptRows(groups, static_cast<const cudf::bitmask_type *>(state->filter_mask.data()), left_out, state->all_true, state->stream);
    return static_cast<size_t>(state->groups->num_rows());
}

__host__ DeviceColumnView CudfGroupBy::key(size_t index) const
{
    if (!state->finalized)
        throwGPUError("the groups were asked for before they were closed");
    if (index >= state->keys.size())
        throwGPUError(("key " + std::to_string(index) + " of " + std::to_string(state->keys.size())).c_str());
    return viewOf(state->groups.get(), index, state->keys[index], state->widened_offsets[index], state->stream);
}

__host__ DeviceColumnView CudfGroupBy::value(size_t index) const
{
    if (!state->finalized)
        throwGPUError("the groups were asked for before they were closed");
    if (index >= state->aggregates.size())
        throwGPUError(("aggregate " + std::to_string(index) + " of " + std::to_string(state->aggregates.size())).c_str());
    const size_t column = state->keys.size() + index;
    return viewOf(state->groups.get(), column, state->aggregates[index].result_type, state->widened_offsets[column], state->stream);
}

struct CudfRowCollector::State
{
    const std::vector<GPUElementType> values;
    const cuda::stream_ref stream;
    rmm::device_buffer filter_mask;
    rmm::device_buffer all_true;
    std::vector<std::unique_ptr<cudf::table>> collected;
    std::unique_ptr<cudf::table> taken;
    mutable std::vector<std::unique_ptr<cudf::column>> widened_offsets;

    __host__ State(GPUSpan<GPUElementType> values_, cuda::stream_ref stream_)
        : values(values_.begin(), values_.end()), stream(stream_), filter_mask(0, stream_), all_true(0, stream_), widened_offsets(values.size())
    {
    }
};

__host__ CudfRowCollector::CudfRowCollector(GPUSpan<GPUElementType> values, cuda::stream_ref stream)
{
    if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
        throwGPUError("a collection of rows on the device's default stream");
    if (values.empty())
        throwGPUError("a collection of rows of no columns");
    state = new State(values, stream);
}

__host__ CudfRowCollector::~CudfRowCollector()
{
    delete state;
}

__host__ void CudfRowCollector::addBatch(GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUExpression * filter)
{
    if (values.size() != state->values.size())
        throwGPUError(("a batch of " + std::to_string(values.size()) + " columns to collect, expected " + std::to_string(state->values.size())).c_str());

    const size_t rows = values[0].rows();
    if (rows == 0)
        return;

    const std::vector<cudf::column_view> columns = columnsOf(values, state->values, rows);
    if (filter)
    {
        const std::vector<GPUExpressionInput> inputs = inputsOf(filter_columns, rows);
        const cudf::size_type left_out = evaluateMask(
            *filter, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, state->filter_mask, state->stream);
        if (static_cast<size_t>(left_out) == rows)
            return;
        if (left_out != 0)
        {
            state->collected.push_back(keptRows(cudf::table_view(columns), static_cast<const cudf::bitmask_type *>(state->filter_mask.data()), left_out, state->all_true, state->stream));
            return;
        }
    }

    /// A copy, as the batch's buffers are freed once it is added.
    state->collected.push_back(guarded("keeping " + std::to_string(rows) + " rows", [&]
    {
        return std::make_unique<cudf::table>(cudf::table_view(columns), state->stream);
    }));
}

__host__ size_t CudfRowCollector::collectedRows() const
{
    size_t rows = 0;
    for (const auto & table : state->collected)
        rows += static_cast<size_t>(table->num_rows());
    return rows;
}

__host__ size_t CudfRowCollector::take()
{
    state->taken.reset();
    if (state->collected.size() == 1)
    {
        state->taken = std::move(state->collected.front());
    }
    else if (!state->collected.empty())
    {
        std::vector<cudf::table_view> views;
        views.reserve(state->collected.size());
        for (const auto & table : state->collected)
            views.push_back(table->view());
        state->taken = guarded("concatenating " + std::to_string(views.size()) + " batches of collected rows", [&]
        {
            return cudf::concatenate(views, state->stream);
        });
    }
    state->collected.clear();
    return state->taken ? static_cast<size_t>(state->taken->num_rows()) : 0;
}

__host__ DeviceColumnView CudfRowCollector::value(size_t index) const
{
    if (index >= state->values.size())
        throwGPUError(("column " + std::to_string(index) + " of " + std::to_string(state->values.size()) + " collected").c_str());
    return viewOf(state->taken.get(), index, state->values[index], state->widened_offsets[index], state->stream);
}

}
