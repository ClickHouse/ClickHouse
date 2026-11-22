#include <GPU/CudfGroupBy.cuh>

#include <GPU/Cudf.cuh>

#include <cudf/binaryop.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/groupby.hpp>
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

/// A batch is aggregated into a table of partial results: the keys, then a column per partial. The partial tables are
/// split by a hash of the keys into parts, and the tables of a part are merged as they pile up. A merger takes one part
/// of every lane, and merges and finishes it: an `avg`, a `sum` and a `count` until then, is divided at the end.
///
/// The first batch chooses how the batches are aggregated:
///     SortedRuns  the batch is sorted by the keys, runs of equal keys are aggregated by a sort-based groupby
///     Streaming   few groups: into one `streaming_groupby`
///     HashTable   otherwise, a hash groupby per batch

namespace DB::GPU
{

namespace
{

constexpr double max_runs_per_row_for_sorted_runs = 0.5;
constexpr double min_groups_per_run_for_sorted_runs = 0.25;

constexpr size_t max_groups_to_stream = 1UL << 16;

/// `streaming_groupby` keeps its capacity plus a row index, and counts, in a `cudf::size_type`.
constexpr size_t max_streaming_rows = std::numeric_limits<cudf::size_type>::max();

/// Below this share, the rows a filter leaves out become null keys, which the groupbys exclude, rather than compacted.
constexpr double min_left_out_share_to_compact = 0.25;

/// With 4 GiB, TPC-H Q1 took 10 GiB of the device.
constexpr size_t min_bytes_to_merge = 256UL << 20;
constexpr size_t max_rows_to_merge = 1UL << 30;

static_assert(std::is_same_v<cudf::bitmask_type, uint32_t>);

__host__ cudf::column_view withNullMask(const cudf::column_view & column, DeviceBitmask kept, cudf::size_type null_count)
{
    std::vector<cudf::column_view> children(column.child_begin(), column.child_end());
    return cudf::column_view(column.type(), column.size(), column.head(), kept.words, null_count, column.offset(), children);
}

/// Widened offsets go into `offsets`, which must outlive the view.
__host__ DeviceColumnView viewOf(
    const cudf::table * table, size_t index, GPUElementType type, std::unique_ptr<cudf::column> & offsets, cuda::stream_ref stream)
{
    const bool fixed = columnKindOf(type) == GPUColumnKind::Fixed;
    if (!table)
        return fixed ? DeviceColumnView{DeviceFixedColumn{.element_type = type}} : DeviceColumnView{DeviceVariableColumn{}};

    const cudf::column_view column = table->view().column(static_cast<cudf::size_type>(index));
    if (fixed)
        return deviceViewByBits(column, type);
    DeviceVariableColumnOfCudf of_cudf = deviceViewOfVariable(column, stream);
    offsets = std::move(of_cudf.widened_offsets);
    return of_cudf.view;
}

__host__ void appendColumns(
    std::vector<cudf::column_view> & columns, const GPUSpan<DeviceColumnView> & batch, const std::vector<GPUElementType> & types, size_t rows)
{
    for (size_t i = 0; i < batch.size(); ++i)
    {
        if (batch[i].rows() != rows)
            throwGPUError(("a column of " + std::to_string(batch[i].rows()) + " rows in a batch of " + std::to_string(rows)).c_str());
        columns.push_back(columnViewOf(batch[i], types[i]));
    }
}

/// The inputs of a predicate over `columns`, of `rows` rows each.
__host__ std::vector<GPUExpressionInput> inputsOf(const GPUExpression & expression, GPUSpan<DeviceFixedColumn> columns, size_t rows)
{
    if (columns.size() != expression.num_columns)
        throwGPUError(("a predicate of " + std::to_string(expression.num_columns) + " columns over " + std::to_string(columns.size())).c_str());

    std::vector<GPUExpressionInput> inputs;
    inputs.reserve(columns.size());
    for (const auto & column : columns)
    {
        if (column.rows != rows)
            throwGPUError(("a column of a predicate of " + std::to_string(column.rows) + " rows among " + std::to_string(rows)).c_str());
        inputs.push_back({.data = column.data, .type = column.element_type});
    }
    return inputs;
}

__host__ std::unique_ptr<cudf::groupby_aggregation> cudfAggregationOf(GPUAggregationKind aggregation)
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

}

/// Not anonymous: the classes are members of `GroupBy`, which is not.
namespace cudf_group_by
{

/// The values computed for each batch from its values, see `GPUDerivedValue`.
class DerivedValues
{
public:
    __host__ DerivedValues(GPUSpan<GPUDerivedValue> derived, cuda::stream_ref stream_) : stream(stream_)
    {
        for (const auto & value : derived)
        {
            if (value.expression.result != GPUExpressionResult::Value)
                throwGPUError("a derived value of an expression that is a predicate");
            expressions.push_back(value.expression);
            columns.emplace_back(value.columns.begin(), value.columns.end());
            buffers.emplace_back(0, stream_);
        }
    }

    __host__ size_t size() const { return expressions.size(); }

    /// Appends the derived values of a batch to `batch`, of the keys and then the values, the first `num_key_columns`.
    __host__ void derive(std::vector<cudf::column_view> & batch, size_t num_key_columns, const std::vector<GPUElementType> & values)
    {
        const auto rows = static_cast<size_t>(batch[0].size());
        const size_t num_values = batch.size() - num_key_columns;
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            std::vector<GPUExpressionInput> inputs;
            inputs.reserve(columns[i].size());
            for (const uint32_t column : columns[i])
            {
                if (column >= num_values)
                    throwGPUError(("a derived value of value " + std::to_string(column) + " of a batch of " + std::to_string(num_values)).c_str());
                const DeviceFixedColumn value = deviceViewOf(batch[num_key_columns + column], values[column]);
                inputs.push_back({.data = value.data, .type = value.element_type});
            }

            const GPUElementType type = expressions[i].value_type;
            rmm::device_buffer & out = buffers[i];
            if (out.size() < rows * sizeOf(type))
                out.resize(rows * sizeOf(type), stream);
            evaluateGPUExpression(expressions[i], {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, out.data(), stream);
            batch.emplace_back(cudfTypeOf(type), static_cast<cudf::size_type>(rows), out.data(), nullptr, 0);
        }
    }

private:
    const cuda::stream_ref stream;
    std::vector<GPUExpression> expressions;
    std::vector<std::vector<uint32_t>> columns;
    /// Of each derived value, its values of the last batch.
    std::vector<rmm::device_buffer> buffers;
};

/// The partials that cuDF computes for the aggregates, and the tables of partial results.
class Partials
{
public:
    __host__ Partials(GPUSpan<GPUGroupByAggregate> aggregates_, GPUSpan<GPUElementType> values_, size_t num_keys_, cuda::stream_ref stream_)
        : aggregates(aggregates_.begin(), aggregates_.end()), values(values_.begin(), values_.end()), num_keys(num_keys_), stream(stream_)
    {
        for (size_t i = 0; i < num_keys; ++i)
            key_indices.push_back(static_cast<cudf::size_type>(i));

        for (const auto & aggregate : aggregates)
        {
            if (aggregate.aggregation == GPUAggregationKind::Mean)
                sources.push_back({
                    .partial = add(GPUAggregationKind::Sum, aggregate.column),
                    .count = add(GPUAggregationKind::Count, GPUGroupByAggregate::no_column)});
            else
                sources.push_back({.partial = add(aggregate.aggregation, aggregate.column)});
        }
    }

    __host__ bool operator==(const Partials & other) const { return num_keys == other.num_keys && partials == other.partials; }

    __host__ const std::vector<cudf::size_type> & keyIndices() const { return key_indices; }

    /// cuDF does not stream `min` or `max` of variable-width values.
    __host__ bool streamable() const
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
        for (const auto & partial : partials)
            requests.push_back({.column_index = columnOf(partial), .aggregation = cudfAggregationOf(partial.aggregation)});
        return requests;
    }

    /// The columns of a batch, the keys then the values, into a table of partial results. Rows of null keys are excluded.
    __host__ std::unique_ptr<cudf::table> aggregate(const std::vector<cudf::column_view> & columns, cudf::sorted sorted) const
    {
        std::vector<cudf::groupby::aggregation_request> requests(partials.size());
        for (size_t i = 0; i < partials.size(); ++i)
        {
            requests[i].values = columns[static_cast<size_t>(columnOf(partials[i]))];
            requests[i].aggregations.push_back(cudfAggregationOf(partials[i].aggregation));
        }

        const cudf::table_view keys(std::vector<cudf::column_view>(columns.begin(), columns.begin() + num_keys));
        cudf::groupby::groupby by_keys(keys, cudf::null_policy::EXCLUDE, sorted);
        auto [group_keys, results] = guarded("aggregating a batch", [&] { return by_keys.aggregate(requests, stream); });
        return tableOf(std::move(group_keys), std::move(results));
    }

    /// Tables of partial results into one, of a row per group.
    __host__ std::unique_ptr<cudf::table> merge(const std::vector<cudf::table_view> & tables) const
    {
        std::unique_ptr<cudf::table> concatenated;
        if (tables.size() > 1)
            concatenated = guarded("concatenating partial results", [&] { return cudf::concatenate(tables, stream); });
        const cudf::table_view all = concatenated ? concatenated->view() : tables.front();

        std::vector<cudf::groupby::aggregation_request> requests(partials.size());
        for (size_t i = 0; i < partials.size(); ++i)
        {
            const GPUAggregationKind kind = partials[i].aggregation;
            requests[i].values = all.column(static_cast<cudf::size_type>(num_keys + i));
            requests[i].aggregations.push_back(cudfAggregationOf(kind == GPUAggregationKind::Count ? GPUAggregationKind::Sum : kind));
        }

        cudf::groupby::groupby by_keys(all.select(key_indices), cudf::null_policy::EXCLUDE);
        auto [group_keys, results] = guarded("merging partial results", [&] { return by_keys.aggregate(requests, stream); });
        return tableOf(std::move(group_keys), std::move(results));
    }

    __host__ std::unique_ptr<cudf::table>
    tableOf(std::unique_ptr<cudf::table> group_keys, std::vector<cudf::groupby::aggregation_result> results) const
    {
        std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();
        for (size_t i = 0; i < partials.size(); ++i)
        {
            std::unique_ptr<cudf::column> & result = results[i].results.front();
            /// As wide as the sums of counts, so that the tables concatenate.
            if (partials[i].aggregation == GPUAggregationKind::Count && result->type().id() != cudf::type_id::INT64)
                result = guarded("widening a count", [&] { return cudf::cast(result->view(), cudf::data_type{cudf::type_id::INT64}, stream); });
            columns.push_back(std::move(result));
        }
        return std::make_unique<cudf::table>(std::move(columns));
    }

    /// A merged table of partial results into the keys, without the first `dropped_keys`, and the result of each aggregate.
    __host__ std::unique_ptr<cudf::table> finish(std::unique_ptr<cudf::table> merged, size_t dropped_keys) const
    {
        std::vector<std::unique_ptr<cudf::column>> columns = merged->release();
        const auto partial = [&](size_t index) -> std::unique_ptr<cudf::column> & { return columns[num_keys + index]; };

        /// The `avg`s first: the other aggregates take the partials, the last one of each without a copy.
        std::vector<std::unique_ptr<cudf::column>> results(aggregates.size());
        std::vector<size_t> last_taker(partials.size(), aggregates.size());
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            if (!sources[i].count)
            {
                last_taker[sources[i].partial] = i;
                continue;
            }
            /// cuDF sums unsigned integers into an `Int64`, ClickHouse into a `UInt64` of the same bits.
            cudf::column_view sums = partial(sources[i].partial)->view();
            if (isUnsigned(values[aggregates[i].column]) && sums.type().id() == cudf::type_id::INT64)
                sums = cudf::bit_cast(sums, cudf::data_type{cudf::type_id::UINT64});
            const cudf::column_view counts = partial(*sources[i].count)->view();
            results[i] = guarded("dividing the sums of an `avg` by its counts", [&]
            {
                return cudf::binary_operation(sums, counts, cudf::binary_operator::TRUE_DIV, cudf::data_type{cudf::type_id::FLOAT64}, stream);
            });
        }
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            if (sources[i].count)
                continue;
            std::unique_ptr<cudf::column> & taken = partial(sources[i].partial);
            results[i] = last_taker[sources[i].partial] == i ? std::move(taken) : std::make_unique<cudf::column>(taken->view(), stream);
        }

        std::vector<std::unique_ptr<cudf::column>> groups;
        for (size_t i = dropped_keys; i < num_keys; ++i)
            groups.push_back(std::move(columns[i]));
        for (auto & result : results)
            groups.push_back(std::move(result));
        return std::make_unique<cudf::table>(std::move(groups));
    }

private:
    struct Partial
    {
        GPUAggregationKind aggregation;
        uint32_t column;

        bool operator==(const Partial &) const = default;
    };

    /// Of an aggregate: its partial, or of an `avg`, its `sum` and its `count`.
    struct Sources
    {
        size_t partial;
        std::optional<size_t> count;
    };

    __host__ size_t add(GPUAggregationKind aggregation, uint32_t column)
    {
        const Partial partial{.aggregation = aggregation, .column = column};
        const auto it = std::find(partials.begin(), partials.end(), partial);
        if (it != partials.end())
            return static_cast<size_t>(it - partials.begin());
        partials.push_back(partial);
        return partials.size() - 1;
    }

    /// A `count` counts the first key, which has nulls only where a filter left rows out.
    __host__ cudf::size_type columnOf(const Partial & partial) const
    {
        return partial.column == GPUGroupByAggregate::no_column ? 0 : static_cast<cudf::size_type>(num_keys + partial.column);
    }

    const std::vector<GPUGroupByAggregate> aggregates;
    const std::vector<GPUElementType> values;
    const size_t num_keys;
    const cuda::stream_ref stream;
    std::vector<cudf::size_type> key_indices;
    std::vector<Partial> partials;
    std::vector<Sources> sources;
};

/// Tables of partial results, split into parts by a hash of the keys.
class PartitionedPartials
{
public:
    __host__ PartitionedPartials(const Partials & partials_, size_t num_parts, cuda::stream_ref stream_)
        : partials(partials_), parts(num_parts), stream(stream_)
    {
    }

    __host__ size_t size() const { return parts.size(); }

    __host__ void add(std::unique_ptr<cudf::table> table)
    {
        if (table->num_rows() == 0)
            return;
        if (parts.size() == 1)
        {
            addToPart(parts.front(), std::move(table));
            return;
        }

        auto [pieces, offsets] = guarded("splitting partial results into parts", [&]
        {
            return cudf::hash_partition(
                table->view(), partials.keyIndices(), static_cast<int>(parts.size()), cudf::hash_id::HASH_MURMUR3, cudf::DEFAULT_HASH_SEED, stream);
        });
        offsets.push_back(pieces->num_rows());
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (offsets[i] == offsets[i + 1])
                continue;
            const cudf::table_view piece = cudf::slice(pieces->view(), {offsets[i], offsets[i + 1]}).front();
            addToPart(parts[i], guarded("copying a part of partial results", [&] { return std::make_unique<cudf::table>(piece, stream); }));
        }
    }

    __host__ std::vector<std::unique_ptr<cudf::table>> copiesOf(size_t part, cuda::stream_ref to_stream) const
    {
        std::vector<std::unique_ptr<cudf::table>> copies;
        for (const auto & table : parts[part].tables)
            copies.push_back(guarded("copying a part of partial results", [&] { return std::make_unique<cudf::table>(table->view(), to_stream); }));
        return copies;
    }

    /// All the parts merged into one table, or nothing without rows.
    __host__ std::unique_ptr<cudf::table> mergeAll()
    {
        std::vector<cudf::table_view> views;
        for (const auto & part : parts)
            for (const auto & table : part.tables)
                views.push_back(table->view());
        if (views.empty())
            return nullptr;

        auto merged = partials.merge(views);
        /// Freed in the order of the stream, after the merge has read them.
        for (auto & part : parts)
            part = {};
        return merged;
    }

private:
    struct Part
    {
        std::vector<std::unique_ptr<cudf::table>> tables;
        size_t rows = 0;
        size_t bytes = 0;
        size_t merged_bytes = 0;
    };

    /// Merging at twice the size after the last merge keeps the memory within about twice that of the groups.
    __host__ void addToPart(Part & part, std::unique_ptr<cudf::table> table)
    {
        part.rows += static_cast<size_t>(table->num_rows());
        part.bytes += table->alloc_size();
        part.tables.push_back(std::move(table));

        const size_t min_bytes = min_bytes_to_merge / parts.size();
        if (part.tables.size() < 2 || (part.bytes <= std::max(min_bytes, 2 * part.merged_bytes) && part.rows < max_rows_to_merge))
            return;

        std::vector<cudf::table_view> views;
        for (const auto & t : part.tables)
            views.push_back(t->view());
        auto merged = partials.merge(views);
        part.tables.clear();
        part.rows = static_cast<size_t>(merged->num_rows());
        part.bytes = part.merged_bytes = merged->alloc_size();
        part.tables.push_back(std::move(merged));
    }

    const Partials & partials;
    std::vector<Part> parts;
    const cuda::stream_ref stream;
};

/// One `streaming_groupby`, which never grows: flushed into the parts before it could overflow, and made again.
class StreamingPartials
{
public:
    __host__ StreamingPartials(const Partials & partials_, PartitionedPartials & parts_, cuda::stream_ref stream_)
        : partials(partials_), parts(parts_), stream(stream_)
    {
    }

    __host__ void add(const cudf::table_view & batch)
    {
        const size_t rows = static_cast<size_t>(batch.num_rows());
        if (grouping && !fits(rows))
            flush();
        if (!grouping)
            start(rows);

        guarded("aggregating a batch into a `streaming_groupby`", [&] { grouping->aggregate(batch, stream); return 0; });
        max_groups += rows;
        rows_grouped += rows;
    }

    __host__ void flush()
    {
        if (!grouping)
            return;

        auto [group_keys, results] = guarded("flushing a `streaming_groupby`", [&] { return std::move(*grouping).finalize_and_release(stream); });
        grouping.reset();
        parts.add(partials.tableOf(std::move(group_keys), std::move(results)));
    }

private:
    __host__ void start(size_t rows)
    {
        capacity = rows + rows / 4 + max_groups_to_stream;
        if (capacity + rows > max_streaming_rows)
            throwGPUError(("a batch of " + std::to_string(rows) + " rows for a `streaming_groupby`").c_str());
        grouping.emplace(guarded("making a `streaming_groupby`", [&]
        {
            return cudf::groupby::streaming_groupby(
                partials.keyIndices(), partials.streamingRequests(), static_cast<cudf::size_type>(capacity), cudf::null_policy::EXCLUDE);
        }));
        max_groups = 0;
        rows_grouped = 0;
    }

    /// `max_groups` bounds the groups by the rows since the device was last asked for them, which waits for it.
    __host__ bool fits(size_t rows)
    {
        if (rows_grouped + rows > max_streaming_rows)
            return false;
        if (max_groups + rows > capacity)
            max_groups = static_cast<size_t>(grouping->distinct_keys());
        return max_groups + rows <= capacity;
    }

    const Partials & partials;
    PartitionedPartials & parts;
    const cuda::stream_ref stream;

    std::optional<cudf::groupby::streaming_groupby> grouping;
    size_t capacity = 0;
    size_t max_groups = 0;
    size_t rows_grouped = 0;
};

class GroupBy
{
public:
    __host__ GroupBy(
        GPUSpan<GPUElementType> keys_,
        const std::vector<GPUElementType> & values_,
        GPUSpan<GPUDerivedValue> derived_,
        GPUSpan<GPUGroupByAggregate> aggregates_,
        size_t num_parts,
        cuda::stream_ref stream_)
        : keys(keys_.begin(), keys_.end())
        , values(values_)
        , aggregates(aggregates_.begin(), aggregates_.end())
        , stream(stream_)
        , keyless(keys_.empty())
        , keyless_key(0, stream_)
        , filter_mask(0, stream_)
        , all_true(0, stream_)
        , derived(derived_, stream_)
        , partials(aggregates_, values, keys.size() + (keyless ? 1 : 0), stream_)
        , parts(partials, num_parts, stream_)
        , streaming(partials, parts, stream_)
        , widened_offsets(keys_.size() + aggregates_.size())
    {
    }

    __host__ void addBatch(
        GPUSpan<DeviceColumnView> batch_keys,
        GPUSpan<DeviceColumnView> batch_values,
        GPUSpan<DeviceFixedColumn> filter_columns,
        const GPUExpression * filter)
    {
        if (finalized)
            throwGPUError("a batch after the groups were finalized");

        const size_t num_batch_values = values.size() - derived.size();
        if (batch_keys.size() != keys.size() || batch_values.size() != num_batch_values)
            throwGPUError(
                ("a batch of " + std::to_string(batch_keys.size()) + " keys and " + std::to_string(batch_values.size()) + " values, expected "
                 + std::to_string(keys.size()) + " and " + std::to_string(num_batch_values)).c_str());

        const size_t rows = batch_keys.empty() ? batch_values[0].rows() : batch_keys[0].rows();
        if (rows == 0)
            return;

        std::vector<cudf::column_view> columns;
        if (keyless)
            columns.push_back(keylessKey(rows));
        appendColumns(columns, batch_keys, keys, rows);
        appendColumns(columns, batch_values, values, rows);

        /// Must live until the batch is aggregated.
        std::unique_ptr<cudf::table> compacted;
        if (filter)
        {
            const std::vector<GPUExpressionInput> inputs = inputsOf(*filter, filter_columns, rows);
            const DeviceBitmask kept = evaluateMask(
                *filter, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, filter_mask, stream);
            if (!applyFilter(columns, kept, compacted))
                return;
        }

        if (derived.size() != 0)
            derived.derive(columns, partials.keyIndices().size(), values);

        if (strategy == Strategy::Undecided)
            chooseStrategy(columns);
        else
            aggregate(columns);
    }

    __host__ void absorbPart(const GroupBy & other, size_t part)
    {
        if (finalized || parts.size() != 1 || !(partials == other.partials))
            throwGPUError("a part of the groups of another `GROUP BY` absorbed into one that cannot take it");

        {
            std::lock_guard lock(other.flushing);
            other.streaming.flush();
        }

        /// The originals are freed in the order of `other`'s stream, which waits for the copies, on an exception too.
        waitFor(stream, other.stream);
        std::vector<std::unique_ptr<cudf::table>> copies;
        try
        {
            copies = other.parts.copiesOf(part, stream);
        }
        catch (...)
        {
            waitFor(other.stream, stream);
            throw;
        }
        waitFor(other.stream, stream);

        for (auto & copy : copies)
            parts.add(std::move(copy));
    }

    __host__ size_t finalize()
    {
        if (finalized)
            throwGPUError("the groups were finalized twice");
        streaming.flush();
        if (auto merged = parts.mergeAll())
            groups = partials.finish(std::move(merged), keyless ? 1 : 0);
        finalized = true;
        return numGroups();
    }

    __host__ size_t filterGroups(const GPUExpression & having, GPUSpan<size_t> columns)
    {
        if (!finalized)
            throwGPUError("the groups were filtered before they were finalized");
        const size_t rows = numGroups();
        if (rows == 0)
            return 0;

        std::vector<DeviceFixedColumn> group_columns;
        group_columns.reserve(columns.size());
        for (const size_t column : columns)
        {
            if (column >= keys.size() + aggregates.size())
                throwGPUError(
                    ("a `HAVING` over column " + std::to_string(column) + " of " + std::to_string(keys.size()) + " keys and "
                     + std::to_string(aggregates.size()) + " aggregates").c_str());
            const DeviceColumnView view = column < keys.size() ? key(column) : value(column - keys.size());
            if (view.kind != GPUColumnKind::Fixed)
                throwGPUError(("a `HAVING` over column " + std::to_string(column) + ", which is not of a fixed width").c_str());
            group_columns.push_back(view.fixed);
        }

        const std::vector<GPUExpressionInput> inputs = inputsOf(having, group_columns, rows);
        const DeviceBitmask kept = evaluateMask(
            having, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, filter_mask, stream);
        const cudf::size_type left_out = leftOut(kept, groups->num_rows());
        if (left_out != 0)
            groups = keptRows(groups->view(), kept, left_out, all_true, stream);
        return numGroups();
    }

    __host__ DeviceColumnView key(size_t index) const
    {
        return viewOf(groups.get(), index, keys[index], widened_offsets[index], stream);
    }

    __host__ DeviceColumnView value(size_t index) const
    {
        const size_t column = keys.size() + index;
        return viewOf(groups.get(), column, aggregates[index].result_type, widened_offsets[column], stream);
    }

private:
    enum class Strategy
    {
        Undecided,
        SortedRuns,
        HashTable,
        Streaming,
    };

    __host__ size_t numGroups() const { return groups ? static_cast<size_t>(groups->num_rows()) : 0; }

    __host__ cudf::column_view keylessKey(size_t rows)
    {
        if (keyless_key.size() < rows)
        {
            keyless_key.resize(rows, stream);
            checkCuda(cudaMemsetAsync(keyless_key.data(), 0, rows, stream.get()), "Cannot fill the key of a `GROUP BY` without keys");
        }
        return cudf::column_view(cudf::data_type{cudf::type_id::INT8}, static_cast<cudf::size_type>(rows), keyless_key.data(), nullptr, 0);
    }

    __host__ cudf::size_type leftOut(DeviceBitmask kept, cudf::size_type rows) const
    {
        if (kept.rows != static_cast<size_t>(rows))
            throwGPUError(("a filter of " + std::to_string(kept.rows) + " rows over " + std::to_string(rows)).c_str());
        return leftOutBy(kept, stream);
    }

    /// Leaves out the rows that `kept` does not keep, by compacting the columns into `compacted` or by nulls in the first key.
    /// Sorted runs are always compacted, as cuDF sorts keys with nulls whatever it is told. Returns whether any row is kept.
    __host__ bool applyFilter(std::vector<cudf::column_view> & columns, DeviceBitmask kept, std::unique_ptr<cudf::table> & compacted)
    {
        const cudf::size_type rows = columns[0].size();
        const cudf::size_type left_out = leftOut(kept, rows);
        if (left_out == rows)
            return false;
        if (left_out == 0)
            return true;

        const bool sorted_runs = strategy == Strategy::Undecided || strategy == Strategy::SortedRuns;
        if (sorted_runs || static_cast<double>(left_out) >= min_left_out_share_to_compact * static_cast<double>(rows))
        {
            compacted = keptRows(cudf::table_view(columns), kept, left_out, all_true, stream);
            const cudf::table_view kept_rows = compacted->view();
            columns.assign(kept_rows.begin(), kept_rows.end());
        }
        else
        {
            columns[0] = withNullMask(columns[0], kept, left_out);
        }
        return true;
    }

    /// Aggregates the first batch as sorted runs, and chooses by their numbers of runs and groups.
    __host__ void chooseStrategy(const std::vector<cudf::column_view> & columns)
    {
        const auto rows = static_cast<double>(columns[0].size());
        const auto runs = partials.aggregate(columns, cudf::sorted::YES);
        const auto num_runs = static_cast<double>(runs->num_rows());
        auto groups_of_batch = partials.merge({runs->view()});
        const auto num_groups = static_cast<size_t>(groups_of_batch->num_rows());

        if (num_runs <= max_runs_per_row_for_sorted_runs * rows && static_cast<double>(num_groups) > min_groups_per_run_for_sorted_runs * num_runs)
            strategy = Strategy::SortedRuns;
        else if (num_groups <= max_groups_to_stream && partials.streamable())
            strategy = Strategy::Streaming;
        else
            strategy = Strategy::HashTable;
        parts.add(std::move(groups_of_batch));
    }

    __host__ void aggregate(const std::vector<cudf::column_view> & columns)
    {
        switch (strategy)
        {
            case Strategy::SortedRuns:
                parts.add(partials.aggregate(columns, cudf::sorted::YES));
                return;
            case Strategy::HashTable:
                parts.add(partials.aggregate(columns, cudf::sorted::NO));
                return;
            case Strategy::Streaming:
                streaming.add(cudf::table_view(columns));
                return;
            case Strategy::Undecided:
                break;
        }
        throwGPUError("a batch of a `GROUP BY` that did not choose how to aggregate it");
    }

    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const cuda::stream_ref stream;
    const bool keyless;
    rmm::device_buffer keyless_key;
    /// Of the `WHERE` of the last batch, or of the `HAVING`.
    rmm::device_buffer filter_mask;
    rmm::device_buffer all_true;
    DerivedValues derived;

    Partials partials;
    Strategy strategy = Strategy::Undecided;
    PartitionedPartials parts;
    mutable StreamingPartials streaming;
    /// Taken by the first merger, which flushes `streaming` into the parts.
    mutable std::mutex flushing;

    std::unique_ptr<cudf::table> groups;
    /// Of the columns of varying width of `groups`, see `viewOf`.
    mutable std::vector<std::unique_ptr<cudf::column>> widened_offsets;
    bool finalized = false;
};

class RowCollector
{
public:
    __host__ RowCollector(GPUSpan<GPUElementType> values_, cuda::stream_ref stream_)
        : values(values_.begin(), values_.end())
        , stream(stream_)
        , filter_mask(0, stream_)
        , all_true(0, stream_)
        , widened_offsets(values.size())
    {
    }

    __host__ void addBatch(GPUSpan<DeviceColumnView> batch_values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUExpression * filter)
    {
        if (batch_values.size() != values.size())
            throwGPUError(("a batch of " + std::to_string(batch_values.size()) + " columns to collect, expected " + std::to_string(values.size())).c_str());

        const size_t rows = batch_values[0].rows();
        if (rows == 0)
            return;

        std::vector<cudf::column_view> columns;
        appendColumns(columns, batch_values, values, rows);
        const cudf::table_view batch(columns);

        if (filter)
        {
            const std::vector<GPUExpressionInput> inputs = inputsOf(*filter, filter_columns, rows);
            const DeviceBitmask kept = evaluateMask(
                *filter, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, filter_mask, stream);
            const cudf::size_type left_out = leftOutBy(kept, stream);
            if (static_cast<size_t>(left_out) == rows)
                return;
            if (left_out != 0)
            {
                collected.push_back(keptRows(batch, kept, left_out, all_true, stream));
                return;
            }
        }

        /// A copy, as the batch's columns are freed once it is added.
        collected.push_back(guarded("keeping " + std::to_string(rows) + " rows", [&] { return std::make_unique<cudf::table>(batch, stream); }));
    }

    __host__ size_t collectedRows() const
    {
        size_t rows = 0;
        for (const auto & table : collected)
            rows += static_cast<size_t>(table->num_rows());
        return rows;
    }

    __host__ size_t take()
    {
        taken.reset();
        if (collected.size() == 1)
        {
            taken = std::move(collected.front());
        }
        else if (!collected.empty())
        {
            std::vector<cudf::table_view> views;
            views.reserve(collected.size());
            for (const auto & table : collected)
                views.push_back(table->view());
            taken = guarded("concatenating " + std::to_string(views.size()) + " batches of collected rows", [&]
            {
                return cudf::concatenate(views, stream);
            });
        }
        /// Freed in the order of the stream, after the concatenation has read them.
        collected.clear();
        return taken ? static_cast<size_t>(taken->num_rows()) : 0;
    }

    __host__ DeviceColumnView value(size_t index) const
    {
        if (index >= values.size())
            throwGPUError(("column " + std::to_string(index) + " of " + std::to_string(values.size()) + " collected").c_str());
        return viewOf(taken.get(), index, values[index], widened_offsets[index], stream);
    }

private:
    const std::vector<GPUElementType> values;
    const cuda::stream_ref stream;
    rmm::device_buffer filter_mask;
    rmm::device_buffer all_true;
    std::vector<std::unique_ptr<cudf::table>> collected;
    std::unique_ptr<cudf::table> taken;
    /// Of the columns of varying width of `taken`, see `viewOf`.
    mutable std::vector<std::unique_ptr<cudf::column>> widened_offsets;
};

}

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
    if (values.empty() || aggregates.empty())
        throwGPUError("a `GROUP BY` without values or aggregates");
    if (num_parts == 0 || num_parts > static_cast<size_t>(std::numeric_limits<int>::max()))
        throwGPUError(("a `GROUP BY` split into " + std::to_string(num_parts) + " parts").c_str());

    /// The values of a batch, then the derived ones.
    std::vector<GPUElementType> all_values(values.begin(), values.end());
    for (const auto & value : derived)
    {
        for (const uint32_t column : value.columns)
        {
            if (column >= values.size() || columnKindOf(values[column]) != GPUColumnKind::Fixed)
                throwGPUError(("a derived value of value " + std::to_string(column) + " of " + std::to_string(values.size())
                    + ", or of one of varying width").c_str());
        }
        all_values.push_back(value.expression.value_type);
    }

    for (const auto & aggregate : aggregates)
    {
        if ((aggregate.aggregation == GPUAggregationKind::Count) != (aggregate.column == GPUGroupByAggregate::no_column))
            throwGPUError("a `count` of a column, or another aggregate of none");
        if (aggregate.column != GPUGroupByAggregate::no_column && aggregate.column >= all_values.size())
            throwGPUError(("an aggregate of value " + std::to_string(aggregate.column) + " of " + std::to_string(all_values.size())).c_str());
    }
    impl = guarded("making a `GROUP BY`", [&] { return new cudf_group_by::GroupBy(keys, all_values, derived, aggregates, num_parts, stream); });
}

__host__ CudfGroupBy::~CudfGroupBy()
{
    delete impl;
}

/// What cuDF, rmm and libstdc++ throw does not leave the island, see `guarded`.

__host__ void CudfGroupBy::addBatch(
    GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUExpression * filter)
{
    guarded("adding a batch to a `GROUP BY`", [&] { impl->addBatch(keys, values, filter_columns, filter); });
}

__host__ void CudfGroupBy::absorbPart(const CudfGroupBy & other, size_t part)
{
    guarded("absorbing a part of the groups of a `GROUP BY`", [&] { impl->absorbPart(*other.impl, part); });
}

__host__ size_t CudfGroupBy::finalize()
{
    return guarded("finalizing a `GROUP BY`", [&] { return impl->finalize(); });
}

__host__ size_t CudfGroupBy::filterGroups(const GPUExpression & having, GPUSpan<size_t> columns)
{
    return guarded("filtering the groups of a `GROUP BY`", [&] { return impl->filterGroups(having, columns); });
}

__host__ DeviceColumnView CudfGroupBy::key(size_t index) const
{
    return guarded("viewing a key of a `GROUP BY`", [&] { return impl->key(index); });
}

__host__ DeviceColumnView CudfGroupBy::value(size_t index) const
{
    return guarded("viewing an aggregate of a `GROUP BY`", [&] { return impl->value(index); });
}

__host__ CudfRowCollector::CudfRowCollector(GPUSpan<GPUElementType> values, cuda::stream_ref stream)
{
    if (stream.get() == cudaStreamLegacy || stream.get() == nullptr)
        throwGPUError("a collection of rows on the device's default stream");
    if (values.empty())
        throwGPUError("a collection of rows of no columns");
    impl = guarded("making a collection of rows", [&] { return new cudf_group_by::RowCollector(values, stream); });
}

__host__ CudfRowCollector::~CudfRowCollector()
{
    delete impl;
}

__host__ void CudfRowCollector::addBatch(GPUSpan<DeviceColumnView> values, GPUSpan<DeviceFixedColumn> filter_columns, const GPUExpression * filter)
{
    guarded("collecting the rows of a batch", [&] { impl->addBatch(values, filter_columns, filter); });
}

__host__ size_t CudfRowCollector::collectedRows() const
{
    return guarded("counting the collected rows", [&] { return impl->collectedRows(); });
}

__host__ size_t CudfRowCollector::take()
{
    return guarded("taking the collected rows", [&] { return impl->take(); });
}

__host__ DeviceColumnView CudfRowCollector::value(size_t index) const
{
    return guarded("viewing a column of the collected rows", [&] { return impl->value(index); });
}

}
