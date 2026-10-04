#include <Processors/Transforms/DistinctTransform.h>

#include <algorithm>

#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Processors/QueryResultPreview.h>

namespace ProfileEvents
{
    extern const Event DistinctTransformsAbandonedDeduplication;
    extern const Event DistinctTransformsSwitchedToPassThrough;
}

namespace DB
{

bool DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return false;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    return unique_rate >= UNIQUE_RATE_THRESHOLD;
}

namespace
{
    template <typename Method>
    void buildDeduplicationFilter(
        Method & method, const ColumnRawPtrs & columns, const Sizes & key_sizes, IColumn::Filter & filter, size_t rows, SetVariants & variants)
    {
        typename Method::State state(columns, key_sizes, nullptr);

        for (size_t i = 0; i < rows; ++i)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            filter[i] = emplace_result.isInserted();
        }
    }
}

void deduplicateChunkForQueryResultPreview(Chunk & chunk, const ColumnNumbers & key_columns_pos)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    const auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// Only const columns - a single row remains.
    if (unlikely(key_columns_pos.empty()))
    {
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        return;
    }

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    Sizes key_sizes;
    SetVariants set;
    set.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    IColumn::Filter filter(num_rows);
    switch (set.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildDeduplicationFilter(*set.NAME, column_ptrs, key_sizes, filter, num_rows, set); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    const auto num_selected = set.getTotalRowCount();
    if (num_selected == num_rows)
    {
        chunk.setColumns(std::move(columns), num_rows);
        return;
    }

    for (auto & column : columns)
        column = column->filter(filter, num_selected);

    chunk.setColumns(std::move(columns), num_selected);
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool allow_abandoning_,
    bool skip_null_keys_,
    const UInt64 max_bytes_before_pass_through_)
    : ISimpleTransform(header_, header_, true)
    , distinct_set(std::in_place, *header_, columns_, set_size_limits_, skip_null_keys_)
    , limit_hint(limit_hint_)
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Releasing the filter permanently switches subsequent chunks to pass-through, previews included:
    /// a preview that is deduplicated while the result is not would not match the result.
    if (!distinct_set)
        return;

    /// A query result preview is a self-contained chunk (see `QueryResultPreview.h`): deduplicate
    /// it standalone. The accumulated set must not filter preview rows, and preview rows must not
    /// poison the set (that would filter real rows out of the result). A preview must not stop the
    /// input either, which is why it takes none of the paths below that do.
    if (isQueryResultPreview(chunk))
    {
        deduplicateChunkForQueryResultPreview(chunk, distinct_set->getKeyColumnsPositions());
        return;
    }

    /// A constant `NULL` key component makes every key contain a `NULL`, so a consumer that skips `NULL`
    /// keys drops all rows; emit nothing and stop the input.
    if (distinct_set->hasConstNullKey())
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Special case - only const columns, return single row.
    if (unlikely(!distinct_set->hasKeyColumns()))
    {
        removeSpecialColumnRepresentations(chunk);
        convertToFullIfConst(chunk);

        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    if (max_bytes_before_pass_through)
    {
        distinct_set->prepareForInsert(chunk);

        /// Preliminary hashing shares the query's remaining spill-threshold budget with the final
        /// transform and other operators.
        const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
        const UInt64 available_memory = max_bytes_before_pass_through - std::min(max_bytes_before_pass_through, query_memory_usage);

        const size_t filtering_memory = distinct_set->estimateFilteringMemory(chunk);
        const size_t growth_memory = distinct_set->estimateGrowthMemory(chunk);
        if (filtering_memory > available_memory || growth_memory > available_memory - filtering_memory)
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: {} "
                "(query memory: {}, spill threshold: {}, "
                "estimated peak extra memory for growth: {}, filtering workspace: {})",
                query_memory_usage > max_bytes_before_pass_through
                    ? "query memory exceeded the spill threshold"
                    : "projected allocations exceed the remaining spill-threshold budget",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through),
                formatReadableSizeWithBinarySuffix(growth_memory),
                formatReadableSizeWithBinarySuffix(filtering_memory));

            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }

    const size_t num_rows = chunk.getNumRows();
    chunk = distinct_set->filter(std::move(chunk));

    /// Return the current chunk and stop before releasing the set if a size limit or the hint is reached.
    if (distinct_set->isLimitReached() || (limit_hint && distinct_set->getTotalRowCount() >= limit_hint))
    {
        stopReading();
        return;
    }

    if (abandon_controller)
    {
        /// The rate is measured against the rows the transform received: the rows dropped as `NULL` keys
        /// (in the `skip_null_keys` mode, inside the filter) count as removed by the deduplication, so a
        /// stream that mostly consists of `NULL` keys keeps the transform even when the non-`NULL` part is
        /// unique - dropping the `NULL` rows is exactly the reduction the consumer benefits from.
        if (abandon_controller->update(num_rows, chunk.getNumRows(), distinct_set->getTotalByteCount()))
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching DISTINCT to pass-through: input is mostly unique (retained keys: {}, set memory: {})",
                distinct_set->getTotalRowCount(), formatReadableSizeWithBinarySuffix(distinct_set->getTotalByteCount()));

            /// The new rows of the current chunk are still emitted (the following chunks flow
            /// through unfiltered).
            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
            return;
        }
    }

    /// Preliminary hashing can release its set under memory pressure because a downstream step
    /// deduplicates the output exactly. This also gives up any remaining local limit hint. The set
    /// can be released even when the current chunk produces no new rows.
    if (max_bytes_before_pass_through)
    {
        const Int64 query_memory_usage = getCurrentQueryMemoryUsage();
        if (query_memory_usage > static_cast<Int64>(max_bytes_before_pass_through))
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: query memory exceeded the spill threshold after insertion "
                "(query memory: {}, spill threshold: {})",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through));

            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }
}

}
