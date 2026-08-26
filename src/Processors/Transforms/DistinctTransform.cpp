#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Mark rows whose `LowCardinality` index is the dictionary's NULL entry with 0 in `keep`, allocating
/// the filter lazily on the first such row.
void markLowCardinalityNullRows(const ColumnLowCardinality & column, IColumn::Filter & keep, size_t num_rows)
{
    const size_t null_index = column.getDictionary().getNullValueIndex();
    const IColumn & indexes_column = *column.getIndexesPtr();

    auto process = [&](const auto & indexes)
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (static_cast<size_t>(indexes[row]) == null_index)
            {
                if (keep.empty())
                    keep.assign(num_rows, static_cast<UInt8>(1));
                keep[row] = 0;
            }
        }
    };

    switch (column.getSizeOfIndexType())
    {
        case sizeof(UInt8): process(assert_cast<const ColumnUInt8 &>(indexes_column).getData()); break;
        case sizeof(UInt16): process(assert_cast<const ColumnUInt16 &>(indexes_column).getData()); break;
        case sizeof(UInt32): process(assert_cast<const ColumnUInt32 &>(indexes_column).getData()); break;
        case sizeof(UInt64): process(assert_cast<const ColumnUInt64 &>(indexes_column).getData()); break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }
}

}

void DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    if (abandoned)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    abandoned = unique_rate >= UNIQUE_RATE_THRESHOLD;
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
    , distinct_set(*header_, columns_, set_size_limits_)
    , limit_hint(limit_hint_)
    , skip_null_keys(skip_null_keys_)
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();

    if (skip_null_keys)
    {
        const size_t num_columns = columns_.empty() ? header_->columns() : columns_.size();
        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto pos = columns_.empty() ? i : header_->getPositionByName(columns_[i]);
            const auto & col = header_->getByPosition(pos).column;
            if (col && isColumnConst(*col) && col->isNullAt(0))
                const_null_key = true;
        }
    }
}

void DistinctTransform::skipNullKeyRows(Chunk & chunk) const
{
    /// The null maps are inspected directly, so the special column representations must be unwrapped
    /// first (DistinctSetFilter does the same for its own hashing anyway).
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs key_columns;
    key_columns.reserve(distinct_set.getKeyColumnsPositions().size());
    for (const auto pos : distinct_set.getKeyColumnsPositions())
        key_columns.emplace_back(columns[pos].get());

    ConstNullMapPtr null_map = nullptr;
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    IColumn::Filter keep;
    if (null_map && !memoryIsZero(null_map->data(), 0, num_rows))
    {
        keep.resize(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
            keep[i] = !(*null_map)[i];
    }

    /// `LowCardinality(Nullable)` keys are not unwrapped by extractNestedColumnsAndNullMap: their NULL
    /// rows are the rows referencing the dictionary's NULL entry.
    for (const auto * column : key_columns)
        if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
            low_cardinality && low_cardinality->nestedIsNullable())
            markLowCardinalityNullRows(*low_cardinality, keep, num_rows);

    if (!keep.empty())
    {
        const auto num_kept = countBytesInFilter(keep);
        for (auto & column : columns)
            column = column->filter(keep, num_kept);
        chunk.setColumns(std::move(columns), num_kept);
        return;
    }

    chunk.setColumns(std::move(columns), num_rows);
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// The set was dropped: under memory pressure (pass_through) or because the deduplication was not
    /// removing enough rows (abandoned); the chunk flows through unchanged.
    if (pass_through || (abandon_controller && abandon_controller->isAbandoned()))
        return;

    if (const_null_key)
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Special case - only const columns, return single row.
    if (unlikely(!distinct_set.hasKeyColumns()))
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

    /// The consumer skips rows with a NULL in any key component (a set fill with
    /// `transform_null_in = 0` strips `LowCardinality` and then drops such rows), so they carry no
    /// value downstream: drop them before deduplication and before the abandon accounting.
    if (skip_null_keys)
    {
        skipNullKeyRows(chunk);
        if (!chunk.hasRows())
            return;
    }

    const size_t num_rows = chunk.getNumRows();
    chunk = distinct_set.filter(std::move(chunk));

    /// In case of overflow_mode = 'break' the limits check inside the filter does not throw. Stop
    /// reading, but still emit the new rows of the current chunk (their keys are already in the set):
    /// 'break' means return a partial result as if the source data ran out, not discard it.
    if (distinct_set.isLimitReached())
        stopReading();

    if (abandon_controller)
    {
        abandon_controller->update(num_rows, chunk.getNumRows(), distinct_set.getTotalByteCount());
        if (abandon_controller->isAbandoned())
        {
            /// The new rows of the current chunk are still emitted (the following chunks flow
            /// through unfiltered).
            distinct_set.clear();
            return;
        }
    }

    /// A preliminary DISTINCT is only an optimization, its result does not have to be exact. When the
    /// memory usage of the query exceeds the threshold, free the set and let the final DISTINCT (which is
    /// able to spill to disk) deal with the duplicates. This check does not depend on the chunk: the set
    /// must be freed under memory pressure even when the chunks stop producing new rows.
    if (max_bytes_before_pass_through && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_pass_through))
    {
        LOG_DEBUG(
            getLogger("DistinctTransform"),
            "Query memory usage exceeded the threshold ({}), preliminary DISTINCT switches to pass-through",
            formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through));

        distinct_set.clear();
        pass_through = true;
        return;
    }

    /// Nothing new in this chunk.
    if (!chunk.hasRows())
        return;

    /// Stop reading if we already reached the limit.
    if (limit_hint && distinct_set.getTotalRowCount() >= limit_hint)
        stopReading();
}

}
