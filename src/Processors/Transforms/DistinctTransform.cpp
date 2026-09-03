#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

namespace ProfileEvents
{
    extern const Event DistinctTransformsAbandonedDeduplication;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
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

void LCOptimizationController::update(size_t num_rows, size_t new_indices_in_chunk)
{
    if (state != State::Observing)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    new_indices_observed += new_indices_in_chunk;

    if (chunks_observed >= OBSERVATION_CHUNK_COUNT)
    {
        double new_index_rate = static_cast<double>(new_indices_observed) / static_cast<double>(rows_observed);

        /// Disable when the mask is almost a no-op: nearly every row introduces
        /// a new dictionary index, so the bitmap bookkeeping is pure overhead.
        if (new_index_rate >= NEW_INDEX_RATE_THRESHOLD)
            state = State::Disabled;
        else
            state = State::Enabled;
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
    bool skip_null_keys_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , skip_null_keys(skip_null_keys_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();

    const size_t num_columns = columns_.empty() ? header_->columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_->getPositionByName(columns_[i]);
        const auto & col = header_->getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
        else if (skip_null_keys && col && col->isNullAt(0))
            const_null_key = true;
    }
}

template <typename Method>
void DistinctTransform::buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumn::Filter & filter,
    const size_t rows,
    SetVariants & variants,
    const IColumn::Filter * mask) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (!(*mask)[i])
            {
                /// Already known duplicate row (by LC index), skip insertion
                filter[i] = 0;
                continue;
            }

            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            filter[i] = emplace_result.isInserted();
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        }
    }
}

std::pair<IColumn::Filter, size_t> DistinctTransform::buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows)
{
    const auto & dictionary = column.getDictionary();
    const auto dict_size = dictionary.size();

    LCDictionaryKey dict_key;
    dict_key.hash = dictionary.getHash();
    dict_key.size = dict_size;

    auto & state = lc_dict_states[dict_key];

    /// The first time we see this dictionary, initialize the seen_indices array to keep track which entries
    /// in the dictionary have been seen.
    chassert(state.seen_count <= dict_size);
    if (state.seen_indices.size() != dict_size)
    {
        chassert(state.seen_indices.empty());
        chassert(state.seen_count == 0);
        state.seen_indices.resize_fill(dict_size);
    }

    /// If we've already seen all dictionary indices for this dictionary,
    /// then no row in this chunk (and also other chunks with the same dictionary) can produce a new distinct value.
    if (state.seen_count == dict_size)
        return {{}, 0}; /// empty mask == no candidates

    const auto seen_count_before = state.seen_count;
    auto & seen = state.seen_indices;

    const auto index_type_size = column.getSizeOfIndexType();
    const IColumn & indexes_column = *column.getIndexesPtr();

    IColumn::Filter mask;

    auto handle_index = [&](size_t idx, size_t row)
    {
        chassert(idx < dict_size);
        if (!seen[idx])
        {
            seen[idx] = 1;
            ++state.seen_count;

            if (mask.empty())
                mask.resize_fill(num_rows);

            mask[row] = 1; /// first time we see this dictionary index for this dictionary
        }
    };

    switch (index_type_size)
    {
        case sizeof(UInt8):
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt16):
        {
            const auto & col = assert_cast<const ColumnUInt16 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt32):
        {
            const auto & col = assert_cast<const ColumnUInt32 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt64):
        {
            const auto & col = assert_cast<const ColumnUInt64 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }

    return {std::move(mask), state.seen_count - seen_count_before};
}

void DistinctTransform::maybeAbandonDeduplication(size_t num_rows, size_t num_unique_rows)
{
    if (!abandon_controller)
        return;

    abandon_controller->update(num_rows, num_unique_rows, data->getTotalByteCount());
    if (abandon_controller->isAbandoned())
    {
        data.reset();
        lc_dict_states.clear();
        ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (abandon_controller && abandon_controller->isAbandoned())
        return;

    if (const_null_key)
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// Special case, - only const columns, return single row
    if (unlikely(key_columns_pos.empty()))
    {
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    /// The consumer skips rows with a NULL in any key component (a set fill with
    /// `transform_null_in = 0` strips `LowCardinality` and then drops such rows), so they carry no
    /// value downstream: drop them before deduplication and before the abandon accounting. Plain
    /// `Nullable` keys are then hashed by their nested columns, the same way the set fill hashes them.
    ColumnPtr null_map_holder;
    if (skip_null_keys)
    {
        ConstNullMapPtr null_map = nullptr;
        null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);

        IColumn::Filter keep;
        if (null_map && !memoryIsZero(null_map->data(), 0, num_rows))
        {
            keep.resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                keep[i] = !(*null_map)[i];
        }

        for (const auto * column : column_ptrs)
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
                low_cardinality && low_cardinality->nestedIsNullable())
                markLowCardinalityNullRows(*low_cardinality, keep, num_rows);

        if (!keep.empty())
        {
            const auto num_kept = countBytesInFilter(keep);
            for (auto & column : columns)
                column = column->filter(keep, num_kept);
            num_rows = num_kept;

            if (num_rows == 0)
            {
                chunk.setColumns(std::move(columns), 0);
                return;
            }

            column_ptrs.clear();
            for (auto pos : key_columns_pos)
                column_ptrs.emplace_back(columns[pos].get());
            null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);
        }
    }

    std::optional<IColumn::Filter> lc_mask;

    if (lc_optimization_controller.isEnabled() && key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            auto [mask, new_indices_count] = buildLowCardinalityMask(*lc, num_rows);
            lc_optimization_controller.update(num_rows, new_indices_count);
            lc_mask.emplace(std::move(mask));

            /// Empty mask -> no candidate rows in this chunk, emit nothing. The chunk is fully
            /// duplicate, which is the strongest evidence in favor of keeping the deduplication, so
            /// the abandon accounting must see it.
            if (lc_mask->empty())
            {
                maybeAbandonDeduplication(num_rows, 0);
                return;
            }
        }
    }

    if (data->empty())
        data->init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    const auto old_set_size = data->getTotalRowCount();
    IColumn::Filter filter(num_rows);

    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildFilter(*data->NAME, column_ptrs, filter, num_rows, *data, lc_mask ? &*lc_mask : nullptr); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    const auto new_set_size = data->getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    maybeAbandonDeduplication(num_rows, num_selected);

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (num_selected == 0)
        return;

    /// In case of overflow_mode = 'break' `check` returns false instead of throwing.
    /// Stop reading, but still emit the new rows from the current chunk (their keys are
    /// already in the set): 'break' means return a partial result as if the source data
    /// ran out, not discard it.
    if (!set_size_limits.check(new_set_size, data ? data->getTotalByteCount() : 0, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        stopReading();

    if (num_selected == num_rows)
    {
        /// Every row is a new distinct value: keep the chunk unchanged, without copying it.
        chunk.setColumns(std::move(columns), num_rows);
    }
    else
    {
        for (auto & column : columns)
            column = column->filter(filter, -1);

        chunk.setColumns(std::move(columns), num_selected);
    }

    /// Stop reading if we already reach the limit
    if (limit_hint && new_set_size >= limit_hint)
        stopReading();
}

}
