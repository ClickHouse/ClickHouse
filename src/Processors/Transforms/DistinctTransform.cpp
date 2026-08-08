#include <Processors/Transforms/DistinctTransform.h>

#include <algorithm>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/FailPoint.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
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

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    QueryStatusPtr process_list_element_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , process_list_element(std::move(process_list_element_))
{
    const size_t num_columns = columns_.empty() ? header_->columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_->getPositionByName(columns_[i]);
        const auto & col = header_->getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
    }
}

template <typename Method>
void DistinctTransform::buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumn::Filter & filter,
    const size_t rows,
    SetVariants & variants,
    const IColumn::Filter * mask,
    const size_t processed_prefix) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if ((i & 0xFFF) == 0)
            {
                if (i > 0) [[unlikely]]
                    FailPointInjection::pauseFailPoint("distinct_transform_pause");
                /// Rows in [0, processed_prefix) are already committed work of a preceding
                /// `buildLowCardinalityMask` call and must be hashed even if the soft timeout
                /// already fired, so the processed prefix is preserved instead of dropped.
                /// A hard cancellation is checked unconditionally: it must not be suppressed by
                /// the processed prefix, otherwise `KILL QUERY` after a soft timeout would keep
                /// hashing every committed row before it is noticed.
                if (isCancelled() || (i >= processed_prefix && isSoftTimeout()))
                {
                    std::fill(filter.begin() + i, filter.end(), 0);
                    return;
                }
            }

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
            if ((i & 0xFFF) == 0)
            {
                if (i > 0) [[unlikely]]
                    FailPointInjection::pauseFailPoint("distinct_transform_pause");
                if (isCancelled() || (i >= processed_prefix && isSoftTimeout()))
                {
                    std::fill(filter.begin() + i, filter.end(), 0);
                    return;
                }
            }

            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        }
    }
}

LowCardinalityMaskResult DistinctTransform::buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows)
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
        return {{}, 0, num_rows}; /// empty mask == no candidates

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
            {
                if ((row & 0xFFF) == 0)
                {
                    if (row > 0) [[unlikely]]
                        FailPointInjection::pauseFailPoint("distinct_transform_lc_pause");
                    if (isCancelled() || isSoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                }
                handle_index(static_cast<size_t>(col[row]), row);
            }
            break;
        }
        case sizeof(UInt16):
        {
            const auto & col = assert_cast<const ColumnUInt16 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
            {
                if ((row & 0xFFF) == 0)
                {
                    if (row > 0) [[unlikely]]
                        FailPointInjection::pauseFailPoint("distinct_transform_lc_pause");
                    if (isCancelled() || isSoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                }
                handle_index(static_cast<size_t>(col[row]), row);
            }
            break;
        }
        case sizeof(UInt32):
        {
            const auto & col = assert_cast<const ColumnUInt32 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
            {
                if ((row & 0xFFF) == 0)
                {
                    if (row > 0) [[unlikely]]
                        FailPointInjection::pauseFailPoint("distinct_transform_lc_pause");
                    if (isCancelled() || isSoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                }
                handle_index(static_cast<size_t>(col[row]), row);
            }
            break;
        }
        case sizeof(UInt64):
        {
            const auto & col = assert_cast<const ColumnUInt64 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
            {
                if ((row & 0xFFF) == 0)
                {
                    if (row > 0) [[unlikely]]
                        FailPointInjection::pauseFailPoint("distinct_transform_lc_pause");
                    if (isCancelled() || isSoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                }
                handle_index(static_cast<size_t>(col[row]), row);
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }

    return {std::move(mask), state.seen_count - seen_count_before, num_rows};
}

bool DistinctTransform::isSoftTimeout() const
{
    if (time_limit_exceeded)
        return true;

    if (!process_list_element)
        return false;

    /// Hard cancellations (KILL QUERY, Ctrl+C, executor cancellation) are surfaced through
    /// `isCancelled`. Do not treat them as soft timeouts, and do not call `checkTimeLimit`
    /// for them either - it would throw the cancellation exception from the hot loop.
    if (process_list_element->getCancelReason() != DB::CancelReason::UNDEFINED)
        return false;

    /// In `timeout_overflow_mode = 'throw'` this throws TIMEOUT_EXCEEDED; in `'break'` mode it
    /// returns false, which we latch so every loop stops and the processed prefix is preserved.
    if (!process_list_element->checkTimeLimit())
    {
        time_limit_exceeded = true;
        return true;
    }

    return false;
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (isCancelled())
    {
        chunk.clear();
        stopReading();
        return;
    }

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    const auto num_rows = chunk.getNumRows();
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

    std::optional<IColumn::Filter> lc_mask;
    size_t processed_prefix = 0;

    if (lc_optimization_controller.isEnabled() && key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            auto [mask, new_indices_count, processed_rows] = buildLowCardinalityMask(*lc, num_rows);
            lc_optimization_controller.update(num_rows, new_indices_count);

            if (isCancelled())
            {
                chunk.clear();
                stopReading();
                return;
            }

            lc_mask.emplace(std::move(mask));

            /// Empty mask -> no candidate rows in this chunk, emit nothing.
            if (lc_mask->empty())
            {
                if (time_limit_exceeded)
                    stopReading();
                return;
            }

            /// On a soft timeout `buildLowCardinalityMask` returned a partial mask covering rows
            /// [0, processed_rows); those rows are committed work and must be hashed even though the
            /// time limit already fired, otherwise the processed prefix would be dropped.
            processed_prefix = time_limit_exceeded ? processed_rows : 0;
        }
    }

    if (data.empty())
        data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    const auto old_set_size = data.getTotalRowCount();
    IColumn::Filter filter(num_rows);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildFilter(*data.NAME, column_ptrs, filter, num_rows, data, lc_mask ? &*lc_mask : nullptr, processed_prefix); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    if (isCancelled())
    {
        chunk.clear();
        stopReading();
        return;
    }

    /// Soft timeout (break mode): emit the already-processed prefix and stop reading, as if the
    /// source ran out. Hard cancellation above keeps dropping the whole chunk.
    if (time_limit_exceeded)
        stopReading();

    const auto new_set_size = data.getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (num_selected == 0)
        return;

    /// In case of overflow_mode = 'break' `check` returns false instead of throwing.
    /// Stop reading, but still emit the new rows from the current chunk (their keys are
    /// already in the set): 'break' means return a partial result as if the source data
    /// ran out, not discard it.
    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
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
