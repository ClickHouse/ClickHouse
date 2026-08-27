#include <Processors/Transforms/DistinctTransform.h>

#include <algorithm>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
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

namespace
{

/// Mark rows whose `LowCardinality` index is the dictionary's NULL entry with 0 in `keep`, allocating
/// the filter lazily on the first such row.
void markLowCardinalityNullRowsRange(const ColumnLowCardinality & column, IColumn::Filter & keep, size_t begin, size_t end)
{
    const size_t null_index = column.getDictionary().getNullValueIndex();
    const IColumn & indexes_column = *column.getIndexesPtr();

    auto process = [&](const auto & indexes)
    {
        for (size_t row = begin; row < end; ++row)
        {
            if (static_cast<size_t>(indexes[row]) == null_index)
                keep[row] = 0;
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
    QueryStatusPtr process_list_element_,
    bool allow_abandoning_,
    bool skip_null_keys_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , process_list_element(std::move(process_list_element_))
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
                /// A hard cancellation is checked first and must not be suppressed by the
                /// processed prefix, otherwise `KILL QUERY` after a soft timeout would keep
                /// hashing every committed row before it is noticed. `isCancelledBySoftTimeout`
                /// recognizes the executor-side break-mode cancel
                /// (`PipelineExecutor::checkTimeLimitSoft` -> `CancelledByTimeout`), which leaves
                /// `cancel_reason` `UNDEFINED` and must still preserve the prefix.
                if (isCancelled() && !isCancelledBySoftTimeout())
                {
                    if (timeoutShouldThrow())
                        process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                    std::fill(filter.begin() + i, filter.end(), 0);
                    return;
                }

                if (i >= processed_prefix && isSoftTimeout())
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
                if (isCancelled() && !isCancelledBySoftTimeout())
                {
                    if (timeoutShouldThrow())
                        process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                    std::fill(filter.begin() + i, filter.end(), 0);
                    return;
                }

                if (i >= processed_prefix && isSoftTimeout())
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
    /// Whether a soft timeout was already latched by an upstream stage (e.g. the `skip_null_keys`
    /// null-marking prepass) before this scan began. When pre-latched, a prefix has already been
    /// committed and must be emitted whole; the scan must not drop part of it at a 4096-boundary.
    const bool pre_latched = time_limit_exceeded;
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
                    /// A hard cancellation (KILL) aborts the chunk immediately.
                    if (isCancelled() && !isCancelledBySoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                    /// A soft timeout already latched by an upstream stage (e.g. the `skip_null_keys`
                    /// null-marking prepass) committed a prefix before this scan began; emit that whole
                    /// prefix instead of dropping the tail of it at the first 4096-row boundary. Only bail
                    /// at a boundary when the timeout fires *during* this scan (i.e. not pre-latched).
                    if (row == 0 && pre_latched)
                    {
                        // fall through and keep scanning the committed prefix
                    }
                    else if (!pre_latched && (isSoftTimeout() || isCancelled()))
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
                    /// A hard cancellation (KILL) aborts the chunk immediately.
                    if (isCancelled() && !isCancelledBySoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                    /// A soft timeout already latched by an upstream stage (e.g. the `skip_null_keys`
                    /// null-marking prepass) committed a prefix before this scan began; emit that whole
                    /// prefix instead of dropping the tail of it at the first 4096-row boundary. Only bail
                    /// at a boundary when the timeout fires *during* this scan (i.e. not pre-latched).
                    if (row == 0 && pre_latched)
                    {
                        // fall through and keep scanning the committed prefix
                    }
                    else if (!pre_latched && (isSoftTimeout() || isCancelled()))
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
                    /// A hard cancellation (KILL) aborts the chunk immediately.
                    if (isCancelled() && !isCancelledBySoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                    /// A soft timeout already latched by an upstream stage (e.g. the `skip_null_keys`
                    /// null-marking prepass) committed a prefix before this scan began; emit that whole
                    /// prefix instead of dropping the tail of it at the first 4096-row boundary. Only bail
                    /// at a boundary when the timeout fires *during* this scan (i.e. not pre-latched).
                    if (row == 0 && pre_latched)
                    {
                        // fall through and keep scanning the committed prefix
                    }
                    else if (!pre_latched && (isSoftTimeout() || isCancelled()))
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
                    /// A hard cancellation (KILL) aborts the chunk immediately.
                    if (isCancelled() && !isCancelledBySoftTimeout())
                        return {std::move(mask), state.seen_count - seen_count_before, row};
                    /// A soft timeout already latched by an upstream stage (e.g. the `skip_null_keys`
                    /// null-marking prepass) committed a prefix before this scan began; emit that whole
                    /// prefix instead of dropping the tail of it at the first 4096-row boundary. Only bail
                    /// at a boundary when the timeout fires *during* this scan (i.e. not pre-latched).
                    if (row == 0 && pre_latched)
                    {
                        // fall through and keep scanning the committed prefix
                    }
                    else if (!pre_latched && (isSoftTimeout() || isCancelled()))
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

    /// Hard cancellations (KILL QUERY, Ctrl+C) are surfaced through `isCancelled` with a
    /// non-`UNDEFINED` cancel reason. Do not treat them as soft timeouts, and do not call
    /// `checkTimeLimit` for them either - it would throw the cancellation exception from the hot
    /// loop. An executor-side break-mode cancel (`CancelledByTimeout`) leaves the cancel reason
    /// `UNDEFINED`, so it is still recognized here as a soft timeout and the committed prefix is
    /// preserved (see the two-part checks in `buildFilter` and `isCancelledBySoftTimeout`).
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

bool DistinctTransform::isCancelledBySoftTimeout() const
{
    if (!process_list_element)
        return false;

    /// The executor poll loop (`PullingAsyncPipelineExecutor::pull`) calls `checkTimeLimitSoft` on
    /// every poll regardless of `timeout_overflow_mode`, and `checkTimeLimitSoft` always uses
    /// `OverflowMode::BREAK`. So a `timeout_overflow_mode = 'throw'` query is also cancelled with a
    /// break-style `CancelledByTimeout` (cancel_reason stays `UNDEFINED`). For such a query the
    /// timeout must raise `TIMEOUT_EXCEEDED` (handled by `timeoutShouldThrow`), not preserve a prefix,
    /// so only the BREAK mode is treated as a soft prefix-preserving timeout here.
    if (process_list_element->getOverflowMode() != OverflowMode::BREAK)
        return false;

    /// User-facing hard cancellations (KILL QUERY, Ctrl+C) always set a cancel reason; they win
    /// over any previously latched soft timeout, so the chunk must be dropped rather than kept.
    if (process_list_element->getCancelReason() != DB::CancelReason::UNDEFINED)
        return false;

    if (time_limit_exceeded)
        return true;

    /// The break-mode `max_execution_time` may be observed by the executor poll loop
    /// (`PipelineExecutor::checkTimeLimitSoft`), which cancels the whole pipeline with
    /// `CancelledByTimeout` without setting a user-facing cancel reason: `is_cancelled` is true
    /// while `cancel_reason` is still `UNDEFINED`. Recognizing that as a soft timeout keeps the
    /// already-committed chunk prefix instead of dropping it. `checkTimeLimitSoft` never throws (it
    /// uses `OverflowMode::BREAK` semantics internally), so it is safe here after the hot loops.
    ///
    /// `checkTimeLimitSoft` short-circuits on `is_killed` and returns false for a concurrent
    /// `KILL QUERY` as well as for a real timeout. Without a re-check a hard kill landing between the
    /// `cancel_reason` test above and this call would be misclassified as a soft timeout and the chunk
    /// prefix would be preserved instead of dropped. Re-test `cancel_reason` so a hard kill wins
    /// immediately and only a genuine soft timeout is latched.
    if (!process_list_element->checkTimeLimitSoft())
    {
        if (process_list_element->getCancelReason() != DB::CancelReason::UNDEFINED)
            return false;
        time_limit_exceeded = true;
        return true;
    }

    return false;
}

bool DistinctTransform::timeoutShouldThrow() const
{
    if (!process_list_element)
        return false;

    /// A timeout observed via the executor poll loop (or `checkTimeLimit`) leaves the cancel reason
    /// `UNDEFINED`, while a `KILL QUERY` sets a real cancel reason. Only a timeout in THROW mode must
    /// surface as `TIMEOUT_EXCEEDED`; a KILL is reported by the pipeline on its own.
    return process_list_element->getCancelReason() == DB::CancelReason::UNDEFINED
        && process_list_element->getOverflowMode() == OverflowMode::THROW;
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (isCancelled())
    {
        if (timeoutShouldThrow())
            process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
        chunk.clear();
        stopReading();
        return;
    }

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
            {
                if ((i & 0xFFF) == 0)
                {
                    if (isCancelled() && !isCancelledBySoftTimeout())
                    {
                        if (timeoutShouldThrow())
                            process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                        chunk.clear();
                        stopReading();
                        return;
                    }
                    if (isSoftTimeout())
                    {
                        /// Break-mode soft timeout: drop the unprocessed tail and stop building the
                        /// null map for this chunk; the rest of the transform handles the prefix.
                        std::fill(keep.begin() + i, keep.end(), 0);
                        break;
                    }
                    if (i > 0) [[unlikely]]
                        FailPointInjection::pauseFailPoint("distinct_transform_null_pause");
                }
                keep[i] = !(*null_map)[i];
            }
        }

        if (isCancelled() && !isCancelledBySoftTimeout())
        {
            if (timeoutShouldThrow())
                process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
            chunk.clear();
            stopReading();
            return;
        }

        for (const auto * column : column_ptrs)
        {
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
                low_cardinality && low_cardinality->nestedIsNullable())
            {
                /// `keep` must already be sized to `num_rows` so the per-range call only flips null rows.
                if (keep.empty())
                    keep.assign(num_rows, static_cast<UInt8>(1));
                for (size_t begin = 0; begin < num_rows; begin += 0x1000)
                {
                    if (isCancelled() && !isCancelledBySoftTimeout())
                    {
                        if (timeoutShouldThrow())
                            process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                        chunk.clear();
                        stopReading();
                        return;
                    }
                    if (isSoftTimeout())
                    {
                        /// Break-mode soft timeout: drop the unprocessed tail so only the already-marked
                        /// prefix survives; the rest of the transform preserves the committed prefix
                        /// (including through `buildLowCardinalityMask`, which is taught not to discard an
                        /// upstream-committed prefix when it sees a pre-latched soft timeout).
                        std::fill(keep.begin() + begin, keep.end(), 0);
                        break;
                    }
                    if (begin > 0) [[unlikely]]
                        FailPointInjection::pauseFailPoint("distinct_transform_null_pause");
                    markLowCardinalityNullRowsRange(*low_cardinality, keep, begin, std::min(begin + 0x1000, num_rows));
                }
            }
        }

        if (isCancelled() && !isCancelledBySoftTimeout())
        {
            if (timeoutShouldThrow())
                process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
            chunk.clear();
            stopReading();
            return;
        }

        if (!keep.empty())
        {
            const auto num_kept = countBytesInFilter(keep);
            /// Whether the soft timeout was already latched by an upstream stage (e.g. the `skip_null_keys`
            /// null-marking prepass) before this materialization began. When pre-latched, the committed
            /// prefix must not be truncated here; only a soft timeout that fires *during* this pass may.
            const bool filter_pre_latched = time_limit_exceeded;

            if (isCancelled() && !isCancelledBySoftTimeout())
            {
                if (timeoutShouldThrow())
                    process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                chunk.clear();
                stopReading();
                return;
            }

            /// The keep-mask application is a monolithic pass over every key column; poll for a hard
            /// cancellation between columns and within each column's materialization so a KILL arriving
            /// during the copy is honored promptly rather than only after the whole chunk is materialized.
            /// Each column is filtered in chunks of `filter_chunk_rows` rows (producing the same result as
            /// a single `column->filter(keep, num_kept)` call) so a cancellation check can also run between
            /// chunks, including for the common single-column `IN (subquery)` set build. A soft timeout that
            /// fires *during* the pass truncates every column at the same source row (`truncated_at`) so the
            /// key columns stay aligned; a soft timeout already latched upstream must not truncate here,
            /// otherwise the already-committed prefix would be dropped.
            constexpr size_t filter_chunk_rows = 1u << 13; /// 8192
            size_t truncated_at = num_rows;
            for (auto & column : columns)
            {
                FailPointInjection::pauseFailPoint("distinct_transform_filter_pause");
                if (isCancelled() && !isCancelledBySoftTimeout())
                {
                    if (timeoutShouldThrow())
                        process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                    chunk.clear();
                    stopReading();
                    return;
                }

                if (num_rows <= filter_chunk_rows)
                {
                    column = column->filter(keep, num_kept);
                    continue;
                }

                auto filtered = column->cloneEmpty();
                size_t offset = 0;
                const size_t limit = truncated_at;
                while (offset < limit)
                {
                    if (isCancelled() && !isCancelledBySoftTimeout())
                    {
                        if (timeoutShouldThrow())
                            process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
                        chunk.clear();
                        stopReading();
                        return;
                    }
                    if (offset > 0 && !filter_pre_latched && isSoftTimeout())
                    {
                        truncated_at = offset; /// break-mode: stop materializing, emit the processed prefix
                        break;
                    }
                    const size_t len = std::min(filter_chunk_rows, limit - offset);
                    IColumn::Filter sub_keep(keep.begin() + offset, keep.begin() + offset + len);
                    const auto sub_kept = countBytesInFilter(sub_keep);
                    auto sub = column->cut(offset, len)->filter(sub_keep, sub_kept);
                    filtered->insertRangeFrom(*sub, 0, sub->size());
                    offset += len;
                }
                column = std::move(filtered);
            }

            /// A soft timeout observed during the materialization may have truncated only the columns
            /// processed after it, leaving earlier columns at their full length. Align every key column to
            /// the same committed source prefix so the key columns stay the same length for the set fill
            /// below (otherwise `buildFilter` would read past the end of the shorter column).
            if (truncated_at < num_rows)
            {
                IColumn::Filter prefix_keep(keep.begin(), keep.begin() + truncated_at);
                const size_t prefix_kept = countBytesInFilter(prefix_keep);
                for (auto & column : columns)
                    column = column->cut(0, prefix_kept);
                num_rows = prefix_kept;
            }
            else
                num_rows = columns[0]->size();

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
    size_t processed_prefix = 0;

    if (lc_optimization_controller.isEnabled() && key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            auto [mask, new_indices_count, processed_rows] = buildLowCardinalityMask(*lc, num_rows);
            lc_optimization_controller.update(num_rows, new_indices_count);

            if (isCancelled() && !isCancelledBySoftTimeout())
            {
                chunk.clear();
                stopReading();
                return;
            }

            lc_mask.emplace(std::move(mask));

            /// Empty mask -> no candidate rows in this chunk, emit nothing. The chunk is fully
            /// duplicate, which is the strongest evidence in favor of keeping the deduplication, so
            /// the abandon accounting must see it.
            if (lc_mask->empty())
            {
                if (time_limit_exceeded)
                    stopReading();
                if (abandon_controller)
                    abandon_controller->update(num_rows, 0, data->getTotalByteCount());
                return;
            }

            /// On a soft timeout `buildLowCardinalityMask` returned a partial mask covering rows
            /// [0, processed_rows); those rows are committed work and must be hashed even though the
            /// time limit already fired, otherwise the processed prefix would be dropped.
            processed_prefix = time_limit_exceeded ? processed_rows : 0;
        }
    }

    /// The `LowCardinality` fast path above sets `processed_prefix`; for the plain `Nullable`
    /// (non-LowCardinality) `IN (subquery)` set build that path is skipped, so set it here from the
    /// committed prefix (`num_rows`). Otherwise a soft timeout discards the whole chunk in `buildFilter`
    /// (it would see the timeout at row 0 with `processed_prefix == 0` and zero the entire filter),
    /// dropping the null-marking prepass's already-committed prefix instead of preserving it.
    if (time_limit_exceeded && processed_prefix == 0)
        processed_prefix = num_rows;

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
            buildFilter(*data->NAME, column_ptrs, filter, num_rows, *data, lc_mask ? &*lc_mask : nullptr, processed_prefix); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    if (isCancelled() && !isCancelledBySoftTimeout())
    {
        if (timeoutShouldThrow())
            process_list_element->checkTimeLimit(); // throws TIMEOUT_EXCEEDED
        chunk.clear();
        stopReading();
        return;
    }

    /// Soft timeout (break mode): emit the already-processed prefix and stop reading, as if the
    /// source ran out. Hard cancellation above keeps dropping the whole chunk.
    if (time_limit_exceeded)
        stopReading();

    const auto new_set_size = data->getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    if (abandon_controller)
    {
        abandon_controller->update(num_rows, num_selected, data->getTotalByteCount());
        if (abandon_controller->isAbandoned())
        {
            data.reset();
            lc_dict_states.clear();
        }
    }

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
