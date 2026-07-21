#include <algorithm>
#include <optional>

#include <base/defines.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Processors/Transforms/BandJoinTransform.h>
#include <Processors/Transforms/JoinKeyEncoding.h>
#include <Common/NaNUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

const char * toString(BandJoinKind kind)
{
    switch (kind)
    {
        case BandJoinKind::Inner: return "INNER";
        case BandJoinKind::Left: return "LEFT";
        case BandJoinKind::LeftSemi: return "LEFT SEMI";
        case BandJoinKind::LeftAnti: return "LEFT ANTI";
    }
}

/// Byte mask of the rows valid as join keys (empty when every row is valid): rows with NULL
/// or NaN in either key column are excluded. A NULL fails every inequality; the index orders
/// by the `compareAt` total order, where NaN is an ordinary greatest value, but the predicates
/// the join implements follow IEEE semantics, under which every comparison involving NaN is
/// false. The keys may alias each other (the `BETWEEN` shape); the second may be nullptr.
static IColumn::Filter collectValidKeyRows(const IColumn * first_key, const IColumn * second_key, size_t rows)
{
    IColumn::Filter valid;
    auto exclude_rows = [&](auto && is_excluded)
    {
        if (valid.empty())
            valid.resize_fill(rows, 1);
        for (size_t row = 0; row < rows; ++row)
            valid[row] &= !is_excluded(row);
    };
    if (second_key == first_key)
        second_key = nullptr;
    for (const IColumn * key : {first_key, second_key})
    {
        if (!key)
            continue;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(key))
        {
            const auto & null_map = nullable->getNullMapData();
            exclude_rows([&](size_t row) { return null_map[row] != 0; });
            key = &nullable->getNestedColumn();
        }
        auto exclude_nan_rows = [&](const auto & typed_key)
        {
            const auto & data = typed_key.getData();
            exclude_rows([&](size_t row) { return isNaN(data[row]); });
        };
        if (const auto * float64_key = checkAndGetColumn<ColumnFloat64>(key))
            exclude_nan_rows(*float64_key);
        else if (const auto * float32_key = checkAndGetColumn<ColumnFloat32>(key))
            exclude_nan_rows(*float32_key);
        else if (const auto * bfloat16_key = checkAndGetColumn<ColumnBFloat16>(key))
            exclude_nan_rows(*bfloat16_key);
    }

    if (!valid.empty() && countBytesInFilter(valid) == rows)
        valid.clear();
    return valid;
}

static void validateBandJoinConditions(const BandJoinConditions & conditions)
{
    if (conditions[0].op != JoinConditionOperator::Greater && conditions[0].op != JoinConditionOperator::GreaterOrEquals)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected lower-bound operator in band join condition: {}",
            toString(conditions[0].op));
    if (conditions[1].op != JoinConditionOperator::Less && conditions[1].op != JoinConditionOperator::LessOrEquals)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected upper-bound operator in band join condition: {}",
            toString(conditions[1].op));
}

BandJoinBuildTransform::BandJoinBuildTransform(
    SharedHeader input_header_,
    const BandJoinConditions & conditions_,
    const SizeLimits & size_limits_,
    BandJoinSharedStatePtr state_)
    : JoinBuildSideTransform(input_header_)
    , input_header(std::move(input_header_))
    , conditions(conditions_)
    , size_limits(size_limits_)
    , state(std::move(state_))
    , index(std::make_shared<BandJoinIndex>())
{
    validateBandJoinConditions(conditions);
    for (const auto & condition : conditions)
        if (condition.interval_key_position >= input_header->columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join interval key position {} is out of range for an input with {} columns",
                condition.interval_key_position, input_header->columns());

    /// The encoded-vs-generic decision is per bound and type-driven, so the build and every
    /// probe (whose key of the same bound has the same type) always agree.
    auto is_encodable = [&](size_t position)
    {
        auto sample = input_header->getByPosition(position).type->createColumn();
        return isJoinKeyColumnEncodable(*sample->convertToFullColumnIfLowCardinality());
    };
    index->lower_encoded = is_encodable(conditions[0].interval_key_position);
    index->upper_encoded = is_encodable(conditions[1].interval_key_position);
}

bool BandJoinBuildTransform::consumeBuildChunk(Chunk chunk)
{
    convertToFullIfConst(chunk);
    /// Expands lazily replicated wrappers besides removing sparse columns: the index stores
    /// the columns as delivered and gathers rows by position, which needs plain full columns.
    removeSpecialColumnRepresentations(chunk);

    /// The size limits apply to the accumulated interval side. With `join_overflow_mode =
    /// 'break'` keep what is already accumulated and drop the rest of the input.
    if (!size_limits.check(
            total_rows + chunk.getNumRows(), total_bytes + chunk.allocatedBytes(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return false;

    total_rows += chunk.getNumRows();
    total_bytes += chunk.allocatedBytes();

    if (chunk.getNumRows() > 0)
        appendBlock(chunk.detachColumns());
    return true;
}

void BandJoinBuildTransform::appendBlock(Columns columns)
{
    const size_t rows = columns.front()->size();

    auto prepare_keys = [&](const Columns & from) -> std::pair<ColumnPtr, ColumnPtr>
    {
        ColumnPtr lo = from[conditions[0].interval_key_position]->convertToFullColumnIfLowCardinality();
        ColumnPtr hi = conditions[1].interval_key_position == conditions[0].interval_key_position
            ? lo
            : from[conditions[1].interval_key_position]->convertToFullColumnIfLowCardinality();
        return {std::move(lo), std::move(hi)};
    };
    auto [lo_key, hi_key] = prepare_keys(columns);

#ifndef NDEBUG
    /// The operator trusts the plan-level pre-sort (ascending by `lo`, NULLS LAST, which is
    /// nan_direction_hint = 1); check the order within the block and across blocks.
    for (size_t row = 1; row < rows; ++row)
        chassert(lo_key->compareAt(row - 1, row, *lo_key, /* nan_direction_hint */ 1) <= 0);
    if (last_lo_key)
        chassert(last_lo_key->compareAt(last_lo_key->size() - 1, 0, *lo_key, /* nan_direction_hint */ 1) <= 0);
    last_lo_key = lo_key;
#endif

    /// Filtering the invalid rows out (instead of masking them at probe time) keeps the
    /// encoded arrays dense and the binary searches mask-free; dropping NULL-`lo` rows also
    /// preserves the sorted order of the encoded domain, which their placeholder encodings
    /// would break. Blocks without invalid rows are kept as delivered, with no copy.
    IColumn::Filter valid = collectValidKeyRows(lo_key.get(), hi_key.get(), rows);
    size_t num_valid = rows;
    if (!valid.empty())
    {
        num_valid = countBytesInFilter(valid);
        if (num_valid == 0)
            return;
        for (auto & column : columns)
            column = column->filter(valid, num_valid);
        std::tie(lo_key, hi_key) = prepare_keys(columns);
    }

    auto & block = index->blocks.emplace_back();
    block.num_rows = num_valid;
    size_t block_bytes = 0;
    for (const auto & column : columns)
        block_bytes += column->byteSize();
    block.avg_row_bytes = std::max<size_t>(1, block_bytes / num_valid);
    block.columns = std::move(columns);
    block.lo_key = std::move(lo_key);
    block.hi_key = std::move(hi_key);

    if (index->lower_encoded)
    {
        [[maybe_unused]] bool encoded = tryAppendEncodedKeys(*block.lo_key, 0, block.encoded_lo);
        chassert(encoded);
        index->dir_first_lo.push_back(block.encoded_lo.front());
    }

    if (index->upper_encoded)
    {
        [[maybe_unused]] bool encoded = tryAppendEncodedKeys(*block.hi_key, 0, block.encoded_hi);
        chassert(encoded);
        block.prefix_max_hi.resize(num_valid);
        UInt64 running_max = block.encoded_hi[0];
        for (size_t row = 0; row < num_valid; ++row)
        {
            running_max = std::max(running_max, block.encoded_hi[row]);
            block.prefix_max_hi[row] = running_max;
        }
        index->dir_block_max_hi.push_back(running_max);
        index->dir_prefix_max_hi.push_back(
            index->dir_prefix_max_hi.empty() ? running_max : std::max(index->dir_prefix_max_hi.back(), running_max));
    }
    else
    {
        block.prefix_max_hi_row.resize(num_valid);
        UInt32 arg_max = 0;
        for (size_t row = 0; row < num_valid; ++row)
        {
            if (block.hi_key->compareAt(row, arg_max, *block.hi_key, /* nan_direction_hint */ 1) > 0)
                arg_max = static_cast<UInt32>(row);
            block.prefix_max_hi_row[row] = arg_max;
        }
        index->dir_block_max_hi_row.push_back(arg_max);

        BandJoinIndex::RowRef running_ref{static_cast<UInt32>(index->blocks.size() - 1), arg_max};
        if (!index->dir_prefix_max_hi_ref.empty())
        {
            const auto & previous_ref = index->dir_prefix_max_hi_ref.back();
            const auto & previous_hi = *index->blocks[previous_ref.block].hi_key;
            if (previous_hi.compareAt(previous_ref.row, arg_max, *block.hi_key, /* nan_direction_hint */ 1) >= 0)
                running_ref = previous_ref;
        }
        index->dir_prefix_max_hi_ref.push_back(running_ref);
    }

    index->total_rows += num_valid;
}

void BandJoinBuildTransform::finishBuild()
{
    state->index = std::move(index);
    state.reset();
}

BandJoinProbeTransform::BandJoinProbeTransform(
    SharedHeader input_header_,
    SharedHeader output_header_,
    const BandJoinConditions & conditions_,
    BandJoinKind kind_,
    std::optional<JoinResidualCondition> residual_,
    BandJoinSharedStatePtr state_,
    size_t max_joined_block_rows_,
    size_t max_joined_block_bytes_)
    : JoinProbeSideTransform(std::move(input_header_), std::move(output_header_))
    , conditions(conditions_)
    , kind(kind_)
    , state(std::move(state_))
    , max_joined_block_rows(max_joined_block_rows_)
    , max_joined_block_bytes(max_joined_block_bytes_)
{
    validateBandJoinConditions(conditions);
    const auto & input_header = getInputs().front().getHeader();
    const auto & output_header = getOutputs().front().getHeader();
    for (const auto & condition : conditions)
        if (condition.point_key_position >= input_header.columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join point key position {} is out of range for an input with {} columns",
                condition.point_key_position, input_header.columns());
    if (output_header.columns() <= input_header.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join output header must be the concatenation of the input headers, got {} and {}",
            input_header.dumpStructure(), output_header.dumpStructure());

    if (residual_)
    {
        /// The interval-side header is the output's tail (the output is the concatenation,
        /// checked above); the residual's side 1 positions refer to it.
        Block interval_header;
        for (size_t i = input_header.columns(); i < output_header.columns(); ++i)
            interval_header.insert(output_header.getByPosition(i));
        residual.emplace(std::move(*residual_), input_header, interval_header);
    }

    lower_strict = conditions[0].op == JoinConditionOperator::Greater;
    upper_strict = conditions[1].op == JoinConditionOperator::Less;
}

void BandJoinProbeTransform::onBarrierReleased()
{
    index = state->index;
    state.reset();
    if (!index)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join build side finished without publishing the index");
}

void BandJoinProbeTransform::consumeProbeChunk(Chunk chunk)
{
    convertToFullIfConst(chunk);
    /// Expands lazily replicated wrappers besides removing sparse columns: the point rows are
    /// replicated per match and their keys encoded by position, which needs plain full columns.
    removeSpecialColumnRepresentations(chunk);

    num_point_rows = chunk.getNumRows();
    point_columns = chunk.detachColumns();
    current_row = 0;
    in_walk = false;

    /// An empty index cannot match anything: unless the kind emits unmatched rows, skip the
    /// chunk outright.
    if (num_point_rows == 0 || (index->blocks.empty() && !emitsUnmatchedRows()))
    {
        resetChunkState();
        return;
    }

    size_t chunk_bytes = 0;
    for (const auto & column : point_columns)
        chunk_bytes += column->byteSize();
    point_avg_row_bytes = std::max<size_t>(1, chunk_bytes / num_point_rows);

    point_keys[0] = point_columns[conditions[0].point_key_position]->convertToFullColumnIfLowCardinality();
    point_keys[1] = conditions[1].point_key_position == conditions[0].point_key_position
        ? point_keys[0]
        : point_columns[conditions[1].point_key_position]->convertToFullColumnIfLowCardinality();

    point_valid = collectValidKeyRows(point_keys[0].get(), point_keys[1].get(), num_point_rows);

    /// Encode the point keys once per chunk, column-wise, into the reused scratch arrays;
    /// each bound's key has the type the build decided the encoding by, so this cannot fail.
    for (size_t bound = 0; bound < 2; ++bound)
    {
        auto & encoded = encoded_point_keys[bound];
        encoded.clear();
        if (bound == 0 ? index->lower_encoded : index->upper_encoded)
        {
            [[maybe_unused]] bool ok = tryAppendEncodedKeys(*point_keys[bound], 0, encoded);
            chassert(ok);
        }
    }

    if (index->lower_encoded)
        computeWalkStarts();
}

void BandJoinProbeTransform::resetChunkState()
{
    /// Every completed row flushes its pending residual candidates in finishRow.
    chassert(pending_count == 0);
    point_columns.clear();
    num_point_rows = 0;
    point_keys = {};
    point_valid.clear();
    current_row = 0;
    in_walk = false;
}

bool BandJoinProbeTransform::lowerAdmits(const BandJoinIndex::Block & block, size_t row, size_t point_row) const
{
    int cmp = block.lo_key->compareAt(row, point_row, *point_keys[0], /* nan_direction_hint */ 1);
    return lower_strict ? cmp < 0 : cmp <= 0;
}

bool BandJoinProbeTransform::upperAdmits(const BandJoinIndex::Block & block, size_t row, size_t point_row) const
{
    int cmp = block.hi_key->compareAt(row, point_row, *point_keys[1], /* nan_direction_hint */ 1);
    return upper_strict ? cmp > 0 : cmp >= 0;
}

bool BandJoinProbeTransform::lowerAdmitsEncoded(UInt64 encoded_lo, size_t point_row) const
{
    UInt64 encoded_point = encoded_point_keys[0][point_row];
    return lower_strict ? encoded_lo < encoded_point : encoded_lo <= encoded_point;
}

bool BandJoinProbeTransform::upperAdmitsEncoded(UInt64 encoded_hi, size_t point_row) const
{
    UInt64 encoded_point = encoded_point_keys[1][point_row];
    return upper_strict ? encoded_hi > encoded_point : encoded_hi >= encoded_point;
}

/// Branchless lower bound (first element >= key): successive probe keys are effectively
/// random, so every classic binary-search step is a dependent cache miss; prefetching both
/// possible next midpoints overlaps them, and the conditional move keeps the mispredicted
/// branch from discarding the prefetched line.
static size_t lowerBoundPrefetched(const PaddedPODArray<UInt64> & arr, UInt64 key)
{
    const UInt64 * base = arr.data();
    size_t len = arr.size();
    while (len > 1)
    {
        const size_t half = len / 2;
        __builtin_prefetch(base + half / 2);
        __builtin_prefetch(base + half + (len - half) / 2);
        base += (base[half - 1] < key) ? half : 0;
        len -= half;
    }
    return (base - arr.data()) + (len == 1 && *base < key);
}

/// First position whose `lo` no longer admits the encoded point under the lower bound's
/// strictness: first `lo >= point` when strict, first `lo > point` when loose.
static size_t firstNonAdmittingLo(const PaddedPODArray<UInt64> & encoded_lo, UInt64 encoded_point, bool strict)
{
    if (!strict)
    {
        if (encoded_point == std::numeric_limits<UInt64>::max())
            return encoded_lo.size();
        ++encoded_point;
    }
    return lowerBoundPrefetched(encoded_lo, encoded_point);
}

void BandJoinProbeTransform::computeWalkStarts()
{
    walk_start_block.resize(num_point_rows);
    walk_start_row.resize(num_point_rows);

    const auto & keys = encoded_point_keys[0];
    constexpr UInt64 max_key = std::numeric_limits<UInt64>::max();
    constexpr size_t batch_size = 16;
    /// Per in-flight search: the point row, the effective lower-bound key, and the shrinking
    /// window [base, base + len) within the block's `encoded_lo`.
    size_t batch_rows[batch_size];
    UInt64 batch_keys[batch_size];
    const UInt64 * batch_base[batch_size];
    size_t batch_len[batch_size];

    /// Run the enqueued level-2 searches in lockstep: each round advances every search one
    /// step, so their dependent cache misses overlap instead of serializing row by row.
    auto flush_batch = [&](size_t count)
    {
        bool in_progress = true;
        while (in_progress)
        {
            in_progress = false;
            for (size_t i = 0; i < count; ++i)
            {
                const size_t len = batch_len[i];
                if (len <= 1)
                    continue;
                in_progress = true;
                const size_t half = len / 2;
                const UInt64 * base = batch_base[i];
                base += (base[half - 1] < batch_keys[i]) ? half : 0;
                batch_base[i] = base;
                batch_len[i] = len - half;
                __builtin_prefetch(base + (len - half) / 2);
            }
        }
        for (size_t i = 0; i < count; ++i)
        {
            const size_t row = batch_rows[i];
            const auto & encoded_lo = index->blocks[walk_start_block[row]].encoded_lo;
            const size_t first_non_admitting = (batch_base[i] - encoded_lo.data())
                + (batch_len[i] == 1 && *batch_base[i] < batch_keys[i]);
            chassert(first_non_admitting != 0);
            walk_start_row[row] = static_cast<UInt32>(first_non_admitting - 1);
        }
    };

    size_t enqueued = 0;
    for (size_t row = 0; row < num_point_rows; ++row)
    {
        walk_start_block[row] = no_walk_start;
        if (!point_valid.empty() && !point_valid[row])
            continue;

        const size_t first_non_admitting = firstNonAdmittingLo(index->dir_first_lo, keys[row], lower_strict);
        if (first_non_admitting == 0)
            continue;
        const size_t block_index = first_non_admitting - 1;
        const auto & block = index->blocks[block_index];
        walk_start_block[row] = static_cast<UInt32>(block_index);

        UInt64 key = keys[row];
        if (!lower_strict)
        {
            /// A loose bound on the greatest encoded key admits the whole block.
            if (key == max_key)
            {
                walk_start_row[row] = static_cast<UInt32>(block.num_rows - 1);
                continue;
            }
            ++key;
        }
        batch_rows[enqueued] = row;
        batch_keys[enqueued] = key;
        batch_base[enqueued] = block.encoded_lo.data();
        batch_len[enqueued] = block.encoded_lo.size();
        if (++enqueued == batch_size)
        {
            flush_batch(enqueued);
            enqueued = 0;
        }
    }
    flush_batch(enqueued);
}

bool BandJoinProbeTransform::findWalkStart(size_t point_row)
{
    const auto & blocks = index->blocks;
    produce_work += 2;

    size_t block_index;
    size_t position;
    if (index->lower_encoded)
    {
        if (walk_start_block[point_row] == no_walk_start)
            return false;
        block_index = walk_start_block[point_row];
        position = walk_start_row[point_row];
    }
    else
    {
        /// Level 1: the last block whose first `lo` admits the point; the pre-sort makes
        /// every row of the earlier blocks admissible under the lower bound, and no row of
        /// the later blocks can be.
        size_t low = 0;
        size_t high = blocks.size();
        while (low < high)
        {
            size_t mid = low + (high - low) / 2;
            if (lowerAdmits(blocks[mid], 0, point_row))
                low = mid + 1;
            else
                high = mid;
        }
        if (low == 0)
            return false;
        block_index = low - 1;

        /// Level 2: the last admissible position within the block; row 0 admits by the
        /// choice of the block, so there is one.
        const auto & block = blocks[block_index];
        low = 0;
        high = block.num_rows;
        while (low < high)
        {
            size_t mid = low + (high - low) / 2;
            if (lowerAdmits(block, mid, point_row))
                low = mid + 1;
            else
                high = mid;
        }
        chassert(low > 0);
        position = low - 1;
    }

    const auto & block = blocks[block_index];

    /// The walk's first touches, issued while the caller is still bookkeeping.
    if (index->upper_encoded)
    {
        __builtin_prefetch(block.prefix_max_hi.data() + position);
        __builtin_prefetch(block.encoded_hi.data() + position);
    }

    walk_block = block_index;
    walk_row = static_cast<ssize_t>(position);
    return true;
}

bool BandJoinProbeTransform::descendDirectory(size_t point_row, bool & out_of_budget)
{
    out_of_budget = false;
    while (walk_block > 0)
    {
        if (produce_work >= produce_work_budget)
        {
            out_of_budget = true;
            return false;
        }
        ++produce_work;

        const size_t candidate = walk_block - 1;
        /// Stop the walk entirely when the across-blocks running max no longer admits the
        /// point: a block-local maximum proves nothing about earlier blocks, only this can.
        bool prefix_admits = index->upper_encoded
            ? upperAdmitsEncoded(index->dir_prefix_max_hi[candidate], point_row)
            : [&]
              {
                  const auto & ref = index->dir_prefix_max_hi_ref[candidate];
                  return upperAdmits(index->blocks[ref.block], ref.row, point_row);
              }();
        if (!prefix_admits)
            return false;

        walk_block = candidate;
        bool block_admits = index->upper_encoded
            ? upperAdmitsEncoded(index->dir_block_max_hi[candidate], point_row)
            : upperAdmits(index->blocks[candidate], index->dir_block_max_hi_row[candidate], point_row);
        if (block_admits)
        {
            walk_row = static_cast<ssize_t>(index->blocks[candidate].num_rows) - 1;
            return true;
        }
        /// The block's own max `hi` cannot admit the point: skip it in O(1).
    }
    return false;
}

bool BandJoinProbeTransform::continueWalk(size_t point_row)
{
    while (true)
    {
        if (walk_row < 0)
        {
            bool out_of_budget = false;
            if (!descendDirectory(point_row, out_of_budget))
                return !out_of_budget;
        }

        const auto & block = index->blocks[walk_block];
        /// Scan the block backwards while the within-block prefix-max still admits the point
        /// (rows below the stop can no longer hold a match in this block), registering the
        /// rows whose own `hi` admits it; all rows at or below the walk start satisfy the
        /// lower bound by the sorted order.
        while (walk_row >= 0)
        {
            if (outputFull() || produce_work >= produce_work_budget)
                return false;
            ++produce_work;

            const size_t row = static_cast<size_t>(walk_row);
            bool prefix_admits = index->upper_encoded
                ? upperAdmitsEncoded(block.prefix_max_hi[row], point_row)
                : upperAdmits(block, block.prefix_max_hi_row[row], point_row);
            if (!prefix_admits)
            {
                walk_row = -1;
                break;
            }
            bool row_admits = index->upper_encoded
                ? upperAdmitsEncoded(block.encoded_hi[row], point_row)
                : upperAdmits(block, row, point_row);
            --walk_row;
            if (row_admits && !onMatch(point_row, walk_block, row))
                return true;
        }
    }
}

bool BandJoinProbeTransform::onMatch(size_t point_row, size_t block_index, size_t row)
{
    if (residual)
    {
        /// A candidate becomes a match only through the residual: buffer it (candidates must
        /// already satisfy both bounds, re-verify like an emitted pair) and flush per bounded
        /// mini-batch, or earlier when the pending rows would reach the output row cap.
        chassert(checkEmittedPair(point_row, index->blocks[block_index], row));
        if (pending_segments.empty() || pending_segments.back().first != block_index)
            pending_segments.emplace_back(block_index, ColumnUInt64::create());
        pending_segments.back().second->getData().push_back(row);
        ++pending_count;
        if (pending_count >= residual_batch_size
            || (max_joined_block_rows != 0 && out_point_rows.size() + pending_count >= max_joined_block_rows))
            return flushPendingCandidates(point_row);
        return true;
    }

    current_row_matched = true;
    switch (kind)
    {
        case BandJoinKind::Inner:
        case BandJoinKind::Left:
            emitMatch(point_row, block_index, row);
            return true;
        case BandJoinKind::LeftSemi:
            emitMatch(point_row, block_index, row);
            return false;
        case BandJoinKind::LeftAnti:
            /// The match decides the row (not emitted); re-verify it like an emitted pair.
            chassert(checkEmittedPair(point_row, index->blocks[block_index], row));
            return false;
    }
}

bool BandJoinProbeTransform::flushPendingCandidates(size_t point_row)
{
    if (pending_count == 0)
        return true;
    produce_work += pending_count;

    /// Gather the residual's input columns for the candidates: the point side is the single
    /// point row replicated, the interval side is gathered per pending segment.
    auto point_indexes = ColumnUInt64::create(pending_count, point_row);
    const auto & output_header = getOutputs().front().getHeader();
    const size_t num_point_columns = point_columns.size();

    Columns expression_columns;
    expression_columns.reserve(residual->sources().size());
    for (const auto & source : residual->sources())
    {
        if (source.side == 0)
        {
            expression_columns.push_back(point_columns[source.position]->index(*point_indexes, 0));
            continue;
        }
        auto gather_segment = [&](size_t block_index, const ColumnUInt64 & rows)
        {
            return index->blocks[block_index].columns[source.position]->index(rows, 0);
        };
        if (pending_segments.size() == 1)
        {
            expression_columns.push_back(gather_segment(pending_segments.front().first, *pending_segments.front().second));
        }
        else
        {
            auto result = output_header.getByPosition(num_point_columns + source.position).type->createColumn();
            result->reserve(pending_count);
            for (const auto & [block_index, rows] : pending_segments)
            {
                auto gathered = gather_segment(block_index, *rows);
                result->insertRangeFrom(*gathered, 0, gathered->size());
            }
            expression_columns.push_back(std::move(result));
        }
    }

    IColumn::Filter mask = residual->evaluateMask(std::move(expression_columns), pending_count);

    bool keep_walking = true;
    size_t candidate = 0;
    for (const auto & [block_index, rows] : pending_segments)
    {
        for (UInt64 row : rows->getData())
        {
            if (!mask[candidate++])
                continue;
            current_row_matched = true;
            if (kind != BandJoinKind::LeftAnti)
                emitMatch(point_row, block_index, row);
            if (kind == BandJoinKind::LeftSemi || kind == BandJoinKind::LeftAnti)
            {
                /// The first passing candidate decides the row; the rest are discarded.
                keep_walking = false;
                break;
            }
        }
        if (!keep_walking)
            break;
    }

    pending_segments.clear();
    pending_count = 0;
    return keep_walking;
}

void BandJoinProbeTransform::finishRow(size_t point_row)
{
    if (residual)
        flushPendingCandidates(point_row);
    if (!current_row_matched && emitsUnmatchedRows())
        emitUnmatched(point_row);
}

bool BandJoinProbeTransform::checkEmittedPair(size_t point_row, const BandJoinIndex::Block & block, size_t row) const
{
    int lower_cmp = point_keys[0]->compareAt(point_row, row, *block.lo_key, /* nan_direction_hint */ 1);
    bool lower_holds = lower_strict ? lower_cmp > 0 : lower_cmp >= 0;
    int upper_cmp = point_keys[1]->compareAt(point_row, row, *block.hi_key, /* nan_direction_hint */ 1);
    bool upper_holds = upper_strict ? upper_cmp < 0 : upper_cmp <= 0;
    return lower_holds && upper_holds;
}

void BandJoinProbeTransform::emitMatch(size_t point_row, size_t block_index, size_t row)
{
    /// Every emitted pair must satisfy both bounds, re-verify by direct evaluation.
    chassert(checkEmittedPair(point_row, index->blocks[block_index], row));

    out_point_rows.push_back(point_row);
    out_blocks.push_back(block_index);
    out_rows.push_back(row);
    out_bytes_estimate += point_avg_row_bytes + index->blocks[block_index].avg_row_bytes;
}

void BandJoinProbeTransform::emitUnmatched(size_t point_row)
{
    out_point_rows.push_back(point_row);
    out_blocks.push_back(padded_segment);
    /// Only the padded rows' count matters; the row values are never read.
    out_rows.push_back(0);
    out_bytes_estimate += point_avg_row_bytes;
}

bool BandJoinProbeTransform::outputFull() const
{
    /// Hash-join semantics: no row cap means no splitting at all; the byte cap only shrinks
    /// a non-zero row cap.
    if (max_joined_block_rows == 0)
        return false;
    if (out_point_rows.size() >= max_joined_block_rows)
        return true;
    return max_joined_block_bytes != 0 && out_bytes_estimate >= max_joined_block_bytes;
}

Chunk BandJoinProbeTransform::buildOutputChunk()
{
    const size_t num_rows = out_point_rows.size();
    chassert(out_blocks.size() == num_rows && out_rows.size() == num_rows);

    Chunk chunk;
    auto point_indexes = ColumnUInt64::create();
    point_indexes->getData().swap(out_point_rows);
    for (const auto & column : point_columns)
        chunk.addColumn(column->index(*point_indexes, 0));

    /// Group the rows by index block so each block is gathered in one batched call: a stable
    /// order by block (the sentinel sorts last), one rows column per run; when the emission
    /// order interleaves blocks, a final gather by the inverse positions restores it.
    struct Run
    {
        size_t block = 0;
        ColumnUInt64::MutablePtr rows;
    };
    std::vector<Run> runs;
    ColumnUInt64::MutablePtr inverse_positions;
    const bool emission_ordered = std::is_sorted(out_blocks.begin(), out_blocks.end());
    if (emission_ordered)
    {
        size_t run_start = 0;
        for (size_t i = 1; i <= num_rows; ++i)
        {
            if (i == num_rows || out_blocks[i] != out_blocks[run_start])
            {
                auto rows = ColumnUInt64::create();
                rows->getData().insert(out_rows.begin() + run_start, out_rows.begin() + i);
                runs.push_back({out_blocks[run_start], std::move(rows)});
                run_start = i;
            }
        }
    }
    else
    {
        /// Counting sort by block (stable, so each block's rows stay in emission order): the
        /// block count is small next to the row count, where a comparison sort would cost.
        const size_t num_buckets = index->blocks.size() + 1;    /// + the padded sentinel, last
        auto bucket_of = [&](UInt64 block) { return block == padded_segment ? num_buckets - 1 : block; };

        PaddedPODArray<UInt64> bucket_begin;
        bucket_begin.resize_fill(num_buckets + 1, 0);
        for (size_t i = 0; i < num_rows; ++i)
            ++bucket_begin[bucket_of(out_blocks[i]) + 1];
        for (size_t bucket = 1; bucket <= num_buckets; ++bucket)
            bucket_begin[bucket] += bucket_begin[bucket - 1];

        PaddedPODArray<UInt64> grouped_rows(num_rows);
        PaddedPODArray<UInt64> cursor;
        cursor.assign(bucket_begin.begin(), bucket_begin.end());
        inverse_positions = ColumnUInt64::create(num_rows);
        auto & inverse = inverse_positions->getData();
        for (size_t i = 0; i < num_rows; ++i)
        {
            const size_t grouped = cursor[bucket_of(out_blocks[i])]++;
            grouped_rows[grouped] = out_rows[i];
            inverse[i] = grouped;
        }

        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
        {
            const size_t begin = bucket_begin[bucket];
            const size_t end = bucket_begin[bucket + 1];
            if (begin == end)
                continue;
            auto rows = ColumnUInt64::create();
            rows->getData().insert(grouped_rows.begin() + begin, grouped_rows.begin() + end);
            runs.push_back({bucket == num_buckets - 1 ? padded_segment : bucket, std::move(rows)});
        }
    }

    const auto & output_header = getOutputs().front().getHeader();
    const size_t num_point_columns = point_columns.size();
    for (size_t i = num_point_columns; i < output_header.columns(); ++i)
    {
        const size_t interval_position = i - num_point_columns;
        /// The padded run gets the column-type defaults: NULL when the type is Nullable
        /// (with `join_use_nulls` the planner made the interval-side columns Nullable in the
        /// pre-join actions).
        auto gather_run = [&](const Run & run) -> ColumnPtr
        {
            if (run.block == padded_segment)
            {
                auto padded = output_header.getByPosition(i).type->createColumn();
                padded->insertManyDefaults(run.rows->size());
                return padded;
            }
            return index->blocks[run.block].columns[interval_position]->index(*run.rows, 0);
        };
        ColumnPtr grouped;
        if (runs.size() == 1)
        {
            grouped = gather_run(runs.front());
        }
        else
        {
            auto concatenated = output_header.getByPosition(i).type->createColumn();
            concatenated->reserve(num_rows);
            for (const auto & run : runs)
            {
                auto gathered = gather_run(run);
                concatenated->insertRangeFrom(*gathered, 0, gathered->size());
            }
            grouped = std::move(concatenated);
        }
        chunk.addColumn(emission_ordered ? std::move(grouped) : grouped->index(*inverse_positions, 0));
    }

    out_blocks.clear();
    out_rows.clear();
    out_bytes_estimate = 0;
    return chunk;
}

std::optional<Chunk> BandJoinProbeTransform::produceChunk()
{
    if (num_point_rows == 0)
        return std::nullopt;

    produce_work = 0;
    while (current_row < num_point_rows)
    {
        if (outputFull())
            return buildOutputChunk();
        if (produce_work >= produce_work_budget)
            /// An empty chunk is a pure yield: control returns to the executor, which
            /// observes cancellation, and the walk resumes on the next call.
            return out_point_rows.empty() ? Chunk() : buildOutputChunk();

        if (!in_walk)
        {
            ++produce_work;
            current_row_matched = false;
            /// Rows with NULL/NaN keys and rows with no admissible walk start match nothing:
            /// finishRow decides whether that means "skip" or "emit padded".
            bool valid = point_valid.empty() || point_valid[current_row];
            if (!valid || !findWalkStart(current_row))
            {
                finishRow(current_row);
                ++current_row;
                continue;
            }
            in_walk = true;
        }

        if (continueWalk(current_row))
        {
            in_walk = false;
            finishRow(current_row);
            ++current_row;
        }
    }

    if (!out_point_rows.empty())
    {
        Chunk chunk = buildOutputChunk();
        resetChunkState();
        return chunk;
    }
    resetChunkState();
    return std::nullopt;
}

}
