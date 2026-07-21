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
    convertToFullIfSparse(chunk);

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
    convertToFullIfSparse(chunk);

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
}

void BandJoinProbeTransform::resetChunkState()
{
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

bool BandJoinProbeTransform::findWalkStart(size_t point_row)
{
    const auto & blocks = index->blocks;
    produce_work += 2;

    /// Level 1: the last block whose first `lo` admits the point; the pre-sort makes every
    /// row of the earlier blocks admissible under the lower bound, and no row of the later
    /// blocks can be.
    size_t block_index;
    if (index->lower_encoded)
    {
        const auto & dir = index->dir_first_lo;
        const UInt64 encoded_point = encoded_point_keys[0][point_row];
        const auto * first_non_admitting = lower_strict
            ? std::lower_bound(dir.begin(), dir.end(), encoded_point)
            : std::upper_bound(dir.begin(), dir.end(), encoded_point);
        if (first_non_admitting == dir.begin())
            return false;
        block_index = (first_non_admitting - dir.begin()) - 1;
    }
    else
    {
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
    }

    /// Level 2: the last admissible position within the block; row 0 admits by the choice of
    /// the block, so there is one.
    const auto & block = blocks[block_index];
    size_t position;
    if (index->lower_encoded)
    {
        const auto & encoded_lo = block.encoded_lo;
        const UInt64 encoded_point = encoded_point_keys[0][point_row];
        const auto * first_non_admitting = lower_strict
            ? std::lower_bound(encoded_lo.begin(), encoded_lo.end(), encoded_point)
            : std::upper_bound(encoded_lo.begin(), encoded_lo.end(), encoded_point);
        chassert(first_non_admitting != encoded_lo.begin());
        position = (first_non_admitting - encoded_lo.begin()) - 1;
    }
    else
    {
        size_t low = 0;
        size_t high = block.num_rows;
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

void BandJoinProbeTransform::finishRow(size_t point_row)
{
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
    if (out_segments.empty() || out_segments.back().first != block_index)
        out_segments.emplace_back(block_index, ColumnUInt64::create());
    out_segments.back().second->getData().push_back(row);
    out_bytes_estimate += point_avg_row_bytes + index->blocks[block_index].avg_row_bytes;
}

void BandJoinProbeTransform::emitUnmatched(size_t point_row)
{
    out_point_rows.push_back(point_row);
    if (out_segments.empty() || out_segments.back().first != padded_segment)
        out_segments.emplace_back(padded_segment, ColumnUInt64::create());
    /// Only the padded segment's row count matters; the row values are never read.
    out_segments.back().second->getData().push_back(0);
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

    Chunk chunk;
    auto point_indexes = ColumnUInt64::create();
    point_indexes->getData().swap(out_point_rows);
    for (const auto & column : point_columns)
        chunk.addColumn(column->index(*point_indexes, 0));

    const auto & output_header = getOutputs().front().getHeader();
    const size_t num_point_columns = point_columns.size();
    for (size_t i = num_point_columns; i < output_header.columns(); ++i)
    {
        const size_t interval_position = i - num_point_columns;
        /// The padded segment gets the column-type defaults: NULL when the type is Nullable
        /// (with `join_use_nulls` the planner made the interval-side columns Nullable in the
        /// pre-join actions).
        auto gather_segment = [&](size_t block_index, const ColumnUInt64 & rows) -> ColumnPtr
        {
            if (block_index == padded_segment)
            {
                auto padded = output_header.getByPosition(i).type->createColumn();
                padded->insertManyDefaults(rows.size());
                return padded;
            }
            return index->blocks[block_index].columns[interval_position]->index(rows, 0);
        };
        if (out_segments.size() == 1)
        {
            const auto & [block_index, rows] = out_segments.front();
            chunk.addColumn(gather_segment(block_index, *rows));
        }
        else
        {
            auto result = output_header.getByPosition(i).type->createColumn();
            result->reserve(num_rows);
            for (const auto & [block_index, rows] : out_segments)
            {
                auto gathered = gather_segment(block_index, *rows);
                result->insertRangeFrom(*gathered, 0, gathered->size());
            }
            chunk.addColumn(std::move(result));
        }
    }

    out_segments.clear();
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
