#include <algorithm>
#include <bit>
#include <optional>

#include <base/defines.h>
#include <base/sort.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Processors/Transforms/IEJoinTransform.h>
#include <Processors/Transforms/JoinKeyEncoding.h>
#include <Common/NaNUtils.h>
#include <Common/iota.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

static bool isInequalityOperator(JoinConditionOperator op)
{
    return op == JoinConditionOperator::Less || op == JoinConditionOperator::LessOrEquals
        || op == JoinConditionOperator::Greater || op == JoinConditionOperator::GreaterOrEquals;
}

const char * toString(IEJoinKind kind)
{
    switch (kind)
    {
        case IEJoinKind::Inner: return "INNER";
        case IEJoinKind::Left: return "LEFT";
        case IEJoinKind::Right: return "RIGHT";
        case IEJoinKind::Full: return "FULL";
        case IEJoinKind::LeftSemi: return "LEFT SEMI";
        case IEJoinKind::LeftAnti: return "LEFT ANTI";
    }
}

IEJoinAlgorithm::IEJoinAlgorithm(
    IEJoinKind kind_,
    const IEJoinConditions & conditions_,
    std::optional<JoinResidualCondition> residual_,
    bool inputs_sorted_by_first_key_,
    const SharedHeaders & input_headers_,
    const SizeLimits & size_limits_,
    size_t max_block_size_)
    : input_headers(input_headers_)
    , max_block_size(std::max<size_t>(1, max_block_size_))
    , kind(kind_)
    , conditions(conditions_)
    , inputs_sorted_by_first_key(inputs_sorted_by_first_key_)
    , size_limits(size_limits_)
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoinAlgorithm requires exactly two inputs");

    for (const auto & condition : conditions)
    {
        if (!isInequalityOperator(condition.op))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected operator in IEJoin condition: {}", toString(condition.op));

        if (condition.left_key_position >= input_headers[0]->columns() || condition.right_key_position >= input_headers[1]->columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoin key positions {} and {} are out of range for inputs with {} and {} columns",
                condition.left_key_position, condition.right_key_position, input_headers[0]->columns(), input_headers[1]->columns());

        /// The planner casts both sides of each condition to a common type.
        const auto & left_type = input_headers[0]->getByPosition(condition.left_key_position).type;
        const auto & right_type = input_headers[1]->getByPosition(condition.right_key_position).type;
        if (!left_type->equals(*right_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoin key types do not match: {} and {}",
                left_type->getName(), right_type->getName());
    }

    key_order[0].descending
        = conditions[0].op == JoinConditionOperator::Greater || conditions[0].op == JoinConditionOperator::GreaterOrEquals;
    key_order[1].descending
        = conditions[1].op == JoinConditionOperator::Less || conditions[1].op == JoinConditionOperator::LessOrEquals;
    for (size_t key_index = 0; key_index < 2; ++key_index)
        key_order[key_index].strict
            = conditions[key_index].op == JoinConditionOperator::Less || conditions[key_index].op == JoinConditionOperator::Greater;

    if (residual_)
        residual.emplace(std::move(*residual_), *input_headers[0], *input_headers[1]);
}

void IEJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two inputs are required, got {}", inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
        consume(inputs[i], i);
}

void IEJoinAlgorithm::consume(Input & input, size_t source_num)
{
    if (input.skip_last_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "skip_last_row is not supported");

    if (input.permutation)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "permutation is not supported");

    if (!input.chunk)
    {
        source_finished[source_num] = true;
        return;
    }

    removeConstAndSparse(input);

    /// Both inputs are materialized entirely, so the join size limits apply to their total.
    /// Append-then-check, like `HashJoin`: the soft check fails at >=, so with
    /// `join_overflow_mode = 'break'` the chunk that reaches the limit is kept and the rest
    /// of both inputs is dropped.
    if (size_limit_reached)
        return;

    stat.num_blocks[source_num] += 1;
    stat.num_rows[source_num] += input.chunk.getNumRows();
    stat.num_bytes[source_num] += input.chunk.allocatedBytes();

    if (input.chunk.getNumRows() > 0)
    {
#ifndef NDEBUG
        if (inputs_sorted_by_first_key)
            checkInputChunkOrder(input.chunk, source_num);
#endif
        accumulated_chunks[source_num].push_back(std::move(input.chunk));
    }

    if (!size_limits.check(
            stat.num_rows[0] + stat.num_rows[1], stat.num_bytes[0] + stat.num_bytes[1],
            "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        size_limit_reached = true;
}

#ifndef NDEBUG
void IEJoinAlgorithm::checkInputChunkOrder(const Chunk & chunk, size_t source_num)
{
    ColumnPtr key = chunk.getColumns()[conditions[0].keyPosition(source_num)]->convertToFullColumnIfReplicated();
    const size_t rows = key->size();
    /// The plan-level sort is ascending with NULLS LAST, which is nan_direction_hint = 1.
    for (size_t row = 1; row < rows; ++row)
        chassert(key->compareAt(row - 1, row, *key, /* nan_direction_hint */ 1) <= 0);

    const auto & previous_key = last_input_key_column[source_num];
    if (previous_key)
        chassert(previous_key->compareAt(previous_key->size() - 1, 0, *key, /* nan_direction_hint */ 1) <= 0);
    last_input_key_column[source_num] = std::move(key);
}
#endif

int IEJoinAlgorithm::compareKeysAt(size_t key_index, size_t union_a, size_t union_b) const
{
    size_t side_a = union_a < num_side_rows[0] ? 0 : 1;
    size_t row_a = union_a - (side_a ? num_side_rows[0] : 0);
    size_t side_b = union_b < num_side_rows[0] ? 0 : 1;
    size_t row_b = union_b - (side_b ? num_side_rows[0] : 0);
    const auto & keys = key_columns[key_index];
    return keys.bySide(side_a)->compareAt(row_a, row_b, *keys.bySide(side_b), /* nan_direction_hint */ 1);
}

bool IEJoinAlgorithm::sideNeedsUnmatchedRows(size_t side) const
{
    if (side == 0)
        return kind == IEJoinKind::Left || kind == IEJoinKind::Full || kind == IEJoinKind::LeftAnti;
    return kind == IEJoinKind::Right || kind == IEJoinKind::Full;
}

bool IEJoinAlgorithm::frontierAdvances(size_t l2_from, size_t l2_current) const
{
    /// The value of `l2_from` satisfies the second condition with respect to the value of `l2_current`
    /// exactly when it sorts strictly earlier in the L2 order (or non-strictly for a loose condition):
    /// L2 is ordered so that earlier entries satisfy the condition against all later ones.
    int cmp = compareKeysAt(1, l1_entries[permutation[l2_from]], l1_entries[permutation[l2_current]]);
    int oriented = key_order[1].descending ? cmp : -cmp;
    return key_order[1].strict ? oriented > 0 : oriented >= 0;
}

void IEJoinAlgorithm::runBuildStage()
{
    switch (build_stage)
    {
        case BuildStage::MaterializeLeft:
            build_validity[0] = materializeSide(0);
            build_stage = BuildStage::MaterializeRight;
            break;
        case BuildStage::MaterializeRight:
            build_validity[1] = materializeSide(1);
            num_union_entries = build_validity[0].num_valid + build_validity[1].num_valid;
            build_stage = BuildStage::EncodeKeys;
            break;
        case BuildStage::EncodeKeys:
            build_encoded_keys = encodeKeys();
            build_stage = BuildStage::BuildL1;
            break;
        case BuildStage::BuildL1:
            buildL1(build_validity, build_encoded_keys[0]);
            /// Only the L1 build reads the first condition's encoding; freeing it here keeps it
            /// out of the L2 build's peak.
            build_encoded_keys[0] = {};
            build_stage = BuildStage::BuildL2;
            break;
        case BuildStage::BuildL2:
            buildL2(build_validity, build_encoded_keys[1]);
            bit_array.resize_fill((num_union_entries + 63) / 64);
            build_validity = {};
            build_encoded_keys = {};
            build_stage = BuildStage::Done;
            break;
        case BuildStage::Done:
            break;
    }
}

/// Concatenate the accumulated chunks into full (non-replicated) columns of the header's layout.
static Columns concatenateChunks(const Block & header, Chunks chunks)
{
    const size_t num_columns = header.columns();

    Columns columns;
    if (chunks.empty())
    {
        columns.reserve(num_columns);
        for (const auto & column_with_type : header)
            columns.push_back(column_with_type.type->createColumn());
    }
    else if (chunks.size() == 1)
    {
        columns = chunks.front().detachColumns();
        for (auto & column : columns)
            column = column->convertToFullColumnIfReplicated();
    }
    else
    {
        size_t total_rows = 0;
        for (const auto & chunk : chunks)
            total_rows += chunk.getNumRows();

        MutableColumns mutable_columns;
        mutable_columns.reserve(num_columns);
        for (const auto & column_with_type : header)
        {
            auto column = column_with_type.type->createColumn();
            column->reserve(total_rows);
            mutable_columns.push_back(std::move(column));
        }

        for (auto & chunk : chunks)
        {
            auto chunk_columns = chunk.detachColumns();
            for (size_t i = 0; i < num_columns; ++i)
            {
                auto full_column = chunk_columns[i]->convertToFullColumnIfReplicated();
                mutable_columns[i]->insertRangeFrom(*full_column, 0, full_column->size());
            }
        }

        columns.reserve(num_columns);
        for (auto & column : mutable_columns)
            columns.push_back(std::move(column));
    }
    return columns;
}

IEJoinAlgorithm::SideValidity IEJoinAlgorithm::materializeSide(size_t side)
{
    Columns columns = concatenateChunks(*input_headers[side], std::move(accumulated_chunks[side]));
    accumulated_chunks[side].clear();

    const size_t rows = columns.empty() ? 0 : columns.front()->size();

    /// The two conditions may read the same column (e.g. `x BETWEEN a AND b`), prepare it once.
    const bool side_key_shared = conditions[0].keyPosition(side) == conditions[1].keyPosition(side);
    std::array<ColumnPtr, 2> comparison_keys;
    for (size_t key_index = 0; key_index < 2; ++key_index)
    {
        if (key_index == 1 && side_key_shared)
            comparison_keys[1] = comparison_keys[0];
        else
            comparison_keys[key_index] = columns[conditions[key_index].keyPosition(side)]->convertToFullColumnIfLowCardinality();
    }

    /// Rows with NULL in any key never enter the union: a NULL fails every inequality, so
    /// they cannot produce matches. Rows with a NaN key are excluded for the same reason:
    /// the operator matches by the `compareAt` total order, where NaN is an ordinary greatest
    /// value, but the predicates the join implements follow IEEE semantics, under which every
    /// comparison involving NaN is false. The rows stay in `side_columns`, their matched bits
    /// are never set, and the post-phases emit them as unmatched naturally.
    SideValidity validity;
    auto & valid = validity.mask;
    auto exclude_rows = [&](auto && is_excluded)
    {
        if (valid.empty())
            valid.resize_fill(rows, 1);
        for (size_t row = 0; row < rows; ++row)
            valid[row] &= !is_excluded(row);
    };
    for (size_t key_index = 0; key_index < (side_key_shared ? 1u : 2u); ++key_index)
    {
        const IColumn * key = comparison_keys[key_index].get();
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

    validity.num_valid = rows;
    if (!valid.empty())
    {
        validity.num_valid = countBytesInFilter(valid);
        if (validity.num_valid == rows)
            valid.clear();
    }

    side_columns[side] = std::move(columns);
    for (size_t key_index = 0; key_index < 2; ++key_index)
        key_columns[key_index].bySide(side) = std::move(comparison_keys[key_index]);
    num_side_rows[side] = rows;

    if (sideNeedsUnmatchedRows(side))
        matched[side].resize_fill(num_side_rows[side], 0);

    return validity;
}

std::array<PaddedPODArray<UInt64>, 2> IEJoinAlgorithm::encodeKeys() const
{
    /// Encoded fixed-width keys per condition, in union-entry order ([left rows..., right rows...])
    /// with the L1/L2 direction folded in, so every hot-loop comparison is a plain `enc[a] < enc[b]`.
    const std::array<UInt64, 2> direction_mask = {key_order[0].descending ? ~UInt64(0) : 0, key_order[1].descending ? ~UInt64(0) : 0};
    std::array<PaddedPODArray<UInt64>, 2> encoded_keys;
    for (size_t key_index = 0; key_index < 2; ++key_index)
    {
        auto & encoded = encoded_keys[key_index];
        encoded.reserve(num_side_rows[0] + num_side_rows[1]);
        for (size_t side = 0; side < 2; ++side)
        {
            const auto & key = key_columns[key_index].bySide(side);
            /// The side may read one column in both conditions (the `BETWEEN` shape): derive the
            /// second encoding from the first instead of encoding the column twice.
            if (key_index == 1 && key == key_columns[0].bySide(side) && !encoded_keys[0].empty())
            {
                const UInt64 reorient = direction_mask[0] ^ direction_mask[1];
                const size_t offset = side ? num_side_rows[0] : 0;
                const size_t old_size = encoded.size();
                encoded.resize(old_size + num_side_rows[side]);
                for (size_t row = 0; row < num_side_rows[side]; ++row)
                    encoded[old_size + row] = encoded_keys[0][offset + row] ^ reorient;
                continue;
            }
            if (!tryAppendEncodedKeys(*key, direction_mask[key_index], encoded))
            {
                encoded = {};
                break;
            }
        }
    }
    return encoded_keys;
}

void IEJoinAlgorithm::buildL1(const std::array<SideValidity, 2> & validity, const PaddedPODArray<UInt64> & encoded_keys)
{
    const bool l1_descending = key_order[0].descending;
    const bool op1_strict = key_order[0].strict;

    /// The L1 order: the first condition's keys in the direction of the operator family, with
    /// ties resolved on the origin side so that for every left entry its own L1 position is the
    /// exact scan boundary. For a loose condition equal-keyed right entries must land at higher
    /// positions than left ones (inside the scan zone: `x <= x` holds), for a strict condition
    /// at lower positions (outside: `x < x` fails). This also makes a row never match its own
    /// other-side copy in a self-join.
    auto l1_order_less = [&](UInt64 a, UInt64 b)
    {
        int cmp = compareKeysAt(0, a, b);
        if (cmp != 0)
            return l1_descending ? cmp > 0 : cmp < 0;

        bool a_from_left = a < num_side_rows[0];
        bool b_from_left = b < num_side_rows[0];
        if (a_from_left != b_from_left)
            return op1_strict ? b_from_left : a_from_left;
        return false;
    };

    /// L1: the union of both sides in the L1 order, so that for any two entries the one at the
    /// higher position satisfies the condition with respect to the lower one.
    l1_entries.resize(num_union_entries);
    if (inputs_sorted_by_first_key)
    {
        /// Each input arrives pre-sorted ascending by its first-condition key (skipping NULL-key
        /// rows keeps the order), so L1 is a two-way merge. A descending L1 iterates both
        /// materialized sides backwards: the direction is just a view over the sorted input.
        /// The tie policy is stated in terms of final L1 positions, so it is independent of the
        /// iteration direction; the order within a run of equal keys on one side is irrelevant.
        const Int64 step = l1_descending ? -1 : 1;
        std::array<Int64, 2> cursor = {0, 0};
        auto in_range = [&](size_t side) { return cursor[side] >= 0 && cursor[side] < static_cast<Int64>(num_side_rows[side]); };
        /// Advance the cursor to a row with non-NULL keys in the iteration direction.
        auto skip_invalid = [&](size_t side)
        {
            if (validity[side].mask.empty())
                return;
            while (in_range(side) && !validity[side].mask[cursor[side]])
                cursor[side] += step;
        };
        for (size_t side = 0; side < 2; ++side)
        {
            cursor[side] = l1_descending ? static_cast<Int64>(num_side_rows[side]) - 1 : 0;
            skip_invalid(side);
        }

        size_t out = 0;
        auto take = [&](size_t side)
        {
            l1_entries[out++] = (side ? num_side_rows[0] : 0) + cursor[side];
            cursor[side] += step;
            skip_invalid(side);
        };
        /// On equal keys a loose condition pulls the left entry first, a strict one the right.
        auto merge_sides = [&](auto && take_left_first)
        {
            while (in_range(0) && in_range(1))
                take(take_left_first() ? 0 : 1);
            while (in_range(0))
                take(0);
            while (in_range(1))
                take(1);
        };
        if (!encoded_keys.empty())
        {
            merge_sides([&]
            {
                UInt64 left_key = encoded_keys[cursor[0]];
                UInt64 right_key = encoded_keys[num_side_rows[0] + cursor[1]];
                return left_key != right_key ? left_key < right_key : !op1_strict;
            });
        }
        else
        {
            merge_sides([&]
            {
                int cmp = compareKeysAt(0, cursor[0], num_side_rows[0] + cursor[1]);
                if (l1_descending)
                    cmp = -cmp;
                return cmp != 0 ? cmp < 0 : !op1_strict;
            });
        }
        chassert(out == num_union_entries);
    }
    else
    {
        size_t out = 0;
        for (size_t side = 0; side < 2; ++side)
        {
            const size_t offset = side ? num_side_rows[0] : 0;
            const auto & mask = validity[side].mask;
            for (size_t row = 0; row < num_side_rows[side]; ++row)
                if (mask.empty() || mask[row])
                    l1_entries[out++] = offset + row;
        }
        chassert(out == num_union_entries);
        if (!encoded_keys.empty())
        {
            ::sort(l1_entries.begin(), l1_entries.end(), [&](UInt64 a, UInt64 b)
            {
                if (encoded_keys[a] != encoded_keys[b])
                    return encoded_keys[a] < encoded_keys[b];
                bool a_from_left = a < num_side_rows[0];
                bool b_from_left = b < num_side_rows[0];
                if (a_from_left != b_from_left)
                    return op1_strict ? b_from_left : a_from_left;
                return false;
            });
        }
        else
            ::sort(l1_entries.begin(), l1_entries.end(), l1_order_less);
    }

#ifndef NDEBUG
    /// Both L1 build paths must produce exactly an order the generic comparator accepts; with
    /// encoded keys this cross-validates the encoding against `compareAt`.
    for (size_t pos = 1; pos < num_union_entries; ++pos)
        chassert(!l1_order_less(l1_entries[pos], l1_entries[pos - 1]));
#endif

}

void IEJoinAlgorithm::buildL2(const std::array<SideValidity, 2> & validity, PaddedPODArray<UInt64> & encoded_keys)
{
    const bool l2_descending = key_order[1].descending;

    /// L2: L1 positions ordered by the second condition's keys, so that every entry satisfies
    /// the condition with respect to all entries after it. No tie-break is needed: the frontier
    /// advances by comparing values, so the order within a run of equal keys is irrelevant.
    auto l2_order_less = [&](size_t pos_a, size_t pos_b)
    {
        int cmp = compareKeysAt(1, l1_entries[pos_a], l1_entries[pos_b]);
        return l2_descending ? cmp > 0 : cmp < 0;
    };

    /// The encoded second-condition keys gathered by L1 position, so the L2 sort comparator
    /// needs no union-entry resolution at all.
    if (!encoded_keys.empty())
    {
        l2_keys_by_position.resize(num_union_entries);
        for (size_t pos = 0; pos < num_union_entries; ++pos)
            l2_keys_by_position[pos] = encoded_keys[l1_entries[pos]];
    }
    /// Fully folded into `l2_keys_by_position`: freed before the sort, so the union-order copy
    /// does not sit in the build-phase peak.
    encoded_keys = {};

    permutation.resize(num_union_entries);
    /// The side whose entries are already in L2 order within L1 order: it reads the same column
    /// in both conditions and the operator families are opposite - exactly then the L1 and L2
    /// directions coincide (entries with equal first keys have equal second keys). This is the
    /// `x BETWEEN a AND b` shape with `x` on either side of the join.
    std::optional<size_t> in_order_side;
    if (key_order[0].descending == l2_descending)
    {
        if (conditions[0].left_key_position == conditions[1].left_key_position)
            in_order_side = 0;
        else if (conditions[0].right_key_position == conditions[1].right_key_position)
            in_order_side = 1;
    }

    auto build_l2_order = [&](auto && less)
    {
        if (in_order_side)
        {
            /// Build L2 by sorting only the other side's entries by the second key and merging the
            /// two runs, instead of sorting the whole union.
            const bool in_order_from_left = *in_order_side == 0;
            IColumn::Permutation sorted_positions;
            sorted_positions.reserve(validity[1 - *in_order_side].num_valid);
            size_t num_in_order = 0;
            for (size_t pos = 0; pos < num_union_entries; ++pos)
            {
                if (entryIsLeft(l1_entries[pos]) == in_order_from_left)
                    permutation[num_in_order++] = pos;
                else
                    sorted_positions.push_back(pos);
            }
            chassert(num_in_order == validity[*in_order_side].num_valid);
            ::sort(sorted_positions.begin(), sorted_positions.end(), less);

            /// Merge the two runs in place from the back; a prefix of the in-order run that is never
            /// displaced stays where it is.
            size_t in_order_remaining = num_in_order;
            size_t sorted_remaining = sorted_positions.size();
            size_t write = num_union_entries;
            while (in_order_remaining > 0 && sorted_remaining > 0)
            {
                if (less(permutation[in_order_remaining - 1], sorted_positions[sorted_remaining - 1]))
                    permutation[--write] = sorted_positions[--sorted_remaining];
                else
                    permutation[--write] = permutation[--in_order_remaining];
            }
            while (sorted_remaining > 0)
                permutation[--write] = sorted_positions[--sorted_remaining];
            chassert(write == in_order_remaining);
        }
        else
        {
            iota(permutation.data(), num_union_entries, size_t(0));
            ::sort(permutation.begin(), permutation.end(), less);
        }
    };

    if (!l2_keys_by_position.empty())
        build_l2_order([&](size_t pos_a, size_t pos_b) { return l2_keys_by_position[pos_a] < l2_keys_by_position[pos_b]; });
    else
        build_l2_order(l2_order_less);

#ifndef NDEBUG
    /// Both L2 build paths must produce an order the general comparator accepts; with encoded
    /// keys this cross-validates the encoding against `compareAt`.
    for (size_t i = 1; i < num_union_entries; ++i)
        chassert(!l2_order_less(permutation[i], permutation[i - 1]));
#endif
}

void IEJoinAlgorithm::setBit(size_t pos)
{
    bit_array[pos / 64] |= UInt64(1) << (pos % 64);
    bit_array_end = std::max(bit_array_end, pos + 1);
}

bool IEJoinAlgorithm::testBit(size_t pos) const
{
    return (bit_array[pos / 64] >> (pos % 64)) & 1;
}

std::optional<size_t> IEJoinAlgorithm::findNextSetBit()
{
    /// No bit at or past bit_array_end is set, so the scan can stop there. This bounds the
    /// common case where all matches of the current entry sit right after its own position
    /// (e.g. band joins): without it every scan would walk empty words to the end of the array.
    if (scan_pos >= bit_array_end)
        return num_union_entries;

    size_t word_index = scan_pos / 64;
    const size_t word_end = (bit_array_end + 63) / 64;
    UInt64 word = bit_array[word_index] & (~UInt64(0) << (scan_pos % 64));
    while (true)
    {
        ++produce_work;
        if (word)
        {
            size_t pos = word_index * 64 + std::countr_zero(word);
            scan_pos = pos + 1;
            return pos;
        }
        ++word_index;
        if (word_index >= word_end)
            return num_union_entries;
        if (produce_work >= produce_work_budget)
        {
            /// Yield mid-scan: all inspected words were clear, resume at the first uninspected one.
            scan_pos = word_index * 64;
            return std::nullopt;
        }
        word = bit_array[word_index];
    }
}

void IEJoinAlgorithm::checkFrontierInvariant() const
{
    /// The set of L2 entries qualifying against the current entry is a prefix of L2, and the frontier
    /// has processed exactly that prefix: a bit is set iff its entry is right-side and qualifies.
    for (size_t i = 0; i < num_union_entries; ++i)
    {
        bool qualifies = frontierAdvances(i, l2_cursor);
        bool is_right_side = !entryIsLeft(l1_entries[permutation[i]]);
        chassert(testBit(permutation[i]) == (is_right_side && qualifies));
    }
}

bool IEJoinAlgorithm::nextLeftRow()
{
    while (l2_cursor < num_union_entries)
    {
        if (produce_work >= produce_work_budget)
            return false;
        ++produce_work;

        size_t pos = permutation[l2_cursor];
        UInt64 entry = l1_entries[pos];
        if (!entryIsLeft(entry))
        {
            /// Right-side entry: nothing to do here, it is marked when the frontier passes it.
            ++l2_cursor;
            continue;
        }

        auto advance_frontier_while = [&](auto && qualifies)
        {
            while (frontier < num_union_entries && produce_work < produce_work_budget && qualifies(frontier))
            {
                ++produce_work;
                size_t frontier_pos = permutation[frontier];
                /// Mark right-side entries only, so that a row can never match same-side rows.
                if (!entryIsLeft(l1_entries[frontier_pos]))
                    setBit(frontier_pos);
                ++frontier;
            }
        };
        if (!l2_keys_by_position.empty())
        {
            /// An entry qualifies when its key sorts before the current entry's in the L2 order,
            /// non-strictly for a loose condition; the encoding reproduces equality exactly.
            const UInt64 current_key = l2_keys_by_position[pos];
            if (key_order[1].strict)
                advance_frontier_while([&](size_t from) { return l2_keys_by_position[permutation[from]] < current_key; });
            else
                advance_frontier_while([&](size_t from) { return l2_keys_by_position[permutation[from]] <= current_key; });
        }
        else
        {
            advance_frontier_while([&](size_t from) { return frontierAdvances(from, l2_cursor); });
        }
        /// The frontier is monotone and never exceeds the union size.
        chassert(frontier <= num_union_entries);

        /// The frontier advance may have stopped on the work budget instead of on the first
        /// non-qualifying entry: yield and re-enter at the same L2 position, the frontier
        /// continues from where it stopped.
        if (frontier < num_union_entries && produce_work >= produce_work_budget)
            return false;

        current_left_row = entry;
        /// The tie-break in the L1 order guarantees that the entry's own position is the exact scan
        /// boundary: every set bit at a position >= pos is a match. The bit at pos itself can never
        /// be set because pos holds a left-side entry.
        scan_pos = pos;

#ifndef NDEBUG
        /// O(num_union_entries) per left row, affordable only on small inputs.
        if (num_union_entries <= 1024)
            checkFrontierInvariant();
#endif

        return true;
    }
    return false;
}

Chunk IEJoinAlgorithm::produceBatch()
{
    Chunk chunk;
    bool done = producePairsBatch(chunk);
    if (chunk.hasRows() || !done)
        return chunk;

    for (size_t side = 0; side < 2; ++side)
    {
        if (!sideNeedsUnmatchedRows(side))
            continue;

        done = produceUnmatchedBatch(side, chunk);
        if (chunk.hasRows() || !done)
            return chunk;
    }

    produce_done = true;
    checkFinalInvariants();
    return {};
}

bool IEJoinAlgorithm::producePairsBatch(Chunk & chunk)
{
    auto left_indexes = ColumnUInt64::create();
    auto right_indexes = ColumnUInt64::create();
    auto & left_data = left_indexes->getData();
    auto & right_data = right_indexes->getData();

    const bool is_semi_or_anti = kind == IEJoinKind::LeftSemi || kind == IEJoinKind::LeftAnti;

    /// With a residual condition candidate pairs are not emitted directly: they accumulate in
    /// these local columns and a flush evaluates the residual over them in one batch. Nothing
    /// survives the call: both columns are flushed before every return, so all resumable state
    /// stays exactly the scan cursors.
    auto pending_left = ColumnUInt64::create();
    auto pending_right = ColumnUInt64::create();
    auto & pending_left_data = pending_left->getData();
    auto & pending_right_data = pending_right->getData();

    /// The work budget bounds the cursor advances, bit-array words inspected, and residual
    /// candidates evaluated by one call, so control returns to the executor (which observes
    /// cancellation) even when a long stretch of the scan emits nothing, e.g. an ANTI join.
    /// All loop state is resumable: an exhausted budget just yields an incomplete (possibly
    /// empty) non-final chunk.
    produce_work = 0;

    bool done = false;
    while (left_data.size() + pending_left_data.size() < max_block_size && produce_work < produce_work_budget)
    {
        if (!has_current_left)
        {
            if (!nextLeftRow())
            {
                done = l2_cursor >= num_union_entries;
                break;
            }
            has_current_left = true;
        }

        std::optional<size_t> found = findNextSetBit();
        if (!found)
            break;
        if (*found >= num_union_entries)
        {
            /// The current left row's scan is exhausted; for SEMI/ANTI decide it on the
            /// candidates accumulated so far.
            if (residual && is_semi_or_anti)
                decideSemiAntiRow(*pending_left, *pending_right, left_data, right_data);
            has_current_left = false;
            ++l2_cursor;
            continue;
        }

        UInt64 right_entry = l1_entries[*found];
        /// Bits are set only at positions of right-side entries.
        chassert(has_current_left && !entryIsLeft(right_entry));

        size_t left_row = current_left_row;
        size_t right_row = right_entry - num_side_rows[0];

        /// Every found pair must satisfy both conditions, re-verify by direct evaluation.
        chassert(checkEmittedPair(0, left_row, right_row));
        chassert(checkEmittedPair(1, left_row, right_row));

        if (residual)
        {
            pending_left_data.push_back(left_row);
            pending_right_data.push_back(right_row);
            /// SEMI/ANTI evaluate per bounded mini-batch of the row's candidates: a passing
            /// candidate decides the row and skips the rest of its scan.
            if (is_semi_or_anti && pending_left_data.size() >= semi_anti_residual_batch_size
                && decideSemiAntiRow(*pending_left, *pending_right, left_data, right_data))
            {
                has_current_left = false;
                ++l2_cursor;
            }
            continue;
        }

        /// The matched bitmaps are allocated exactly for the sides that emit unmatched rows
        /// in a post-phase.
        if (!matched[0].empty())
            matched[0][left_row] = 1;
        if (!matched[1].empty())
            matched[1][right_row] = 1;

        /// For SEMI/ANTI one pair decides the fate of the left row (its matches are contiguous):
        /// skip the rest of its scan. SEMI emits the first pair, ANTI only marks.
        if (is_semi_or_anti)
        {
            has_current_left = false;
            ++l2_cursor;
        }

        if (kind != IEJoinKind::LeftAnti)
        {
            left_data.push_back(left_row);
            right_data.push_back(right_row);
        }
    }

    if (residual)
    {
        if (is_semi_or_anti)
        {
            if (decideSemiAntiRow(*pending_left, *pending_right, left_data, right_data) && has_current_left)
            {
                has_current_left = false;
                ++l2_cursor;
            }
        }
        else
            flushPendingPairs(*pending_left, *pending_right, left_data, right_data);
    }

    if (!left_data.empty())
    {
        appendGathered(chunk, 0, *left_indexes);
        appendGathered(chunk, 1, *right_indexes);
    }
    return done;
}

void IEJoinAlgorithm::flushPendingPairs(
    ColumnUInt64 & pending_left, ColumnUInt64 & pending_right,
    ColumnUInt64::Container & left_out, ColumnUInt64::Container & right_out)
{
    auto & pending_left_data = pending_left.getData();
    auto & pending_right_data = pending_right.getData();
    if (pending_left_data.empty())
        return;

    IColumn::Filter mask = evaluateResidualMask(pending_left, pending_right);
    for (size_t i = 0; i < mask.size(); ++i)
    {
        if (!mask[i])
            continue;
        if (!matched[0].empty())
            matched[0][pending_left_data[i]] = 1;
        if (!matched[1].empty())
            matched[1][pending_right_data[i]] = 1;
        left_out.push_back(pending_left_data[i]);
        right_out.push_back(pending_right_data[i]);
    }
    pending_left_data.clear();
    pending_right_data.clear();
}

bool IEJoinAlgorithm::decideSemiAntiRow(
    ColumnUInt64 & pending_left, ColumnUInt64 & pending_right,
    ColumnUInt64::Container & left_out, ColumnUInt64::Container & right_out)
{
    auto & pending_left_data = pending_left.getData();
    auto & pending_right_data = pending_right.getData();
    if (pending_left_data.empty())
        return false;

    IColumn::Filter mask = evaluateResidualMask(pending_left, pending_right);
    size_t first_passing = mask.size();
    for (size_t i = 0; i < mask.size() && first_passing == mask.size(); ++i)
        if (mask[i])
            first_passing = i;

    bool decided = first_passing != mask.size();
    if (decided)
    {
        if (kind == IEJoinKind::LeftSemi)
        {
            left_out.push_back(pending_left_data[first_passing]);
            right_out.push_back(pending_right_data[first_passing]);
        }
        else
            matched[0][pending_left_data[first_passing]] = 1;
    }
    pending_left_data.clear();
    pending_right_data.clear();
    return decided;
}

bool IEJoinAlgorithm::produceUnmatchedBatch(size_t side, Chunk & chunk)
{
    /// Rows that the pair scan did not mark; rows with NULL keys never entered the union,
    /// so their bits are never set and they are emitted here as well.
    auto & row_cursor = unmatched_row_cursor[side];
    auto indexes = ColumnUInt64::create();
    auto & data = indexes->getData();
    /// Bounded by the work budget as well: when (nearly) every row is matched the block never
    /// fills and one call would otherwise walk the whole side.
    while (row_cursor < num_side_rows[side] && data.size() < max_block_size && produce_work < produce_work_budget)
    {
        ++produce_work;
        if (!matched[side][row_cursor])
            data.push_back(row_cursor);
        ++row_cursor;
    }

    if (!data.empty())
    {
        unmatched_emitted[side] += data.size();
        if (side == 0)
        {
            appendGathered(chunk, 0, *indexes);
            appendPadded(chunk, 1, data.size());
        }
        else
        {
            appendPadded(chunk, 0, data.size());
            appendGathered(chunk, 1, *indexes);
        }
    }
    return row_cursor >= num_side_rows[side];
}

void IEJoinAlgorithm::appendGathered(Chunk & chunk, size_t side, const ColumnUInt64 & indexes) const
{
    for (const auto & column : side_columns[side])
        chunk.addColumn(column->index(indexes, 0));
}

IColumn::Filter IEJoinAlgorithm::evaluateResidualMask(const ColumnUInt64 & left_rows, const ColumnUInt64 & right_rows)
{
    size_t num_rows = left_rows.size();
    chassert(num_rows == right_rows.size());
    produce_work += num_rows;

    Columns expression_columns;
    expression_columns.reserve(residual->sources().size());
    for (const auto & source : residual->sources())
        expression_columns.push_back(side_columns[source.side][source.position]->index(source.side == 0 ? left_rows : right_rows, 0));

    return residual->evaluateMask(std::move(expression_columns), num_rows);
}

void IEJoinAlgorithm::appendPadded(Chunk & chunk, size_t side, size_t num_rows) const
{
    for (const auto & column_with_type : *input_headers[side])
    {
        /// The default of the column type: NULL when it is Nullable (with `join_use_nulls`
        /// the planner made the padded side's columns Nullable in the pre-join actions).
        auto column = column_with_type.type->createColumn();
        column->insertManyDefaults(num_rows);
        chunk.addColumn(std::move(column));
    }
}

void IEJoinAlgorithm::checkFinalInvariants() const
{
    /// Every filtered row of a post-phase side ended up exactly one of matched/unmatched.
    for (size_t side = 0; side < 2; ++side)
    {
        if (sideNeedsUnmatchedRows(side))
            chassert(countBytesInFilter(matched[side]) + unmatched_emitted[side] == num_side_rows[side]);
    }
}

bool IEJoinAlgorithm::checkEmittedPair(size_t key_index, size_t left_row, size_t right_row) const
{
    const auto & keys = key_columns[key_index];
    int cmp = keys.left->compareAt(left_row, right_row, *keys.right, /* nan_direction_hint */ 1);
    switch (conditions[key_index].op)
    {
        case JoinConditionOperator::Less:
            return cmp < 0;
        case JoinConditionOperator::LessOrEquals:
            return cmp <= 0;
        case JoinConditionOperator::Greater:
            return cmp > 0;
        case JoinConditionOperator::GreaterOrEquals:
            return cmp >= 0;
        default:
            return false;
    }
}

IMergingAlgorithm::Status IEJoinAlgorithm::merge()
{
    /// Materialize both inputs entirely, read from both uniformly to increase parallelism
    if (!source_finished[0] && !source_finished[1])
    {
        auto lhs_chunks = accumulated_chunks[0].size();
        auto rhs_chunks = accumulated_chunks[1].size();
        return Status(lhs_chunks < rhs_chunks ? 0 : 1);
    }
    else if (!source_finished[0])
        return Status(0);
    else if (!source_finished[1])
        return Status(1);

    if (build_stage != BuildStage::Done)
    {
        runBuildStage();
        /// An empty non-final status: the executor observes cancellation between the stages.
        return Status(Chunk());
    }

    if (produce_done)
        return Status({}, true);

    Chunk result = produceBatch();
    return Status(std::move(result), produce_done);
}

IMergingAlgorithm::MergedStats IEJoinAlgorithm::getMergedStats() const
{
    return
    {
        .bytes = stat.num_bytes[0] + stat.num_bytes[1],
        .rows = stat.num_rows[0] + stat.num_rows[1],
        .blocks = stat.num_blocks[0] + stat.num_blocks[1],
    };
}

IEJoinTransform::IEJoinTransform(
    IEJoinKind kind,
    const IEJoinConditions & conditions,
    std::optional<JoinResidualCondition> residual,
    bool inputs_sorted_by_first_key,
    SharedHeaders & input_headers,
    SharedHeader output_header,
    const SizeLimits & size_limits,
    size_t max_block_size)
    : IMergingTransform<IEJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        /* limit_hint_= */ 0,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        kind,
        conditions,
        std::move(residual),
        inputs_sorted_by_first_key,
        input_headers,
        size_limits,
        max_block_size)
{
}

}
