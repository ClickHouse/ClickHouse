#include <algorithm>
#include <bit>
#include <optional>
#include <type_traits>

#include <base/defines.h>
#include <base/sort.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Processors/Transforms/IEJoinTransform.h>
#include <Common/NaNUtils.h>
#include <Common/iota.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

static bool isInequalityOperator(JoinConditionOperator op)
{
    return op == JoinConditionOperator::Less || op == JoinConditionOperator::LessOrEquals
        || op == JoinConditionOperator::Greater || op == JoinConditionOperator::GreaterOrEquals;
}

namespace
{

/// The fixed-width key fast path encodes a condition's key values once into UInt64 values whose
/// unsigned order reproduces the column's `compareAt(..., nan_direction_hint = 1)` order exactly,
/// including equality (the L1 merge tie policy and the frontier's non-strict comparisons depend
/// on it). The hot loops then compare plain integers instead of calling the virtual comparator.

/// Everything whose order is its underlying signed integer's order (signed integers, Date32,
/// DateTime64, Decimal32/64, Enum8/16): offset the sign bit so the unsigned order matches.
UInt64 encodeSignedKey(Int64 value)
{
    return static_cast<UInt64>(value) ^ (UInt64(1) << 63);
}

/// `compareAt` with nan_direction_hint = 1 treats every NaN, of either sign, as equal to other
/// NaNs and greater than all numbers, and -0.0 as equal to +0.0. The plain total-order bit trick
/// preserves neither (a negative NaN would sort below -inf), so every NaN maps to the greatest
/// encoding and -0.0 is canonicalized to +0.0 before the trick.
UInt64 encodeFloatKey(Float64 value)
{
    if (isNaN(value))
        return ~UInt64(0);
    UInt64 bits = std::bit_cast<UInt64>(value == 0.0 ? 0.0 : value);
    return bits & (UInt64(1) << 63) ? ~bits : bits | (UInt64(1) << 63);
}

UInt64 encodeFloatKey(Float32 value)
{
    if (isNaN(value))
        return ~UInt64(0);
    UInt32 bits = std::bit_cast<UInt32>(value == 0.0f ? 0.0f : value);
    return bits & (UInt32(1) << 31) ? ~bits : bits | (UInt32(1) << 31);
}

template <typename T>
UInt64 encodeKeyValue(T value)
{
    if constexpr (std::is_floating_point_v<T>)
        return encodeFloatKey(value);
    else if constexpr (is_decimal<T>)
        return encodeSignedKey(value.value);
    else if constexpr (std::is_signed_v<T>)
        return encodeSignedKey(value);
    else
        return static_cast<UInt64>(value);
}

/// Append the column's keys, encoded and XOR-ed with `flip_mask` (all-ones folds a descending
/// sort direction into the unsigned order). The dispatch is on the column type: the order the
/// encoding must reproduce is the column's own `compareAt`, so it covers exactly the types
/// stored in these columns (integers, Date/DateTime/Enum/Bool over them, floats, Decimal32/64,
/// DateTime64). Nullable encodes its nested column: rows with NULL keys never enter the union,
/// so their cells are never read and need no sentinel. Returns false when the column has no
/// fixed-width encoding (the caller then keeps the generic comparator).
bool tryAppendEncodedKeys(const IColumn & column, UInt64 flip_mask, PaddedPODArray<UInt64> & out)
{
    const IColumn * data_column = &column;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(data_column))
        data_column = &nullable->getNestedColumn();

    auto try_column_type = [&]<typename ColumnType>()
    {
        const auto * concrete = checkAndGetColumn<ColumnType>(data_column);
        if (!concrete)
            return false;

        const auto & data = concrete->getData();
        const size_t old_size = out.size();
        out.resize(old_size + data.size());
        for (size_t i = 0; i < data.size(); ++i)
            out[old_size + i] = encodeKeyValue(data[i]) ^ flip_mask;
        return true;
    };

    return try_column_type.operator()<ColumnUInt8>()
        || try_column_type.operator()<ColumnUInt16>()
        || try_column_type.operator()<ColumnUInt32>()
        || try_column_type.operator()<ColumnUInt64>()
        || try_column_type.operator()<ColumnInt8>()
        || try_column_type.operator()<ColumnInt16>()
        || try_column_type.operator()<ColumnInt32>()
        || try_column_type.operator()<ColumnInt64>()
        || try_column_type.operator()<ColumnFloat32>()
        || try_column_type.operator()<ColumnFloat64>()
        || try_column_type.operator()<ColumnDecimal<Decimal32>>()
        || try_column_type.operator()<ColumnDecimal<Decimal64>>()
        || try_column_type.operator()<ColumnDecimal<DateTime64>>();
}

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
    bool inputs_sorted_by_first_key_,
    const SharedHeaders & input_headers_,
    size_t max_block_size_)
    : input_headers(input_headers_)
    , max_block_size(std::max<size_t>(1, max_block_size_))
    , kind(kind_)
    , conditions(conditions_)
    , inputs_sorted_by_first_key(inputs_sorted_by_first_key_)
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
    const auto op2 = conditions[1].op;
    const bool descending = op2 == JoinConditionOperator::Less || op2 == JoinConditionOperator::LessOrEquals;
    const bool strict = op2 == JoinConditionOperator::Less || op2 == JoinConditionOperator::Greater;

    /// The value of `l2_from` satisfies the second condition with respect to the value of `l2_current`
    /// exactly when it sorts strictly earlier in the L2 order (or non-strictly for a loose condition):
    /// L2 is ordered so that earlier entries satisfy the condition against all later ones.
    int cmp = compareKeysAt(1, l2_union[l2_from], l2_union[l2_current]);
    int oriented = descending ? cmp : -cmp;
    return strict ? oriented > 0 : oriented >= 0;
}

void IEJoinAlgorithm::buildJoinState()
{
    join_state_built = true;

    /// Byte mask of the rows with non-NULL keys per side; left empty when every row is valid.
    std::array<IColumn::Filter, 2> valid_mask;
    std::array<size_t, 2> num_valid_rows = {0, 0};

    for (size_t side = 0; side < 2; ++side)
    {
        const auto & header = *input_headers[side];
        const size_t num_columns = header.columns();

        Columns columns;
        if (accumulated_chunks[side].empty())
        {
            columns.reserve(num_columns);
            for (const auto & column_with_type : header)
                columns.push_back(column_with_type.type->createColumn());
        }
        else if (accumulated_chunks[side].size() == 1)
        {
            columns = accumulated_chunks[side].front().detachColumns();
            for (auto & column : columns)
                column = column->convertToFullColumnIfReplicated();
        }
        else
        {
            size_t total_rows = 0;
            for (const auto & chunk : accumulated_chunks[side])
                total_rows += chunk.getNumRows();

            MutableColumns mutable_columns;
            mutable_columns.reserve(num_columns);
            for (const auto & column_with_type : header)
            {
                auto column = column_with_type.type->createColumn();
                column->reserve(total_rows);
                mutable_columns.push_back(std::move(column));
            }

            for (auto & chunk : accumulated_chunks[side])
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
        /// they cannot produce matches. The rows stay in `side_columns`, their matched bits are
        /// never set, and the post-phases emit them as unmatched naturally.
        auto & valid = valid_mask[side];
        for (size_t key_index = 0; key_index < (side_key_shared ? 1u : 2u); ++key_index)
        {
            const auto & key = comparison_keys[key_index];
            const auto * nullable = checkAndGetColumn<ColumnNullable>(key.get());
            if (!nullable)
                continue;

            const auto & null_map = nullable->getNullMapData();
            if (valid.empty())
                valid.resize_fill(rows, 1);
            for (size_t row = 0; row < rows; ++row)
                valid[row] &= !null_map[row];
        }

        num_valid_rows[side] = rows;
        if (!valid.empty())
        {
            num_valid_rows[side] = countBytesInFilter(valid);
            if (num_valid_rows[side] == rows)
                valid.clear();
        }

        side_columns[side] = std::move(columns);
        for (size_t key_index = 0; key_index < 2; ++key_index)
            key_columns[key_index].bySide(side) = std::move(comparison_keys[key_index]);
        num_side_rows[side] = rows;

        if (sideNeedsUnmatchedRows(side))
            matched[side].resize_fill(num_side_rows[side], 0);
    }

    n_union = num_valid_rows[0] + num_valid_rows[1];

    const auto op1 = conditions[0].op;
    const bool l1_descending = op1 == JoinConditionOperator::Greater || op1 == JoinConditionOperator::GreaterOrEquals;
    const bool op1_strict = op1 == JoinConditionOperator::Less || op1 == JoinConditionOperator::Greater;

    const auto op2 = conditions[1].op;
    const bool l2_descending = op2 == JoinConditionOperator::Less || op2 == JoinConditionOperator::LessOrEquals;

    /// Encoded fixed-width keys per condition, in union-entry order ([left rows..., right rows...])
    /// with the L1/L2 direction folded in, so every hot-loop comparison is a plain `enc[a] < enc[b]`.
    /// An empty array means the condition's type has no encoding and the generic comparator runs;
    /// the two conditions decide independently.
    const std::array<UInt64, 2> direction_mask = {l1_descending ? ~UInt64(0) : 0, l2_descending ? ~UInt64(0) : 0};
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
    l1_union.resize(n_union);
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
            if (valid_mask[side].empty())
                return;
            while (in_range(side) && !valid_mask[side][cursor[side]])
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
            l1_union[out++] = (side ? num_side_rows[0] : 0) + cursor[side];
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
        if (!encoded_keys[0].empty())
        {
            merge_sides([&]
            {
                UInt64 left_key = encoded_keys[0][cursor[0]];
                UInt64 right_key = encoded_keys[0][num_side_rows[0] + cursor[1]];
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
        chassert(out == n_union);
    }
    else
    {
        size_t out = 0;
        for (size_t side = 0; side < 2; ++side)
        {
            const size_t offset = side ? num_side_rows[0] : 0;
            for (size_t row = 0; row < num_side_rows[side]; ++row)
                if (valid_mask[side].empty() || valid_mask[side][row])
                    l1_union[out++] = offset + row;
        }
        chassert(out == n_union);
        if (!encoded_keys[0].empty())
        {
            ::sort(l1_union.begin(), l1_union.end(), [&](UInt64 a, UInt64 b)
            {
                if (encoded_keys[0][a] != encoded_keys[0][b])
                    return encoded_keys[0][a] < encoded_keys[0][b];
                bool a_from_left = a < num_side_rows[0];
                bool b_from_left = b < num_side_rows[0];
                if (a_from_left != b_from_left)
                    return op1_strict ? b_from_left : a_from_left;
                return false;
            });
        }
        else
            ::sort(l1_union.begin(), l1_union.end(), l1_order_less);
    }

#ifndef NDEBUG
    /// Both L1 build paths must produce exactly an order the generic comparator accepts; with
    /// encoded keys this cross-validates the encoding against `compareAt`.
    for (size_t pos = 1; pos < n_union; ++pos)
        chassert(!l1_order_less(l1_union[pos], l1_union[pos - 1]));
#endif

    li.resize(n_union);
    for (size_t pos = 0; pos < n_union; ++pos)
    {
        UInt64 entry = l1_union[pos];
        /// Row ids are 1-based so that the sign always carries the side.
        if (entry < num_side_rows[0])
            li[pos] = static_cast<Int64>(entry) + 1;
        else
            li[pos] = -(static_cast<Int64>(entry - num_side_rows[0]) + 1);
    }

    /// L2: L1 positions ordered by the second condition's keys, so that every entry satisfies
    /// the condition with respect to all entries after it. No tie-break is needed: the frontier
    /// advances by comparing values, so the order within a run of equal keys is irrelevant.
    auto l2_order_less = [&](UInt64 pos_a, UInt64 pos_b)
    {
        int cmp = compareKeysAt(1, l1_union[pos_a], l1_union[pos_b]);
        return l2_descending ? cmp > 0 : cmp < 0;
    };

    /// The encoded second-condition keys gathered by L1 position, so the L2 sort comparator
    /// needs no union-entry resolution at all.
    if (!encoded_keys[1].empty())
    {
        l2_keys_by_position.resize(n_union);
        for (size_t pos = 0; pos < n_union; ++pos)
            l2_keys_by_position[pos] = encoded_keys[1][l1_union[pos]];
    }

    permutation.resize(n_union);
    /// The side whose entries are already in L2 order within L1 order: it reads the same column
    /// in both conditions and the operator families are opposite - exactly then the L1 and L2
    /// directions coincide (entries with equal first keys have equal second keys). This is the
    /// `x BETWEEN a AND b` shape with `x` on either side of the join.
    std::optional<size_t> in_order_side;
    if (l1_descending == l2_descending)
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
            PaddedPODArray<UInt64> sorted_positions;
            sorted_positions.reserve(num_valid_rows[1 - *in_order_side]);
            size_t num_in_order = 0;
            for (size_t pos = 0; pos < n_union; ++pos)
            {
                if ((li[pos] > 0) == in_order_from_left)
                    permutation[num_in_order++] = pos;
                else
                    sorted_positions.push_back(pos);
            }
            chassert(num_in_order == num_valid_rows[*in_order_side]);
            ::sort(sorted_positions.begin(), sorted_positions.end(), less);

            /// Merge the two runs in place from the back; a prefix of the in-order run that is never
            /// displaced stays where it is.
            size_t in_order_remaining = num_in_order;
            size_t sorted_remaining = sorted_positions.size();
            size_t write = n_union;
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
            iota(permutation.data(), n_union, UInt64(0));
            ::sort(permutation.begin(), permutation.end(), less);
        }
    };

    if (!l2_keys_by_position.empty())
        build_l2_order([&](UInt64 pos_a, UInt64 pos_b) { return l2_keys_by_position[pos_a] < l2_keys_by_position[pos_b]; });
    else
        build_l2_order(l2_order_less);

#ifndef NDEBUG
    /// Both L2 build paths must produce an order the general comparator accepts; with encoded
    /// keys this cross-validates the encoding against `compareAt`.
    for (size_t i = 1; i < n_union; ++i)
        chassert(!l2_order_less(permutation[i], permutation[i - 1]));
#endif

    l2_union.resize(n_union);
    for (size_t i = 0; i < n_union; ++i)
        l2_union[i] = l1_union[permutation[i]];

    bit_array.resize_fill((n_union + 63) / 64);
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

size_t IEJoinAlgorithm::findNextSetBit(size_t from) const
{
    /// No bit at or past bit_array_end is set, so the scan can stop there. This bounds the
    /// common case where all matches of the current entry sit right after its own position
    /// (e.g. band joins): without it every scan would walk empty words to the end of the array.
    if (from >= bit_array_end)
        return n_union;

    size_t word_index = from / 64;
    const size_t word_end = (bit_array_end + 63) / 64;
    UInt64 word = bit_array[word_index] & (~UInt64(0) << (from % 64));
    while (true)
    {
        if (word)
            return word_index * 64 + std::countr_zero(word);
        ++word_index;
        if (word_index >= word_end)
            return n_union;
        word = bit_array[word_index];
    }
}

void IEJoinAlgorithm::checkFrontierInvariant() const
{
    /// The set of L2 entries qualifying against the current entry is a prefix of L2, and the frontier
    /// has processed exactly that prefix: a bit is set iff its entry is right-side and qualifies.
    for (size_t i = 0; i < n_union; ++i)
    {
        bool qualifies = frontierAdvances(i, l2_cursor);
        bool is_right_side = li[permutation[i]] < 0;
        chassert(testBit(permutation[i]) == (is_right_side && qualifies));
    }
}

bool IEJoinAlgorithm::nextLeftRow()
{
    while (l2_cursor < n_union)
    {
        UInt64 pos = permutation[l2_cursor];
        Int64 rid = li[pos];
        if (rid < 0)
        {
            /// Right-side entry: nothing to do here, it is marked when the frontier passes it.
            ++l2_cursor;
            continue;
        }

        auto advance_frontier_while = [&](auto && qualifies)
        {
            while (frontier < n_union && qualifies(frontier))
            {
                UInt64 frontier_pos = permutation[frontier];
                /// Mark right-side entries only, so that a row can never match same-side rows.
                if (li[frontier_pos] < 0)
                    setBit(frontier_pos);
                ++frontier;
            }
        };
        if (!l2_keys_by_position.empty())
        {
            /// An entry qualifies when its key sorts before the current entry's in the L2 order,
            /// non-strictly for a loose condition; the encoding reproduces equality exactly.
            const auto op2 = conditions[1].op;
            const bool op2_strict = op2 == JoinConditionOperator::Less || op2 == JoinConditionOperator::Greater;
            const UInt64 current_key = l2_keys_by_position[pos];
            if (op2_strict)
                advance_frontier_while([&](size_t from) { return l2_keys_by_position[permutation[from]] < current_key; });
            else
                advance_frontier_while([&](size_t from) { return l2_keys_by_position[permutation[from]] <= current_key; });
        }
        else
        {
            advance_frontier_while([&](size_t from) { return frontierAdvances(from, l2_cursor); });
        }
        /// The frontier is monotone and never exceeds the union size.
        chassert(frontier <= n_union);

        current_left_rid = rid;
        /// The tie-break in the L1 order guarantees that the entry's own position is the exact scan
        /// boundary: every set bit at a position >= pos is a match. The bit at pos itself can never
        /// be set because pos holds a left-side entry.
        scan_pos = pos;

#ifndef NDEBUG
        /// O(n_union) per left row, affordable only on small inputs.
        if (n_union <= 1024)
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

    bool done = false;
    while (left_data.size() < max_block_size)
    {
        if (!has_current_left)
        {
            if (!nextLeftRow())
            {
                done = true;
                break;
            }
            has_current_left = true;
        }

        size_t found = findNextSetBit(scan_pos);
        if (found >= n_union)
        {
            has_current_left = false;
            ++l2_cursor;
            continue;
        }
        scan_pos = found + 1;

        Int64 right_rid = li[found];
        /// Bits are set only at positions of right-side entries.
        chassert(current_left_rid > 0 && right_rid < 0);

        size_t left_row = current_left_rid - 1;
        size_t right_row = -right_rid - 1;

        /// Every found pair must satisfy both conditions, re-verify by direct evaluation.
        chassert(checkEmittedPair(0, left_row, right_row));
        chassert(checkEmittedPair(1, left_row, right_row));

        /// The matched bitmaps are allocated exactly for the sides that emit unmatched rows
        /// in a post-phase.
        if (!matched[0].empty())
            matched[0][left_row] = 1;
        if (!matched[1].empty())
            matched[1][right_row] = 1;

        /// For SEMI/ANTI one pair decides the fate of the left row (its matches are contiguous):
        /// skip the rest of its scan. SEMI emits the first pair, ANTI only marks.
        if (kind == IEJoinKind::LeftSemi || kind == IEJoinKind::LeftAnti)
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

    if (!left_data.empty())
    {
        appendGathered(chunk, 0, *left_indexes);
        appendGathered(chunk, 1, *right_indexes);
    }
    return done;
}

bool IEJoinAlgorithm::produceUnmatchedBatch(size_t side, Chunk & chunk)
{
    /// Rows that the pair scan did not mark; rows with NULL keys never entered the union,
    /// so their bits are never set and they are emitted here as well.
    auto & row_cursor = unmatched_row_cursor[side];
    auto indexes = ColumnUInt64::create();
    auto & data = indexes->getData();
    while (row_cursor < num_side_rows[side] && data.size() < max_block_size)
    {
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

    if (!join_state_built)
        buildJoinState();

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
    bool inputs_sorted_by_first_key,
    SharedHeaders & input_headers,
    SharedHeader output_header,
    size_t max_block_size,
    UInt64 limit_hint_)
    : IMergingTransform<IEJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        limit_hint_,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        kind,
        conditions,
        inputs_sorted_by_first_key,
        input_headers,
        max_block_size)
{
}

}
