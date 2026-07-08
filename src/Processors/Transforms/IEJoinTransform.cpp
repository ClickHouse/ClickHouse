#include <algorithm>
#include <bit>

#include <base/defines.h>
#include <base/sort.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Processors/Transforms/IEJoinTransform.h>
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

IEJoinAlgorithm::IEJoinAlgorithm(const IEJoinConditions & conditions_, const SharedHeaders & input_headers_, size_t max_block_size_)
    : input_headers(input_headers_)
    , max_block_size(std::max<size_t>(1, max_block_size_))
    , conditions(conditions_)
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoinAlgorithm requires exactly two inputs");

    for (const auto & condition : conditions.conditions)
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
        if (conditions.inputs_sorted_by_first_key)
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

        std::array<ColumnPtr, 2> comparison_keys;
        for (size_t key_index = 0; key_index < 2; ++key_index)
            comparison_keys[key_index] = columns[conditions[key_index].keyPosition(side)]->convertToFullColumnIfLowCardinality();

        /// Exclude rows with NULL in any key from the union entirely: a NULL fails every inequality,
        /// so such rows cannot produce matches (the join is INNER).
        IColumn::Filter non_null_filter;
        for (const auto & key : comparison_keys)
        {
            const auto * nullable = checkAndGetColumn<ColumnNullable>(key.get());
            if (!nullable)
                continue;

            const auto & null_map = nullable->getNullMapData();
            if (non_null_filter.empty())
                non_null_filter.resize_fill(rows, 1);
            for (size_t row = 0; row < rows; ++row)
                non_null_filter[row] &= !null_map[row];
        }

        if (!non_null_filter.empty() && countBytesInFilter(non_null_filter) != rows)
        {
            for (auto & column : columns)
                column = column->filter(non_null_filter, -1);
            for (auto & key : comparison_keys)
                key = key->filter(non_null_filter, -1);
        }

        side_columns[side] = std::move(columns);
        for (size_t key_index = 0; key_index < 2; ++key_index)
            key_columns[key_index].bySide(side) = std::move(comparison_keys[key_index]);
        num_side_rows[side] = side_columns[side].empty() ? 0 : side_columns[side].front()->size();
    }

    n_union = num_side_rows[0] + num_side_rows[1];

    const auto op1 = conditions[0].op;
    const bool l1_descending = op1 == JoinConditionOperator::Greater || op1 == JoinConditionOperator::GreaterOrEquals;
    const bool op1_strict = op1 == JoinConditionOperator::Less || op1 == JoinConditionOperator::Greater;

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
    if (conditions.inputs_sorted_by_first_key)
    {
        /// Each input arrives pre-sorted ascending by its first-condition key (NULL-key rows were
        /// filtered above, which preserves order), so L1 is a two-way merge. A descending L1
        /// iterates both materialized sides backwards: the direction is just a view over the
        /// sorted input. The tie policy is stated in terms of final L1 positions, so it is
        /// independent of the iteration direction; the order within a run of equal keys on one
        /// side is irrelevant.
        const size_t num_left = num_side_rows[0];
        const size_t num_right = num_side_rows[1];
        auto left_entry = [&](size_t index) -> UInt64 { return l1_descending ? num_left - 1 - index : index; };
        auto right_entry = [&](size_t index) -> UInt64 { return num_left + (l1_descending ? num_right - 1 - index : index); };

        size_t out = 0;
        size_t left_taken = 0;
        size_t right_taken = 0;
        while (left_taken < num_left && right_taken < num_right)
        {
            int cmp = compareKeysAt(0, left_entry(left_taken), right_entry(right_taken));
            if (l1_descending)
                cmp = -cmp;
            /// On equal keys a loose condition pulls the left entry first, a strict one the right.
            bool take_left = cmp != 0 ? cmp < 0 : !op1_strict;
            if (take_left)
                l1_union[out++] = left_entry(left_taken++);
            else
                l1_union[out++] = right_entry(right_taken++);
        }
        while (left_taken < num_left)
            l1_union[out++] = left_entry(left_taken++);
        while (right_taken < num_right)
            l1_union[out++] = right_entry(right_taken++);
        chassert(out == n_union);

#ifndef NDEBUG
        /// The merge must produce exactly an order the comparator sort would accept.
        for (size_t pos = 1; pos < n_union; ++pos)
            chassert(!l1_order_less(l1_union[pos], l1_union[pos - 1]));
#endif
    }
    else
    {
        iota(l1_union.data(), n_union, UInt64(0));
        ::sort(l1_union.begin(), l1_union.end(), l1_order_less);
    }

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

    const auto op2 = conditions[1].op;
    const bool l2_descending = op2 == JoinConditionOperator::Less || op2 == JoinConditionOperator::LessOrEquals;

    /// L2: L1 positions ordered by the second condition's keys, so that every entry satisfies
    /// the condition with respect to all entries after it. No tie-break is needed: the frontier
    /// advances by comparing values, so the order within a run of equal keys is irrelevant.
    permutation.resize(n_union);
    iota(permutation.data(), n_union, UInt64(0));
    ::sort(permutation.begin(), permutation.end(), [&](UInt64 pos_a, UInt64 pos_b)
    {
        int cmp = compareKeysAt(1, l1_union[pos_a], l1_union[pos_b]);
        return l2_descending ? cmp > 0 : cmp < 0;
    });

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

        while (frontier < n_union && frontierAdvances(frontier, l2_cursor))
        {
            UInt64 frontier_pos = permutation[frontier];
            /// Mark right-side entries only, so that a row can never match same-side rows.
            if (li[frontier_pos] < 0)
                setBit(frontier_pos);
            ++frontier;
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
    auto left_indexes = ColumnUInt64::create();
    auto right_indexes = ColumnUInt64::create();
    auto & left_data = left_indexes->getData();
    auto & right_data = right_indexes->getData();

    while (left_data.size() < max_block_size)
    {
        if (!has_current_left)
        {
            if (!nextLeftRow())
            {
                produce_done = true;
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

        /// Every emitted pair must satisfy both conditions, re-verify by direct evaluation.
        chassert(checkEmittedPair(0, left_row, right_row));
        chassert(checkEmittedPair(1, left_row, right_row));

        left_data.push_back(left_row);
        right_data.push_back(right_row);
    }

    if (left_data.empty())
        return {};

    Chunk result;
    for (const auto & column : side_columns[0])
        result.addColumn(column->index(*left_indexes, 0));
    for (const auto & column : side_columns[1])
        result.addColumn(column->index(*right_indexes, 0));
    return result;
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
    /// Materialize the left input entirely, then the right one.
    if (!source_finished[0])
        return Status(0);
    if (!source_finished[1])
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
    const IEJoinConditions & conditions,
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
        conditions,
        input_headers,
        max_block_size)
{
}

}
