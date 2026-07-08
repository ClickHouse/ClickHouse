#pragma once

#include <array>
#include <memory>

#include <Core/Block_fwd.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <Common/PODArray.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/*
 * Joins two fully materialized streams by two inequality conditions
 * `left.x op1 right.x AND left.y op2 right.y` with the IEJoin algorithm
 * (Khayyat et al., "Lightning Fast and Space Efficient Inequality Joins", PVLDB 8(13), 2015).
 *
 * Both inputs are accumulated entirely, then a union of both sides is sorted twice:
 * L1 by the first condition's keys (so that a higher L1 position implies the first condition holds
 * with respect to any lower position) and L2 by the second condition's keys. A single pass over L2
 * with a monotone frontier and a bit array over L1 positions produces all result pairs:
 * the first condition is answered by position in L1, the second by membership in the bit array,
 * so no join predicate is ever evaluated per candidate pair.
 */
class IEJoinAlgorithm final : public IMergingAlgorithm
{
public:
    IEJoinAlgorithm(JoinPtr table_join_, const SharedHeaders & input_headers_, size_t max_block_size_);

    const char * getName() const override { return "IEJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override;

private:
    /// Materialize both sides, drop rows with NULL keys, sort the union, build Li/P and the bit array.
    void buildJoinState();

    /// Compare key values (key_index: 0 for L1 keys, 1 for L2 keys) of two union entries.
    int compareKeysAt(size_t key_index, size_t union_a, size_t union_b) const;

    /// Whether the frontier should advance past L2 entry `l2_from` while processing L2 entry `l2_current`,
    /// i.e. whether the value of `l2_from` satisfies the second condition with respect to `l2_current`.
    bool frontierAdvances(size_t l2_from, size_t l2_current) const;

    /// Advance the L2 cursor to the next left-side entry, updating the frontier and the bit array.
    /// Returns false when L2 is exhausted.
    bool nextLeftRow();

    /// Emit up to max_block_size result pairs. Resumable: sets produce_done when everything is emitted.
    Chunk produceBatch();

    void setBit(size_t pos);
    bool testBit(size_t pos) const;
    /// Position of the first set bit >= from, or n_union if there is none.
    size_t findNextSetBit(size_t from) const;

    /// Re-verify an emitted pair against both conditions by direct evaluation (debug builds).
    bool checkEmittedPair(size_t key_index, size_t left_row, size_t right_row) const;
    /// Check that the bit array is exactly {right-side entries whose L2 key qualifies
    /// against the current left entry} (debug builds, small inputs only).
    void checkFrontierInvariant() const;

    SharedHeaders input_headers;
    size_t max_block_size;

    std::array<JoinConditionOperator, 2> operators;
    /// Positions of the key columns in the input headers: [side][key_index].
    std::array<std::array<size_t, 2>, 2> key_positions;

    std::array<Chunks, 2> accumulated_chunks;
    std::array<bool, 2> source_finished = {false, false};

    /// Populated by buildJoinState:

    /// All columns of each side with NULL-key rows removed. Result rows are gathered from them.
    std::array<Columns, 2> side_columns;
    /// Key columns of side_columns prepared for comparisons: [side][key_index].
    std::array<std::array<ColumnPtr, 2>, 2> key_columns;
    std::array<size_t, 2> num_side_rows = {0, 0};
    size_t n_union = 0;

    /// Union entry at each L1 position. Entry u is left row u if u < num_side_rows[0],
    /// otherwise right row u - num_side_rows[0].
    PaddedPODArray<UInt64> l1_union;
    /// Signed 1-based row ids per L1 position: +k for the k-th left row, -k for the k-th right row.
    PaddedPODArray<Int64> li;
    /// L1 position of each L2 entry (the permutation array P).
    PaddedPODArray<UInt64> permutation;
    /// Union entry of each L2 entry (== l1_union[permutation[i]], denormalized for key comparisons).
    PaddedPODArray<UInt64> l2_union;
    /// One bit per L1 position, set for right-side entries that passed the frontier.
    PaddedPODArray<UInt64> bit_array;
    /// One past the highest set bit; lets scans stop instead of walking empty words to the end.
    size_t bit_array_end = 0;

    /// Merge loop state.
    bool join_state_built = false;
    bool produce_done = false;
    size_t l2_cursor = 0;
    size_t frontier = 0;
    size_t scan_pos = 0;
    Int64 current_left_rid = 0;
    bool has_current_left = false;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};
        size_t num_bytes[2] = {0, 0};
    };
    Statistic stat;
};

class IEJoinTransform final : public IMergingTransform<IEJoinAlgorithm>
{
    using Base = IMergingTransform<IEJoinAlgorithm>;

public:
    IEJoinTransform(
        JoinPtr table_join,
        SharedHeaders & input_headers,
        SharedHeader output_header,
        size_t max_block_size,
        UInt64 limit_hint_ = 0);

    String getName() const override { return "IEJoinTransform"; }
};

}
