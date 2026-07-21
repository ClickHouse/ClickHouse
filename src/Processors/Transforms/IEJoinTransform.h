#pragma once

#include <array>
#include <optional>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Transforms/JoinResidualCondition.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/PODArray.h>

namespace DB
{

/// One inequality join condition `left.x op right.x`: the comparison operator and the
/// positions of the key columns in the input headers (not bound to column names).
struct IEJoinCondition
{
    JoinConditionOperator op = JoinConditionOperator::Unknown;
    size_t left_key_position = 0;
    size_t right_key_position = 0;

    /// `side` is 0 for the left input, 1 for the right one.
    size_t keyPosition(size_t side) const { return side == 0 ? left_key_position : right_key_position; }
};

/// The join types the IEJoin operator executes. There are no right-side SEMI/ANTI:
/// `IEJoinStep` executes them as the left-side mirror with swapped inputs and reversed operators.
enum class IEJoinKind : uint8_t
{
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    LeftAnti,
};

const char * toString(IEJoinKind kind);

/// The two conditions `left.x op1 right.x AND left.y op2 right.y` executed by the IEJoin
/// algorithm: the first condition defines the L1 order, the second the L2 order.
using IEJoinConditions = std::array<IEJoinCondition, 2>;

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
    IEJoinAlgorithm(
        IEJoinKind kind_,
        const IEJoinConditions & conditions_,
        std::optional<JoinResidualCondition> residual_,
        bool inputs_sorted_by_first_key_,
        const SharedHeaders & input_headers_,
        const SizeLimits & size_limits_,
        size_t max_block_size_);

    const char * getName() const override { return "IEJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override;

private:
    /// Stages of building the join state (the L1 union, the L2 permutation and the bit array),
    /// in execution order. One stage runs per merge() call, so that between the whole-input
    /// passes control returns to the executor, which observes cancellation.
    enum class BuildStage : uint8_t
    {
        MaterializeLeft,
        MaterializeRight,
        EncodeKeys,
        BuildL1,
        BuildL2,
        Done,
    };

    /// Run the stage `build_stage` points at and advance it.
    void runBuildStage();

    /// Rows admitted to the union: byte mask over the side's rows (empty when every row is
    /// valid) and the count of valid rows.
    struct SideValidity
    {
        IColumn::Filter mask;
        size_t num_valid = 0;
    };

    /// Concatenate the side's accumulated chunks into `side_columns`, prepare the key columns
    /// and allocate the matched bitmap if the side needs one. Returns the validity mask that
    /// excludes rows with NULL or NaN keys: they cannot match, never enter the union, and are
    /// emitted by the post-phases as unmatched.
    SideValidity materializeSide(size_t side);

    /// Encode both conditions' keys into fixed-width values in union-entry order
    /// ([left rows..., right rows...]) with the sort direction folded in. An empty array means
    /// the condition's type has no encoding and the generic comparator serves it instead;
    /// the two conditions decide independently.
    std::array<PaddedPODArray<UInt64>, 2> encodeKeys() const;

    /// Build L1 (`l1_entries`): the union of both sides ordered by the first
    /// condition so that any entry satisfies it with respect to all entries below.
    /// `encoded_keys` is the first condition's encoding (may be empty).
    void buildL1(const std::array<SideValidity, 2> & validity, const PaddedPODArray<UInt64> & encoded_keys);

    /// Build L2 (`permutation`, `l2_keys_by_position`): L1 positions ordered by the second
    /// condition so that any entry satisfies it with respect to all entries after it.
    /// `encoded_keys` is the second condition's encoding (may be empty); it is fully folded
    /// into `l2_keys_by_position` and freed before the sort, cutting the build-phase peak.
    void buildL2(const std::array<SideValidity, 2> & validity, PaddedPODArray<UInt64> & encoded_keys);

    /// Compare key values (key_index: 0 for L1 keys, 1 for L2 keys) of two union entries via the
    /// generic virtual comparator. This is the reference implementation: the encoded fixed-width
    /// fast path must reproduce its order exactly, all debug checks run against it, and it serves
    /// the types that have no encoding.
    int compareKeysAt(size_t key_index, size_t union_a, size_t union_b) const;

    /// Which side a union entry comes from (see `l1_entries`); the row within the side is the
    /// entry itself for the left one and `entry - num_side_rows[0]` for the right one.
    bool entryIsLeft(UInt64 entry) const { return entry < num_side_rows[0]; }

    /// Whether the frontier should advance past L2 entry `l2_from` while processing L2 entry `l2_current`,
    /// i.e. whether the value of `l2_from` satisfies the second condition with respect to `l2_current`.
    bool frontierAdvances(size_t l2_from, size_t l2_current) const;

    /// Advance the L2 cursor to the next left-side entry, updating the frontier and the bit array.
    /// Returns false when L2 is exhausted.
    bool nextLeftRow();

    /// Whether the side's unmatched rows are emitted in a post-phase;
    /// exactly these sides get a matched bitmap filled by the pair scan.
    bool sideNeedsUnmatchedRows(size_t side) const;

    /// Emit up to max_block_size rows: the pair scan first, then the unmatched rows of the
    /// sides that need them; sets produce_done when everything is emitted.
    Chunk produceBatch();
    /// Run the pair scan and fill up to max_block_size rows per the join kind, bounded also
    /// by the per-call work budget (see `produce_work_budget`). Returns true when the scan is
    /// exhausted; an empty chunk alone is not a completion signal.
    bool producePairsBatch(Chunk & chunk);
    /// Fill up to max_block_size rows of the side without a set bit in the matched bitmap,
    /// padding the other side; bounded also by the work budget. Returns true when all rows
    /// of the side were examined.
    bool produceUnmatchedBatch(size_t side, Chunk & chunk);

    /// Evaluate the residual over the pending candidate pairs, set the matched bits of the
    /// passing pairs and append them to the output row ids; the matched bits are deferred to
    /// this point. Clears the pending columns.
    void flushPendingPairs(
        ColumnUInt64 & pending_left, ColumnUInt64 & pending_right,
        ColumnUInt64::Container & left_out, ColumnUInt64::Container & right_out);
    /// Decide the current left row on its pending residual candidates: the first passing one
    /// decides it (SEMI appends the pair to the outputs, ANTI only marks). Returns whether the
    /// row is decided; an undecided row continues its scan (deciding on a prefix of the row's
    /// candidates is sound - scanning continues only when the whole prefix failed).
    /// Clears the pending columns.
    bool decideSemiAntiRow(
        ColumnUInt64 & pending_left, ColumnUInt64 & pending_right,
        ColumnUInt64::Container & left_out, ColumnUInt64::Container & right_out);

    /// Append the side's columns gathered by `indexes`.
    void appendGathered(Chunk & chunk, size_t side, const ColumnUInt64 & indexes) const;
    /// Append `num_rows` default values of the side's column types (NULL for a Nullable type).
    void appendPadded(Chunk & chunk, size_t side, size_t num_rows) const;

    /// Gather the residual condition's input columns for the candidate pairs (row i is the pair
    /// `(left_rows[i], right_rows[i])`), evaluate it, and fold the result into a byte mask
    /// (1 = the pair passes; a NULL result folds to 0). Charges the work budget per candidate.
    IColumn::Filter evaluateResidualMask(const ColumnUInt64 & left_rows, const ColumnUInt64 & right_rows);

    void setBit(size_t pos);
    bool testBit(size_t pos) const;
    /// Position of the first set bit at or after `scan_pos` (advancing `scan_pos` past it),
    /// or num_union_entries if there is none. Returns nothing when the work budget stops the
    /// scan first; `scan_pos` then points at the first uninspected position, so the next call
    /// resumes there.
    std::optional<size_t> findNextSetBit();

    /// Re-verify an emitted pair against both conditions by direct evaluation (debug builds).
    bool checkEmittedPair(size_t key_index, size_t left_row, size_t right_row) const;
    /// Check that the bit array is exactly {right-side entries whose L2 key qualifies
    /// against the current left entry} (debug builds, small inputs only).
    void checkFrontierInvariant() const;
    /// Check at the end of emission that every filtered row of each post-phase side ended up
    /// exactly one of matched/unmatched (debug builds).
    void checkFinalInvariants() const;
#ifndef NDEBUG
    /// With `inputs_sorted_by_first_key` the operator trusts the upstream order; check that an
    /// input chunk is ordered by the first condition's key, also against the previous chunk of
    /// the same input (debug builds; catches wiring bugs).
    void checkInputChunkOrder(const Chunk & chunk, size_t source_num);
#endif

    SharedHeaders input_headers;
    size_t max_block_size;

    IEJoinKind kind;
    IEJoinConditions conditions;
    /// Direction and strictness of each condition's sorted order, derived from its operator in
    /// the constructor. The directions differ by construction: L1 is descending for {>, >=}
    /// (a higher L1 position must satisfy the first condition against lower positions), L2 is
    /// descending for {<, <=} (an earlier L2 entry must satisfy the second condition against
    /// later entries); strict operators exclude equal keys.
    struct KeyOrder
    {
        bool descending = false;
        bool strict = false;
    };
    std::array<KeyOrder, 2> key_order;
    /// The residual ON condition gating candidate pairs, if any.
    std::optional<JoinResidualConditionEvaluator> residual;
    /// The inputs are each sorted by the first condition's key (ascending, NULLS LAST):
    /// selects the merge-based L1 build; with the flag off the operator orders the union
    /// itself with an index sort.
    bool inputs_sorted_by_first_key;

    /// Limits on the accumulated input (both sides), from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;
    /// The soft limit check failed: with `join_overflow_mode = 'break'` the rest of both
    /// inputs is dropped.
    bool size_limit_reached = false;

    /// Input chunks per side in arrival order (with `inputs_sorted_by_first_key` that order is
    /// ascending by the first condition's key); concatenated by `materializeSide`.
    std::array<Chunks, 2> accumulated_chunks;
    std::array<bool, 2> source_finished = {false, false};

#ifndef NDEBUG
    /// Key column of the last accumulated chunk per input, to check order across chunk boundaries.
    std::array<ColumnPtr, 2> last_input_key_column;
#endif

    /// Populated by the build stages (see `runBuildStage`):

    /// All columns of each side, rows in the side's input order; every per-side row index in
    /// this class (`matched`, the row within a union entry, emitted pairs) refers to this order.
    std::array<Columns, 2> side_columns;

    /// One byte per row of the side, set by the pair scan; allocated only for sides that need it
    std::array<IColumn::Filter, 2> matched;

    /// Both sides' key column of one condition, prepared for comparisons
    /// (from `side_columns`, with `LowCardinality` stripped).
    struct ConditionKeyColumns
    {
        ColumnPtr left;
        ColumnPtr right;

        ColumnPtr & bySide(size_t side) { return side == 0 ? left : right; }
        const ColumnPtr & bySide(size_t side) const { return side == 0 ? left : right; }
    };

    /// Prepared key columns, one entry per condition.
    std::array<ConditionKeyColumns, 2> key_columns;
    /// Rows per side in `side_columns`, including the NULL/NaN-keyed rows that stay out of the union.
    std::array<size_t, 2> num_side_rows = {0, 0};
    /// Number of union entries: rows of both sides minus the rows with NULL/NaN keys.
    size_t num_union_entries = 0;

    /// Union entry at each L1 position: this array IS the L1 order - positions ascend in the
    /// first condition's direction, equal keys split by origin side (the scan-boundary tie
    /// policy). Entry u is left row u if u < num_side_rows[0], otherwise right row
    /// u - num_side_rows[0] (see `entryIsLeft`).
    PaddedPODArray<UInt64> l1_entries;
    /// L1 position of each L2 entry (the permutation array P).
    IColumn::Permutation permutation;
    /// Second-condition keys encoded into fixed-width values whose unsigned order is the L2 order,
    /// indexed by L1 position (the frontier reaches them through the `permutation` entry it
    /// loads for bit-marking anyway); empty when the condition's type has no encoding
    /// (the frontier then falls back to `compareAt`).
    PaddedPODArray<UInt64> l2_keys_by_position;
    /// One bit per L1 position, set for right-side entries that passed the frontier.
    PaddedPODArray<UInt64> bit_array;
    /// One past the highest set bit; lets scans stop instead of walking empty words to the end.
    size_t bit_array_end = 0;

    BuildStage build_stage = BuildStage::MaterializeLeft;
    /// Intermediate build products handed between the stages; freed when the build completes.
    std::array<SideValidity, 2> build_validity;
    /// Encoded keys per condition, indexed by union entry ([left rows..., right rows...],
    /// direction folded in - see `encodeKeys`).
    std::array<PaddedPODArray<UInt64>, 2> build_encoded_keys;

    /// Pair-scan state; all of it is resumable, so a call may stop at any point (block full,
    /// work budget exhausted) and the next call continues exactly where it stopped.
    bool produce_done = false;
    /// L2 position whose entry the scan currently processes.
    size_t l2_cursor = 0;
    /// Number of L2 entries folded into the bit array; the qualifying entries form an L2 prefix
    /// that only grows as the cursor advances, so the frontier never re-visits an entry.
    size_t frontier = 0;
    /// L1 position where the search for the current left row's next match resumes.
    size_t scan_pos = 0;
    /// The left row being scanned (0-based row of the left side), valid iff `has_current_left`.
    size_t current_left_row = 0;
    bool has_current_left = false;

    /// Bound on the work of one produceBatch call - cursor advances, bit-array words inspected,
    /// residual candidates evaluated, unmatched rows examined - so that control regularly
    /// returns to the executor, which observes cancellation.
    static constexpr size_t produce_work_budget = 1 << 20;
    /// Work spent by the current produceBatch call (reset at the start of its pair-scan phase).
    size_t produce_work = 0;
    /// Bound on the candidates of one left row accumulated before a residual evaluation in the
    /// SEMI/ANTI scan: the first passing candidate decides the row, so small batches restore
    /// the first-match short-circuit at batch granularity.
    static constexpr size_t semi_anti_residual_batch_size = 1024;

    /// Post-phase state, per side: the row cursor and the number of unmatched rows emitted
    /// (for the final invariant check).
    std::array<size_t, 2> unmatched_row_cursor = {0, 0};
    std::array<size_t, 2> unmatched_emitted = {0, 0};

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
        IEJoinKind kind,
        const IEJoinConditions & conditions,
        std::optional<JoinResidualCondition> residual,
        bool inputs_sorted_by_first_key,
        SharedHeaders & input_headers,
        SharedHeader output_header,
        const SizeLimits & size_limits,
        size_t max_block_size);

    String getName() const override { return "IEJoinTransform"; }
};

}
