#pragma once

#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>
#include <Processors/Transforms/JoiningTransform.h>

#include <deque>
#include <optional>

namespace DB
{

/// Fills `BlockNestedLoopJoinData` with the build side. Carries no data downstream: its output port
/// has an empty header and is finished once the whole build side is stored, which is how the probe
/// side learns that it may start.
class BlockNestedLoopBuildTransform final : public IProcessor
{
public:
    BlockNestedLoopBuildTransform(
        SharedHeader input_header, BlockNestedLoopJoinDataPtr data_, FinishCounterPtr finish_counter_, size_t stream_index_);

    String getName() const override { return "BlockNestedLoopBuild"; }

    /// Routes the build side's `WITH TOTALS` row into the store. Only one build stream may carry
    /// it, so a pipeline with build-side totals uses a single build stream.
    InputPort * addTotalsPort();

    Status prepare() override;
    void work() override;

    ProcessorMemoryStats getMemoryStats() override;
    bool spillOnSize(size_t bytes) override;

private:
    /// Counts this stream out of the build phase, closing the store when it is the last one.
    void finishBuild();

    BlockNestedLoopJoinDataPtr data;
    FinishCounterPtr finish_counter;
    /// This stream's place among the build streams, and so the temporary file it spills to.
    const size_t stream_index;
    Chunk chunk;
    bool stop_reading = false;
    bool for_totals = false;
    /// Closing the store is asked for by `prepare` and done by `work`, which is where the work of
    /// this stream belongs; `build_finished` is what makes `prepare` ask for it exactly once.
    bool finish_build_requested = false;
    bool build_finished = false;
};

/// An arbitrary `JOIN ON` condition evaluated on (left row, right row) pairs inside the operator.
struct BlockNestedLoopPredicate
{
    /// Where a required column of `actions` comes from: the input (0 = left, 1 = right)
    /// and the column's position in that input's header.
    struct Source
    {
        size_t side = 0;
        size_t position = 0;
    };

    ExpressionActionsPtr actions;
    /// One entry per required column of `actions`, in `getRequiredColumnsWithTypes` order.
    std::vector<Source> inputs;
};

/// Matches every probe (left) row against the materialized build side by evaluating the join
/// condition on tiles of candidate pairs. Runs only after the build phase is over, which the
/// pipeline guarantees by holding the probe streams back until the build streams finish.
class BlockNestedLoopProbeTransform final : public IProcessor
{
public:
    BlockNestedLoopProbeTransform(
        SharedHeader probe_header_,
        SharedHeader output_header_,
        BlockNestedLoopJoinDataPtr data_,
        BlockNestedLoopPredicate predicate_,
        size_t max_block_size_,
        size_t max_block_bytes_,
        JoinAnalyzeMode analyze_mode_ = JoinAnalyzeMode::None,
        /// How many probe streams the step runs, which decides how much of the build side this one
        /// may keep alive for pairs it has not emitted yet.
        size_t num_probe_streams_ = 1);

    String getName() const override { return "BlockNestedLoopProbe"; }

    Status prepare() override;
    void work() override;

    /// What this stream contributes to the probe side of the step's `EXPLAIN ANALYZE` report: the
    /// rows it pulled, and how many of them matched at least one build row - `nullopt` where the walk
    /// does not record that, which `matches = 1` is what turns on.
    UInt64 getProbeRows() const { return total_probe_rows; }
    std::optional<UInt64> getMatchedProbeRows() const;
    /// The store every probe stream of the step shares, and where the build side's own numbers are.
    const BlockNestedLoopJoinData & getJoinData() const { return *data; }

private:
    /// A maximal group of accumulated pairs that share a stored build block. An output chunk can
    /// span several blocks, and the build side of each group is gathered from its own block, which
    /// the run keeps alive - the walk may have moved on, and a compressed or spilled block would
    /// otherwise have to be read back a second time.
    struct BuildRun
    {
        size_t block_index;
        size_t length;
        BuildBlockPtr block;
        /// What holding the block costs, counted only for a block the reader had to materialize.
        size_t retained_bytes = 0;
    };

    /// Everything the walk over one probe chunk holds. Grouped so that starting a chunk and
    /// abandoning one both reset it whole, and no field can be left over from the chunk before.
    struct Walk
    {
        /// The probe chunk being walked.
        Columns probe_columns;
        size_t probe_num_rows = 0;
        bool has_probe_chunk = false;

        /// The walk goes over tiles of a bounded number of pairs: a window of the probe rows still
        /// in the walk against a window of the rows of one stored block.
        /// The probe rows still in the walk, in increasing order; every one of them under `ALL`.
        PaddedPODArray<UInt64> active_probe_rows;
        /// Where the probe window starts in `active_probe_rows`. Back to zero once the sweep over
        /// them against the current build rows is complete, which is the only point at which those
        /// advance.
        size_t probe_window_cursor = 0;
        size_t build_block_cursor = 0;
        size_t build_row_cursor = 0;
        /// The block at `build_block_cursor`, held for as long as the walk stays inside it.
        BuildBlockPtr current_build_block;

        /// The pairs that satisfied the condition and are not emitted yet: a row index within the
        /// probe chunk, and a row index within the stored block named by the matching entry of
        /// `build_runs`.
        PaddedPODArray<UInt64> matched_probe_rows;
        PaddedPODArray<UInt64> matched_build_rows;
        /// Where the pairs still to be emitted start. One tile accumulates far more pairs than one
        /// output chunk may hold, and dropping the emitted prefix on every chunk would move the rest
        /// of them each time; the prefix is dropped in one go instead, once it is the larger part.
        size_t matched_rows_offset = 0;
        std::deque<BuildRun> build_runs;
        /// What the blocks held by `build_runs` cost on top of the store, i.e. the sum over the runs
        /// whose block the reader had to decompress or read back from disk.
        size_t retained_build_bytes = 0;

        /// Rough size of one output row, used only to decide when `max_block_bytes` is reached.
        size_t probe_row_bytes = 0;
        size_t build_row_bytes = 0;

        /// Set for every probe row that matched at least one build row. Empty for the kinds whose
        /// result does not depend on it.
        IColumnFilter probe_row_matched;
        size_t unmatched_probe_cursor = 0;
    };

    void startProbeChunk(Chunk chunk);
    /// Evaluates the condition on the next tile of candidate pairs, accumulates the surviving ones
    /// and advances the build cursor. Returns the number of pairs evaluated.
    size_t matchNextTile();
    /// Keeps the pairs of the current tile that the strictness selects, and records the match on
    /// the probe row and on the build row where the kind needs it.
    void appendMatchedPairs(const IColumn & matched_probe, const IColumn & matched_build, size_t num_matched);
    /// Extends the last run of accumulated pairs, or opens one for the current build block.
    void addBuildRun(size_t length);
    /// Removes the probe rows that have matched from the walk over the rest of the build side.
    void dropMatchedProbeRows();
    /// How many accumulated pairs are still waiting to be emitted.
    size_t numPendingPairs() const { return walk.matched_probe_rows.size() - walk.matched_rows_offset; }
    /// Whether the accumulated pairs already fill an output chunk.
    bool hasFullOutputChunk() const;
    /// How many rows one output chunk may hold under both limits, for rows of `row_bytes` each.
    size_t maxOutputChunkRows(size_t row_bytes) const;
    /// Materializes that many accumulated pairs, keeping the rest for the next call.
    Chunk takeMatchedRows();
    /// Reclaims the space the already emitted pairs take, once they are the larger part of it.
    void dropEmittedPairs();
    /// Emits the next window of probe rows that stayed unmatched, padded with build-side defaults.
    Chunk takeUnmatchedProbeRows();
    /// Drops everything the walk over the build side holds. Called when nothing more will be
    /// matched or emitted, which is not the same point as this processor's destruction.
    void releaseProbeState();

    const SharedHeader probe_header;
    const SharedHeader output_header;
    BlockNestedLoopJoinDataPtr data;
    /// This stream's own way into the stored build blocks; the walk over them is what it reads back.
    BuildSideBlockReader build_reader;
    BlockNestedLoopPredicate predicate;
    /// The fixed input structure of `predicate.actions`, and its precomputed input mapping.
    Block predicate_input_header;
    std::vector<ssize_t> predicate_input_positions;

    /// Limits on one output chunk; the walk over the build side yields as soon as either is reached.
    const size_t max_block_size;
    const size_t max_block_bytes;
    /// What this stream may keep alive of a build side it had to materialize, which is the other
    /// reason to cut an output chunk short.
    const size_t max_retained_build_bytes;

    /// What the kind and strictness decide about which pairs and which unmatched rows are emitted.
    const BlockNestedLoopJoinRules rules;
    /// Whether the walk records which probe rows have matched.
    const bool track_probe_row_match;

    /// The probe side's participation, summed over the chunks this stream has walked.
    UInt64 total_probe_rows = 0;
    UInt64 total_matched_probe_rows = 0;

    /// The chunk pulled by `prepare` and not started yet: setting up a probe chunk is work over all
    /// of its rows, and `prepare` runs under the executor's node lock and is not timed as this
    /// processor's own time, so it happens in `work`.
    std::optional<Chunk> pending_probe_chunk;

    Walk walk;

    std::optional<Chunk> output_chunk;
};

/// Emits the build rows that no probe row matched, padded with the probe side's column defaults -
/// the `RIGHT`/`FULL` half of the result. Runs only after every probe stream has finished, which is
/// what makes the match flags readable; the stored blocks are dealt out over the streams so that
/// several of these scan the build side in parallel without overlapping.
class BlockNestedLoopUnmatchedBuildRowsTransform final : public ISource
{
public:
    BlockNestedLoopUnmatchedBuildRowsTransform(
        SharedHeader output_header_,
        BlockNestedLoopJoinDataPtr data_,
        size_t max_block_size_,
        size_t max_block_bytes_,
        size_t stream_index_,
        size_t num_streams_);

    String getName() const override { return "BlockNestedLoopUnmatchedBuildRows"; }

    Status prepare() override;

protected:
    Chunk generate() override;

private:
    BlockNestedLoopJoinDataPtr data;
    BuildSideBlockReader build_reader;
    /// The columns of the output header that belong to the probe side and are padded here.
    const size_t num_probe_columns;
    const size_t max_block_size;
    const size_t max_block_bytes;
    /// This stream owns the blocks `stream_index, stream_index + num_streams, ...`, unless the build
    /// side spilled - then stream 0 walks all of them and the others have nothing to do.
    const size_t stream_index;
    size_t num_streams;
    size_t block_cursor;
    size_t row_cursor = 0;
    bool scan_partitioned = false;
};

/// Produces the joined `WITH TOTALS` row: the probe side's totals row extended with the build
/// side's totals row, or with defaults where the build side has no totals of its own. The totals
/// rows never take part in matching, exactly as in `JoinCommon::joinTotals`.
class BlockNestedLoopTotalsTransform final : public ISimpleTransform
{
public:
    BlockNestedLoopTotalsTransform(
        SharedHeader probe_header_,
        SharedHeader output_header_,
        BlockNestedLoopJoinDataPtr data_,
        bool probe_totals_are_default_);

    String getName() const override { return "BlockNestedLoopTotals"; }

    void transform(Chunk & chunk) override;

private:
    BlockNestedLoopJoinDataPtr data;
    /// The probe side had no totals of its own; the row was synthesized only to carry the build
    /// side's totals. With no build totals either there is nothing to report, so the row is dropped.
    bool probe_totals_are_default;
};

}
