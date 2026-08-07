#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>

#include <optional>

namespace DB
{

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
        size_t max_block_bytes_);

    String getName() const override { return "BlockNestedLoopProbe"; }

    Status prepare() override;
    void work() override;

private:
    BlockNestedLoopJoinDataPtr data;
    BlockNestedLoopPredicate predicate;
    /// Limits on one output chunk; the walk over the build side yields as soon as either is reached.
    [[maybe_unused]] size_t max_block_size;
    [[maybe_unused]] size_t max_block_bytes;

    Chunk input_chunk;
    std::optional<Chunk> output_chunk;
    bool has_input = false;
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
