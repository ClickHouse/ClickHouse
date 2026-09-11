#pragma once

#include "config.h"

#if USE_GPU

#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Aggregation done on a GPU: the `sum`s of a query, computed over batches of the argument columns
/// by cuDF's reduction without `GROUP BY` and by its groupby with one.
///
/// A narrow replacement for `AggregatingStep` rather than an extension of it. The planner puts
/// this step in place of that one when every part of the aggregation is one the device can do, and
/// leaves the CPU path untouched otherwise, so the setting behind it never changes a query's
/// result - only where the result is computed. `canRunOnDevice` says what "can" means.
///
/// Two consequences of being a step of its own rather than a mode of `AggregatingStep`, and both
/// are why the planner only reaches for it where the aggregation is the whole of the query's:
/// the plan optimizations that look for an `AggregatingStep` do not fire - the aggregate
/// projections among them - and the step has no serialization, so a plan carrying it cannot be
/// sent to a replica.
class GPUAggregatingStep : public ITransformingStep
{
public:
    GPUAggregatingStep(const SharedHeader & input_header_, Aggregator::Params params_, size_t batch_bytes_);

    String getName() const override { return "GPUAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

    /// Whether the aggregation `params` describe over an input of `input_header` is one the device
    /// can do, as far as the aggregate functions, the keys and their types go. What else has to
    /// hold - that this is the query's final aggregation, over a single local `MergeTree` table,
    /// with no `ROLLUP`, `CUBE`, `GROUPING SETS`, `WITH TOTALS` or `group_by_use_nulls` - is about
    /// the query rather than about this step, and the planner checks it.
    static bool canRunOnDevice(const Block & input_header, const Aggregator::Params & params);

    /// What this step aggregates, for a plan pass that has to know - as
    /// `optimizeAggregationFromGPUResidentColumns` does, which matches the aggregates by name and
    /// their arguments by type.
    const Aggregator::Params & getParams() const { return params; }

private:
    void updateOutputHeader() override;

    Aggregator::Params params;

    /// How much of a column to gather in host memory before sending it over - see
    /// `GPU::SumAccumulator`, which explains why a block at a time would be the wrong size.
    size_t batch_bytes;
};

}

#endif
