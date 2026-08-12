#pragma once

#include <Interpreters/MaterializedCTE.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{


class MaterializingCTEStep : public ITransformingStep
{
public:
    explicit MaterializingCTEStep(
        SharedHeader input_header_,
        MaterializedCTEWeakPtr materialized_cte_
    );

    String getName() const override { return "MaterializingCTE"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:

    void updateOutputHeader() override {} // Output header should stay empty.

    /// Weak on purpose: this step is appended to `materialized_cte->plan`, so a strong handle
    /// would make the CTE keep itself alive with no external holder - and a query dying before
    /// `resolveMaterializingCTEs` claims the plan would leak the graph, and the table references
    /// it carries, for the life of the process. `StorageMemory::materialized_cte` is weak for
    /// the same reason.
    MaterializedCTEWeakPtr materialized_cte;
};


class MaterializingCTEsStep : public IQueryPlanStep
{
public:
    MaterializingCTEsStep(SharedHeaders input_headers_, std::vector<MaterializedCTEPtr> ctes_);

    String getName() const override { return "MaterializingCTEs"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }

    /// This step replaces `DelayedMaterializingCTEsStep` once the CTE plans have been claimed
    /// and hung below it, so it takes over that step's role as the owner keeping the CTEs alive
    /// until the pipeline is built. Holds exactly the CTEs whose plans hang below this node -
    /// a CTE claimed by someone else is owned by whoever attached its plan.
    std::vector<MaterializedCTEPtr> ctes;
};


/// Stores pre-built CTE plans and materializes them lazily during query plan
/// optimization. This is the analogue of `DelayedCreatingSetsStep` for materialized CTEs.
///
/// The step cannot build a pipeline directly and must be converted to
/// `MaterializingCTEsStep` during the `addStepsToBuildSets` optimization pass.
///
/// Using a delayed step (rather than eagerly inserting `MaterializingCTEsStep`)
/// lets `addPlansForMaterializingCTEs` skip CTEs whose `is_planned` flag is
/// already set — which happens when `buildOrderedSetInplace` already executed the
/// CTE as part of primary-key index analysis before the main plan runs.
class DelayedMaterializingCTEsStep final : public IQueryPlanStep
{
public:

    DelayedMaterializingCTEsStep(SharedHeader input_header, std::vector<MaterializedCTEPtr> ctes_);

    String getName() const override { return "DelayedMaterializingCTEs"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &) override;

    /// A CTE claimed by `makePlansForCTEs`: the pre-built plan to hang below the
    /// `MaterializingCTEsStep` that replaces this step, paired with the strong handle
    /// that keeps the CTE alive for as long as that plan exists. The pairing is what
    /// lets the replacing step own exactly the CTEs it materializes - the plan itself
    /// refers back to the CTE only weakly, see `MaterializingCTEStep::materialized_cte`.
    struct ClaimedCTE
    {
        MaterializedCTEPtr cte;
        std::unique_ptr<QueryPlan> plan;
    };

    /// Returns the subset of pre-built CTE plans that still need to be executed,
    /// atomically marking each as materialized. CTEs already marked are skipped.
    /// The plans must have already been optimized via `optimizePlans` in the
    /// first traversal of `resolveMaterializingCTEs`.
    static std::vector<ClaimedCTE> makePlansForCTEs(DelayedMaterializingCTEsStep && step);

    /// Optimize each owned CTE's pre-built plan. Called by
    /// `resolveMaterializingCTEs`'s first traversal; the matching second
    /// traversal then calls `makePlansForCTEs` to claim and attach.
    /// Safe to call even after `makePlansForCTEs` has moved a CTE's plan
    /// out — the per-CTE check `if (cte->plan)` makes the call a no-op
    /// for CTEs whose plan has already been claimed (which happens when a
    /// recursive `buildSetInplace` claims the same CTE first).
    void optimizePlans(const QueryPlanOptimizationSettings & optimization_settings);

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }

    std::vector<MaterializedCTEPtr> ctes;
};

/// Strip every `DelayedMaterializingCTEsStep` node from `plan`'s tree, at
/// any depth. Called from `DelayedCreatingSetsStep::makePlansForSets` when
/// attaching a pre-built IN-subquery plan for runtime set construction; the
/// strip forces the outer query plan to win `is_materialization_planned`
/// for every referenced CTE so the outer `MaterializingCTEsStep`'s
/// `DelayedPortsProcessor` becomes the single point that gates every
/// reader. Nested `DelayedCreatingSetsStep` source plans (held in
/// `subqueries`, not in the immediate node tree) are not touched.
void removeAllDelayedMaterializingCTEsStep(QueryPlan & plan);

}
