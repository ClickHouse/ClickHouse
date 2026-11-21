#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

class FutureSetFromSubquery;
using FutureSetFromSubqueryPtr = std::shared_ptr<FutureSetFromSubquery>;

/// Reads a sealed view (see `StorageView::isSealed`). The view's plan is a child plan of this step rather
/// than its subtree, so the optimizations of the outer query cannot see through it; it is optimized on its own.
/// The plan runs with the privileges of the view's definer and may hold values folded from data the invoker
/// cannot read, such as a scalar subquery over a private table, so EXPLAIN shows it only if `show_plan` is set.
class ReadFromSealedViewStep final : public ISourceStep
{
public:
    ReadFromSealedViewStep(QueryPlan view_plan_, const ContextPtr & view_context, bool show_plan_);

    [[nodiscard]] String getName() const override { return "ReadFromSealedView"; }

    [[nodiscard]] QueryPlanStepPtr clone() const override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    QueryPlanRawPtrs getChildPlans(bool for_explain) override;

    /// `getBuiltChildPlans` is deliberately left at the base implementation, which reports no child plans.
    /// It is what the query plan captured into `system.query_log` walks, and that table is readable beyond
    /// the user who ran the query, so returning `view_plan` here would write a sealed view's internals into
    /// the log for readers that `show_plan` is meant to keep them from. The cost is that a sealed view is an
    /// opaque leaf of the captured plan.

    void describePipeline(FormatSettings & settings) const override;

    void optimize();

    /// Seals the plans of the sets taken out of the view's plan, which are built by the outer plan.
    void sealSets(const std::vector<FutureSetFromSubqueryPtr> & sets) const;

private:
    ReadFromSealedViewStep(QueryPlan view_plan_, QueryPlanOptimizationSettings optimization_settings_, bool show_plan_);

    QueryPlan view_plan;
    QueryPlanOptimizationSettings optimization_settings;
    bool show_plan;
    bool optimized = false;
};

}
