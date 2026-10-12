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

    String getName() const override { return "ReadFromSealedView"; }

    QueryPlanStepPtr clone() const override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    QueryPlanRawPtrs getChildPlans(bool for_explain) override;

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
