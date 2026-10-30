#include <Processors/QueryPlan/ReadFromSealedViewStep.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <ranges>

namespace DB
{

ReadFromSealedViewStep::ReadFromSealedViewStep(QueryPlan view_plan_, const ContextPtr & view_context, bool show_plan_)
    : ISourceStep(view_plan_.getCurrentHeader())
    , view_plan(std::move(view_plan_))
    , optimization_settings(view_context)
    , show_plan(show_plan_)
{
    /// The view's plan becomes a part of the outer pipeline, so parallel replicas, which ship the fragment
    /// of the outer plan around the read, cannot apply. A distributed plan is decided for the view's plan
    /// on its own, as for a child plan of `ReadFromMerge`: the outer plan cannot be distributed, because
    /// this step is not serializable, but the view's plan still can.
    optimization_settings.enable_parallel_replicas = false;
    view_plan.applyDistributedPlanFallbackToLocal(optimization_settings);
}

ReadFromSealedViewStep::ReadFromSealedViewStep(QueryPlan view_plan_, QueryPlanOptimizationSettings optimization_settings_, bool show_plan_)
    : ISourceStep(view_plan_.getCurrentHeader())
    , view_plan(std::move(view_plan_))
    , optimization_settings(std::move(optimization_settings_))
    , show_plan(show_plan_)
{
}

QueryPlanStepPtr ReadFromSealedViewStep::clone() const
{
    auto step = std::unique_ptr<ReadFromSealedViewStep>(new ReadFromSealedViewStep(view_plan.clone(), optimization_settings, show_plan));
    step->optimized = optimized;
    step->setStepDescription(*this);
    return step;
}

void ReadFromSealedViewStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline = std::move(*view_plan.buildQueryPipeline(optimization_settings, settings, /*do_optimize=*/ false));

    /// The processors name the steps they come from and carry their descriptions, which show up in
    /// `EXPLAIN PIPELINE graph = 1` and in `system.processors_profile_log`. Attribute them to this step instead.
    if (!show_plan)
    {
        for (const auto & processor : pipeline.getProcessors())
        {
            processor->setQueryPlanStep(this);
            processor->setDescription({});
        }
    }
}

QueryPlanRawPtrs ReadFromSealedViewStep::getChildPlans(bool for_explain)
{
    if (for_explain && !show_plan)
        return {};
    return {&view_plan};
}

void ReadFromSealedViewStep::describePipeline(FormatSettings & settings) const
{
    if (!show_plan)
        return;

    WriteBufferFromOwnString out;
    view_plan.explainPipeline(out, {.header = settings.write_header, .compact_repeated_processor_chains = settings.compact_repeated_processor_chains});

    const String indent(settings.offset + settings.base_indent, settings.indent_char);
    for (const auto line : std::views::split(std::string_view(out.str()), '\n'))
        if (!line.empty())
            settings.out << indent << std::string_view(line) << '\n';
}

void ReadFromSealedViewStep::optimize()
{
    if (optimized)
        return;
    optimized = true;
    view_plan.optimize(optimization_settings);
}

void ReadFromSealedViewStep::sealSets(const std::vector<FutureSetFromSubqueryPtr> & sets) const
{
    /// The outer plan runs locally, because this step is not serializable.
    auto set_optimization_settings = optimization_settings;
    set_optimization_settings.make_distributed_plan = false;

    for (const auto & set : sets)
    {
        auto * set_plan = set->getQueryPlan();
        if (!set_plan || !set_plan->isInitialized())
            continue;

        auto sealed_set = std::unique_ptr<ReadFromSealedViewStep>(
            new ReadFromSealedViewStep(std::move(*set_plan), set_optimization_settings, show_plan));
        sealed_set->setStepDescription(*this);

        auto sealed_set_plan = std::make_unique<QueryPlan>();
        sealed_set_plan->addStep(std::move(sealed_set));
        set->setQueryPlan(std::move(sealed_set_plan));
    }
}

}
