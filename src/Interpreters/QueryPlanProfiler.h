#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/CapturedPlan.h>
namespace DB
{

class QueryPipeline;
class StepProfiler;
using StepProfilerPtr = std::shared_ptr<StepProfiler>;

class QueryPlanProfiler
{
public:
    explicit QueryPlanProfiler(size_t max_description_length_)
        : max_description_length(max_description_length_)
    {
    }

    /// Whether this query should have its plan captured.
    static bool canEnableProfiler(const ContextPtr & context, const ASTPtr & ast, bool internal);

    /// Reports why a query that asked for its plan is not going to get one.
    static void declineCapture(const ContextPtr & context, const char * reason);

    /// Records the shape of the plan, and everything the analysis will need from it, as values.
    /// Called once the pipeline has been built, which is both the first moment the index analysis
    /// and the lazily built child plans exist, and the last moment the plan does. Nothing after
    /// this touches the plan.
    void capturePlanStructure(const QueryPlan & plan, const PrettyNamesPerPlan & pretty_names);

    /// Records what the pipeline measured. Does not keep the pipeline object, but extracts
    /// the statistics out of it.
    void captureStatistics(QueryPipeline & pipeline);

    /// Hands the `StepProfiler` to the pipeline, which owns it from then on. Without it the
    /// processors would report 0.00 ns as executed time.
    void instrumentPipeline(QueryPipeline & pipeline);

    /// The captured plan, if there is one. Rendering it is the caller's business: the profiler
    /// captures, and only the places that write a plan out decide in what form.
    const std::optional<CapturedPlan> & getCapturedPlan() const { return captured; }

private:
    const size_t max_description_length;

    /// The plan, as values. Taken when the structure is captured and valid from then on, so the
    /// plan itself is not kept: it belongs to the interpreter and dies whenever that does.
    std::optional<CapturedPlan> captured;

    /// Built while the plan is still around, because it walks it for the clocks and the analyzers.
    /// Null again after `instrumentPipeline`: this object lives in the `Context`, which outlives
    /// the query, so holding it here would keep the analyzers' hash tables alive with it.
    StepProfilerPtr step_profiler;
};
}
