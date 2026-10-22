#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/Profiling/Capture/CapturedPlan.h>
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

    /// Records the shape of the plan as values. Called once the pipeline has been built, which is
    /// both the first moment the index analysis and the lazily built child plans exist, and the
    /// last moment the plan does. Nothing after this touches the plan.
    ///
    /// May throw: the caller starting the profiling is what keeps a diagnostic from failing a
    /// query that would otherwise have run.
    void capturePlanStructure(const PlanWalk & walk, const PrettyNamesPerPlan & pretty_names);

    /// Forgets the captured plan, so that a query whose profiling could not be set up logs no
    /// plan at all rather than one that can never be given its statistics.
    void dropCapturedPlan() { captured.reset(); }

    /// Records what the pipeline measured. Does not keep the pipeline object, but extracts
    /// the statistics out of it.
    void captureStatistics(QueryPipeline & pipeline);

    /// The captured plan, if there is one. Rendering it is the caller's business: the profiler
    /// captures, and only the places that write a plan out decide in what form.
    const std::optional<CapturedPlan> & getCapturedPlan() const { return captured; }

private:
    const size_t max_description_length;

    /// The plan, as values. Taken when the structure is captured and valid from then on, so the
    /// plan itself is not kept: it belongs to the interpreter and dies whenever that does.
    std::optional<CapturedPlan> captured;
};
}
