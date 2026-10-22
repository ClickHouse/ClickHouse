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

    /// Attaches a StepProfiler to the pipeline, otherwise the processors would report
    /// 0.00 ns as executed time.
    void instrumentPipeline(QueryPipeline & pipeline);

    /// Frees what was only needed while the query was being profiled: the clocks, and the
    /// analyzers, which hold the `IJoin`s and with them their hash tables.
    /// What was captured stays. Idempotent: a query that failed before its pipeline finished
    /// reaches this at logging time instead.
    void releaseRunningState();

    /// The captured plan, if there is one. Rendering it is the caller's business: the profiler
    /// captures, and only the places that write a plan out decide in what form.
    const std::optional<CapturedPlan> & getCapturedPlan() const { return captured; }

private:
    const size_t max_description_length;

    /// The plan, as values. Taken when the structure is captured and valid from then on, so the
    /// plan itself is not kept: it belongs to the interpreter and dies whenever that does.
    std::optional<CapturedPlan> captured;

    /// Built while the plan is still around, because it walks it for the clocks and the analyzers,
    /// then attached to the pipeline so the processors can time their steps and read back once the
    /// query has run.
    StepProfilerPtr step_profiler;

    /// The order the methods above must come in is asserted against this in debug builds.
    bool finished = false;
};
}
