#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/CapturedPlan.h>
#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>
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

    size_t getMaxDescriptionLength() const { return max_description_length; }

    /// Reads the names out of the `ActionsDAG`s, which building the pipeline moves into the
    /// `ExpressionActions`. Returns them rather than keeping them: they are needed only until the
    /// structure is captured, a few lines later in the same function.
    static PrettyNamesPerPlan capturePrettyNames(const QueryPlan & plan);

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

    /// Ends profiling, drops every object kept while the query was being executed,
    /// and keeps only objects about the execution and query structure. Idempotent: a query that
    /// failed before its pipeline did finishes at logging time instead.
    void finish();

    /// Renders the plan as JSON.
    String render();

private:
    const size_t max_description_length;

    /// The plan, as values. Taken when the structure is captured and valid from then on, so the
    /// plan itself is not kept: it belongs to the interpreter and dies whenever that does.
    std::optional<CapturedPlan> captured;
    PlanAnalysisSnapshot plan_snapshot;

    /// Built while the plan is still around, because it takes a clock per step of it, then attached
    /// to the pipeline so the processors can time their steps and read back once the query has run.
    StepProfilerPtr step_profiler;

    /// The order the methods above must come in is asserted against this in debug builds.
    bool finished = false;
};
}
