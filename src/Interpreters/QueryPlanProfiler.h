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

    size_t getMaxDescriptionLength() const { return max_description_length; }

    /// Records the plan the query is about to run, taking ownership of it and handing back
    /// a reference.
    QueryPlan & captureQueryPlan(QueryPlan plan_);

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
    /// The body shared by `captureStatistics` and `finish`: the pipeline is what the statistics
    /// come from, and there is none when a query failed before finishing.
    void capture(QueryPipeline * pipeline);

    bool canCapture() const
    {
        return running.query_plan && running.query_plan->isInitialized() && running.pretty_names.has_value();
    }

    const size_t max_description_length;

    /// Set of fields the profiler needs to keep alive while the query is still being executed.
    /// Once the query finishes, or throws, these fields are cleared and only `captured` has
    /// valid state.
    struct WhileRunning
    {
        /// QueryPlan has to be dropped as it holds `QueryPlanResourceHolder`.
        std::optional<QueryPlan> query_plan;

        /// Attached to the pipeline so the processors can time their steps; kept so the statistics
        /// can be read back off it once the query has run.
        StepProfilerPtr step_profiler;
        std::optional<PrettyNamesPerPlan> pretty_names;

        void release()
        {
            query_plan.reset();
            step_profiler.reset();
            pretty_names.reset();
        }
    };

    WhileRunning running;

    /// The order the methods above must come in is asserted against this in debug builds.
    bool finished = false;

    /// Valid once the profiling is finished
    std::optional<CapturedPlan> captured;
};
}
