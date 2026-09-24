#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/CapturedPlan.h>
#include <Processors/QueryPlan/QueryPlanToJSON.h>
#include <mutex>
namespace DB
{

class QueryPipeline;
class QueryPlanProfiler;
class StepStatisticsCollector;

/// Records one plan that runs for a query without being part of its plan tree. For example:
///     - an `IN (SELECT ...)` whose set is built during planning so that index analysis can use it
///     - a scalar `(SELECT ...)` executed during analysis and folded into the query as a literal.
class SubPlanCapture
{
public:
    /// Inert: what a query without a profiler gets, and what every method below then does nothing on.
    SubPlanCapture() = default;
    ~SubPlanCapture();

    /// Not copyable: it is a scope guard over a plan and a pipeline that outlive it. Movable so a
    /// caller can declare it before the branch that creates it. Move-assignment publishes whatever
    /// the target already held, so an overwritten capture is never silently dropped.
    SubPlanCapture(const SubPlanCapture &) = delete;
    SubPlanCapture & operator=(const SubPlanCapture &) = delete;
    SubPlanCapture(SubPlanCapture && other) noexcept;
    SubPlanCapture & operator=(SubPlanCapture && other) noexcept;

    /// Attaches a StepWallClockRegistry, without which every step of the sub-plan renders as
    /// "time 0.00 ns". Call after the pipeline is built and before it runs.
    void instrument(QueryPipeline & pipeline);

    /// Serializes the sub-plan and hands it to the profiler.
    void finish(const QueryPipeline & pipeline);

private:
    friend class QueryPlanProfiler;
    SubPlanCapture(
        QueryPlanProfilerPtr profiler_,
        const QueryPlan & plan_,
        PrettyNamesPerPlan pretty_names_,
        size_t subquery_id_,
        SubPlanKind kind_);

    /// Serializes the sub-plan and gives it to the profiler, once.
    void publish(const StepStatisticsCollector * stats) noexcept;

    QueryPlanProfilerPtr profiler;
    const QueryPlan * plan = nullptr;
    PrettyNamesPerPlan pretty_names;
    size_t subquery_id = 0;
    SubPlanKind kind = SubPlanKind::Set;
};

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

    /// Starts recording a sub-plan, if the query in `context` is being profiled at all.
    static SubPlanCapture captureSubPlan(
        const ContextPtr & context, QueryPlan & sub_plan, size_t subquery_id, SubPlanKind kind);

    /// Records the plan the query is about to run, taking ownership of it and handing back
    /// a reference.
    QueryPlan & captureQueryPlan(QueryPlan plan_);

    /// Records what the pipeline measured. Does not keep the pipeline obejct, but extracts
    /// the statistics out of it.
    void captureStatistics(const QueryPipeline & pipeline);

    /// Ends profiling, drops every object kept while the query was being executed,
    /// and keeps only objects about the execution and query structure. Idempotent: a query that
    /// failed before its pipeline did finishes at logging time instead.
    void finish();

    /// Renders the plan as JSON.
    String render();

    size_t getMaxDescriptionLength() const { return max_description_length; }

    /// Attaches a StepWallClockRegistry in the processors, otherwise they would report
    /// 0.00 ns as executed time.
    void instrumentPipeline(QueryPipeline & pipeline) const;

private:
    friend class SubPlanCapture;

    /// The body shared by `captureStatistics` and `finish`: the pipeline is what the statistics
    /// come from, and there is none when a query failed before finishing.
    void capture(const QueryPipeline * pipeline);

    /// Takes a finished sub-plan. Safe to call concurrently.
    void addSubPlan(CapturedSubPlan sub_plan);

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
        /// Sub-plans arrive as subqueries finish, from index analysis, which runs concurrently.
        std::mutex sub_plans_mutex;
        std::vector<CapturedSubPlan> sub_plans TSA_GUARDED_BY(sub_plans_mutex);

        /// QueryPlan has to be dropped as it holds `QueryPlanResourceHolder`.
        std::optional<QueryPlan> query_plan;
        std::optional<PrettyNamesPerPlan> pretty_names;

        /// `sub_plans` is not cleared here: `finish` moves it into the capture under the lock,
        /// which leaves it empty, and a late `addSubPlan` appending to it afterwards is harmless.
        void release()
        {
            query_plan.reset();
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
