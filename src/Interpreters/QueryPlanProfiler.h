#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanToJSON.h>
#include <mutex>
namespace DB
{

class QueryPipeline;
class QueryPlanProfiler;
class StepStatsStorage;

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
    void publish(const StepStatsStorage * stats) noexcept;

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

    /// Takes ownership of the plan and returns it.
    QueryPlan & setQueryPlan(QueryPlan plan_);

    /// Renders the query plan as JSON.
    /// Only renders at the first call, all subsequent calls return the cached object.
    ///
    /// The `pipeline` parameter is used to include per-step runtime statistics.
    const String & render(const QueryPipeline * pipeline = nullptr);

    size_t getMaxDescriptionLength() const { return max_description_length; }

    /// Attaches a StepWallClockRegistry in the processors, otherwise they would report
    /// 0.00 ns as executed time.
    void instrumentPipeline(QueryPipeline & pipeline) const;

    /// Starts recording a sub-plan, if the query in `context` is being profiled at all.
    static SubPlanCapture captureSubPlan(
        const ContextPtr & context, QueryPlan & sub_plan, size_t subquery_id, SubPlanKind kind);

private:
    friend class SubPlanCapture;

    /// Takes a finished sub-plan. Safe to call concurrently.
    void addSubPlan(SerializedSubPlan sub_plan);

    /// Drops the captured plan, to avoid holding QueryPlanResourceHolder.
    void releasePlan()
    {
        query_plan.reset();
        pretty_names.reset();
    }

    bool canRender() const { return query_plan && query_plan->isInitialized() && pretty_names.has_value(); }

    const size_t max_description_length;

    std::mutex sub_plans_mutex;
    std::vector<SerializedSubPlan> sub_plans TSA_GUARDED_BY(sub_plans_mutex);
    std::optional<QueryPlan> query_plan;
    std::optional<PrettyNamesPerPlan> pretty_names;
    std::optional<String> plan_json;
};
}
