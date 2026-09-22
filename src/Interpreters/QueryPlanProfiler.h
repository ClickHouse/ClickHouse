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

/// Records one plan that runs for a query without being part of its plan tree. Two kinds today:
/// an `IN (SELECT ...)` whose set is built during planning so that index analysis can use it, and
/// a scalar `(SELECT ...)` executed during analysis and folded into the query as a literal. Either
/// runs in a pipeline of its own before the main one exists and is linked from nowhere, yet its
/// rows count towards the query: without this the stored plan describes a query reading millions of
/// rows and never names the table they came from.
///
/// Wraps code that already exists, in this order:
///
///     auto capture = QueryPlanProfiler::captureSubPlan(context, plan, id, kind);
///     ... build the pipeline from `plan` ...
///     capture.instrument(pipeline);
///     ... run the pipeline ...
///     capture.finish(pipeline);
///
/// The order is the whole of the contract, and each step is where it is for a reason. The capture
/// is taken before the pipeline is built because it reads the pretty column names off the
/// `ActionsDAG`s, which building the pipeline moves out of every expression step. `instrument` must
/// come before execution starts, since it is what makes the per-processor stopwatches run at all.
/// `finish` must come after execution and while the pipeline is still alive: it is what serializes
/// the sub-plan, and both of its inputs are only there at that moment -- the plan is in the shape
/// that actually ran only once `buildQueryPipeline` has optimized it, and the statistics are read
/// from the pipeline's processors.
///
/// The capture must also come *after* the sub-plan has been optimized, which is why the call sites
/// optimize explicitly and then build with `do_optimize = false`, as the interpreter does for the
/// query's own plan. Optimization rewrites the plan -- it merges and replaces steps and pushes
/// filters into `PREWHERE` -- so a capture taken before it would describe a shape that never ran,
/// would hold pretty names for steps that no longer exist, and would record consumed subquery ids
/// onto steps that are then discarded.
///
/// Hard to misuse rather than merely documented: every call is a no-op on a query that is not being
/// profiled, so no caller needs a condition of its own, and skipping `instrument` or `finish` --
/// which an exception mid-pipeline does -- costs the sub-plan its statistics and nothing more,
/// because the destructor still serializes and publishes its structure. Nothing here throws.
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

    /// Serializes the sub-plan with what its pipeline measured, and hands it to the profiler.
    void finish(const QueryPipeline & pipeline);

private:
    friend class QueryPlanProfiler;
    SubPlanCapture(
        QueryPlanProfilerPtr profiler_,
        const QueryPlan & plan_,
        PrettyNamesPerPlan pretty_names_,
        size_t subquery_id_,
        SubPlanKind kind_);

    /// Serializes the sub-plan and gives it to the profiler, once. Clears `profiler`, which both
    /// marks the capture spent and turns every later call into the no-op an inert capture performs.
    void publish(const StepStatsStorage * stats) noexcept;

    QueryPlanProfilerPtr profiler;
    const QueryPlan * plan = nullptr;

    /// Captured before the pipeline was built, when the ActionsDAGs it is derived from still
    /// existed. Plain name maps, so plan optimization does not invalidate them.
    PrettyNamesPerPlan pretty_names;

    /// Identifies this subquery within the query. The steps that consume its result carry the same
    /// id, which is what lets the document name them.
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

    /// Whether this query should have its plan captured. Everything the capture costs -- keeping
    /// the plan, per-processor timings, and the join analyze mode -- is decided here, so the
    /// condition has to describe the queries that can actually end up with a `query_plan` value,
    /// not merely those that asked for one.
    static bool canEnableProfiler(const ContextPtr & context, const ASTPtr & ast, bool internal);

    /// Reports why a query that asked for its plan is not going to get one.
    static void declineCapture(const ContextPtr & context, const char * reason);

    /// Takes ownership of the plan and returns it, so that the caller can go on building the
    /// pipeline from the copy the profiler will render -- the two must be the same object, because
    /// the pretty names built here are keyed by plan pointer.
    QueryPlan & setQueryPlan(QueryPlan plan_);

    /// The plan as JSON, for `system.query_log.query_plan`.
    ///
    /// One shot, and not repeatable: the first call serializes the plan and then releases it, so
    /// every later call can only hand back the same string. There is no re-rendering it with
    /// better inputs afterwards, because by then there is no plan left to render. Whatever the
    /// first call produces is what the `system.query_log` row will carry.
    ///
    /// That makes *when* it is first called the whole of the contract. Pass the pipeline to
    /// include per-step runtime statistics, and do it while the pipeline is still alive:
    /// StepStatsStorage reads the processors, whose reports are unreachable once the pipeline has
    /// been reset. So the first call belongs at pipeline-finalize time, even though nothing reads
    /// the result until the query is logged much later -- call it any earlier, or without the
    /// pipeline, and the statistics are lost for this query. Queries that fail during or before
    /// execution never reach that point, and for them a plan without statistics is the right
    /// answer rather than a mistake.
    ///
    /// Always either valid JSON or empty, never a bare diagnostic string: the caller writes it
    /// into a JSON column, which parses what it is given. Empty also covers the queries that failed
    /// before a plan was ever captured, so callers need not ask whether there is one.
    ///
    /// Everything this allocates, the statistics included, happens under a memory-tracker blocker,
    /// and no exception escapes.
    const String & render(const QueryPipeline * pipeline = nullptr);

    size_t getMaxDescriptionLength() const { return max_description_length; }

    /// Instruments the pipeline so per-step timings are collected, by attaching a
    /// StepWallClockRegistry built from the captured plan. Without it the per-processor stopwatch
    /// is never started (see ExecutionThreadContext.cpp) and every step renders as
    /// "time 0.00 ns · parallelism Unknown". Must be called before execution starts.
    void instrumentPipeline(QueryPipeline & pipeline) const;

    /// Starts recording a set sub-plan, if the query in `context` is being profiled at all. See
    /// SubPlanCapture for what the caller then does with the result; a query without a profiler
    /// gets an inert capture, so the call site needs no condition.
    ///
    /// `subquery_id` is `FutureSetFromSubquery::getSubqueryId`. The steps that consume the set
    /// carry the same id, which is what lets the document say who used it rather than leaving the
    /// sub-plan looking like an unrelated plan that happened to run.
    /// Takes the plan by non-const reference because it records, on the sub-plan's own steps, the
    /// ids of any subqueries they consume -- a subquery nested inside this one.
    static SubPlanCapture captureSubPlan(
        const ContextPtr & context, QueryPlan & sub_plan, size_t subquery_id, SubPlanKind kind);

private:
    friend class SubPlanCapture;

    /// Takes a finished sub-plan. Safe to call from index analysis, which runs concurrently.
    void addSubPlan(SerializedSubPlan sub_plan);

    /// Drops the captured plan, which render does as soon as it has serialized it. A QueryPlan owns
    /// a QueryPlanResourceHolder -- storages, table locks, contexts -- so holding one after the
    /// query has finished keeps a table from being dropped: `DROP TABLE` waits for the last storage
    /// reference under `database_atomic_wait_for_drop_and_detach_synchronously`, which the
    /// stateless tests set.
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
