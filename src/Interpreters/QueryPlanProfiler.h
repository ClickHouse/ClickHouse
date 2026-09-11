#pragma once

#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
namespace DB
{

class QueryPipeline;

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

private:

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
    std::optional<QueryPlan> query_plan;
    std::optional<PrettyNamesPerPlan> pretty_names;
    std::optional<String> plan_json;
};
}
