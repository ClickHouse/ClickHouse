#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/Profiling/Capture/CapturedPlan.h>
#include <base/defines.h>

#include <mutex>
#include <optional>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class QueryPipeline;
class StepProfiler;
using StepProfilerPtr = std::shared_ptr<StepProfiler>;

/// Records one plan that runs for a query without being part of its plan tree. For example:
///     - an `IN (SELECT ...)` whose set is built during planning so that index analysis can use it
///     - a scalar `(SELECT ...)` executed during analysis and folded into the query as a literal.
///
/// Made by `QueryPlanLogCapture::captureSubPlan` before the sub-plan's pipeline is built, because
/// that is the last moment its `ActionsDAG`s can be read. Inert when the query is not captured.
class SubPlanCapture
{
public:
    SubPlanCapture() = default;
    ~SubPlanCapture();

    /// Not copyable: it is a scope guard over a plan and a pipeline that outlive it. Movable so a
    /// caller can declare it before the branch that creates it. Move-assignment publishes whatever
    /// the target already held, so an overwritten capture is never silently dropped.
    SubPlanCapture(const SubPlanCapture &) = delete;
    SubPlanCapture & operator=(const SubPlanCapture &) = delete;
    SubPlanCapture(SubPlanCapture && other) noexcept;
    SubPlanCapture & operator=(SubPlanCapture && other) noexcept;

    /// Reads the shape of the sub-plan and attaches a `StepProfiler` to its pipeline, without which
    /// every step would report no time. Call after the pipeline is built and before it runs.
    void instrument(QueryPipeline & pipeline);

    /// Fills in what the pipeline measured and hands the sub-plan to the query's capture.
    void finish(QueryPipeline & pipeline);

private:
    friend class QueryPlanLogCapture;
    SubPlanCapture(
        QueryPlanLogCapturePtr owner_,
        const QueryPlan & plan_,
        PrettyNamesPerPlan pretty_names_,
        size_t max_description_length_,
        size_t subquery_id_,
        SubPlanKind kind_);

    /// Gives the captured sub-plan to the query's capture, once. Does nothing if `instrument` did
    /// not get as far as reading the plan.
    void publish() noexcept;

    QueryPlanLogCapturePtr owner;
    const QueryPlan * plan = nullptr;
    PrettyNamesPerPlan pretty_names;
    size_t max_description_length = 0;
    size_t subquery_id = 0;
    SubPlanKind kind = SubPlanKind::Set;
    std::optional<CapturedSubPlan> captured;
};

class QueryPlanLogCapture
{
public:
    /// Whether this query should have its plan captured. `query` is the text of the query as the
    /// client sent it, before `query_masking_rules` were applied.
    static bool canCapturePlan(const ContextPtr & context, const ASTPtr & ast, std::string_view query, bool internal);

    /// Reports why a query that asked for its plan is not going to get one.
    static void declineCapture(const ContextPtr & context, const char * reason);

    /// Records, on every step of `plan` that reads the result of a subquery, the id of that
    /// subquery. Must run while the `ActionsDAG`s are still in the steps, before the pipeline is
    /// built.
    static void recordConsumedSubqueries(QueryPlan & plan);

    /// Starts recording a sub-plan, if the query in `context` is being captured at all. Call after
    /// the sub-plan is optimized and before its pipeline is built, with the settings it is going
    /// to be built with.
    ///
    /// A sub-plan that runs as a distributed plan is not recorded, as a distributed query's own
    /// plan is not: building the pipeline replaces its steps with one that reads from the
    /// distributed executor, which is all that would be left to describe.
    static SubPlanCapture captureSubPlan(
        const ContextPtr & context,
        QueryPlan & sub_plan,
        const QueryPlanOptimizationSettings & optimization_settings,
        size_t subquery_id,
        SubPlanKind kind);

    /// Records the shape of the plan as values. Called once the pipeline has been built, which is
    /// both the first moment the index analysis and the lazily built child plans exist, and the
    /// last moment the plan does. Nothing after this touches the plan.
    ///
    /// May throw: the caller starting the capture is what keeps a diagnostic from failing a
    /// query that would otherwise have run.
    void capturePlanStructure(const PlanWalk & walk, const PrettyNamesPerPlan & pretty_names, size_t max_description_length);

    /// Forgets the captured plan, so that a query whose profiling could not be set up logs no
    /// plan at all rather than one that can never be given its statistics.
    void dropCapturedPlan();

    /// Records what the pipeline measured. Does not keep the pipeline object, but extracts
    /// the statistics out of it.
    void captureStatistics(QueryPipeline & pipeline);

    /// Calls `f` with the captured plan, if there is one. Rendering it is the caller's business:
    /// this captures, and only the places that write a plan out decide in what form.
    ///
    /// `f` runs under the lock that sub-plans finishing on other threads take, so the plan cannot
    /// change while it is being read.
    template <typename F>
    void withCapturedPlan(F && f) const
    {
        std::lock_guard lock(mutex);
        if (captured)
            std::forward<F>(f)(*captured);
    }

private:
    friend class SubPlanCapture;

    /// Takes a finished sub-plan. Safe to call concurrently: sets for index analysis are built
    /// from several threads.
    void addSubPlan(CapturedSubPlan sub_plan);

    /// Guards everything below. Sub-plans are published from whichever thread built them, and
    /// nothing guarantees that a set still being built on another thread has finished by the
    /// time a failed query renders its plan.
    mutable std::mutex mutex;

    /// Sub-plans finished before the query's own plan was captured, such as every scalar subquery,
    /// which runs during analysis. Moved into `captured` when it is taken; later ones are appended
    /// to it directly.
    VectorWithMemoryTracking<CapturedSubPlan> pending_sub_plans TSA_GUARDED_BY(mutex);

    /// The plan, as values. Taken when the structure is captured and valid from then on, so the
    /// plan itself is not kept: it belongs to the interpreter and dies whenever that does.
    std::optional<CapturedPlan> captured TSA_GUARDED_BY(mutex);
};
}
