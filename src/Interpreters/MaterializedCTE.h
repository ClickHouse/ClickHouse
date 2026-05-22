#pragma once

#include <Interpreters/DatabaseCatalog.h>
#include <base/defines.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class QueryPlan;

/// Owns a temporary Memory table used by a materialized CTE, and coordinates
/// the single writer (`MaterializingCTETransform`) with potentially many
/// concurrent readers (`MemorySource` instances inside arbitrary side
/// pipelines). Readers block on `build_future` until the writer fulfils
/// `build_promise`; the writer's `onCancel` and destructor signal an
/// exception on the same promise so blocked readers unwind cleanly on query
/// cancellation. Pattern parallel to `CreatingSetsTransform`'s
/// `promise_to_build` / `prepared_sets_cache` coordination.
struct MaterializedCTE
{
    explicit MaterializedCTE(const std::string & cte_name_);

    MaterializedCTE(const MaterializedCTE &) = delete;
    MaterializedCTE & operator=(const MaterializedCTE &) = delete;

    ~MaterializedCTE() noexcept;

    bool isStorageInitialized() const noexcept
    {
        return storage != nullptr;
    }

    /// True iff the writer (`MaterializingCTETransform`) has finished, with
    /// either success (`set_value`) or failure (`set_exception` from
    /// `onCancel` / destructor). Equivalent to "the promise has been
    /// fulfilled"; non-blocking. Use this where you previously checked
    /// `is_built`; if you need to block until completion, call
    /// `build_future.get()` instead.
    bool isBuilt() const
    {
        return build_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    /// True iff `plan` is still held here OR the writer has finished. Used
    /// by the Planner to decide whether to (re-)build the CTE subquery plan:
    /// once the plan has been claimed AND materialized, replanning would
    /// create a detached duplicate that the runtime does not wire into the
    /// materialization pipeline.
    bool hasPlanOrBuilt() const
    {
        return plan != nullptr || isBuilt();
    }

    TemporaryTableHolder extractTableHolder()
    {
        chassert(table_holder.has_value());
        return std::move(*table_holder);
    }

    /// Temporary table storage.
    StoragePtr storage = {};
    /// Temporary table storage.
    std::optional<TemporaryTableHolder> table_holder = {};
    /// Name of the CTE.
    const std::string cte_name;
    /// Temporary table name
    const std::string temporary_table_name;
    /// Query Plan for the CTE
    std::unique_ptr<QueryPlan> plan = {};
    /// If true, query plan is built for the CTE (i.e. the table is being populated, but is not ready for reads yet).
    std::atomic_bool is_materialization_planned{false};
    /// If true, the CTE's pre-built plan has already been passed through
    /// `QueryPlan::optimize`. Multiple `DelayedMaterializingCTEsStep`
    /// instances can reference the same `MaterializedCTE` (e.g. UNION
    /// branches plus the UNION-level step planted by
    /// `addBuildSubqueriesForMaterializedCTEsIfNeeded`); this flag lets
    /// `DelayedMaterializingCTEsStep::optimizePlans` skip redundant
    /// optimize calls in pass 1 of `resolveMaterializingCTEs`. Logically
    /// parallel to `is_materialization_planned`, but for the optimize
    /// stage rather than the claim/move stage.
    std::atomic_bool is_plan_optimized{false};
    /// Coordinates the writer (`MaterializingCTETransform`) with concurrent
    /// readers (`MemorySource` of any plan that reads `storage`). Fulfilled
    /// exactly once:
    /// - `set_value()` from `MaterializingCTETransform::generate` on success;
    /// - `set_exception(...)` from `MaterializingCTETransform::onCancel` on
    ///   pipeline cancellation, or from the destructor's fallback path on
    ///   any other abnormal exit.
    /// Readers in `MemorySource::generate` call `build_future.get()` to wait
    /// for completion; the get() rethrows the writer's exception on failure.
    std::promise<void> build_promise;
    std::shared_future<void> build_future{build_promise.get_future().share()};
};

using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;
using MaterializedCTEWeakPtr = std::weak_ptr<MaterializedCTE>;

}
