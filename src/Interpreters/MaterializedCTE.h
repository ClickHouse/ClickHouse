#pragma once

#include <Interpreters/DatabaseCatalog.h>
#include <base/defines.h>

#include <atomic>
#include <memory>
#include <optional>
#include <string>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class QueryPlan;

/// Owns a temporary Memory table used by a materialized CTE. The writer
/// (`MaterializingCTETransform`) commits the data to `storage` via
/// `MemorySink::onFinish` and sets `is_built` with `memory_order_release`.
/// Readers (`MemorySource` instances inside any consumer pipeline) are
/// gated on the writer's completion by the `DelayedPortsProcessor`
/// inserted by `MaterializingCTEsStep::updatePipeline` via
/// `addPipelineBefore` - by the time a reader's `generate` runs, the
/// gate has released and `is_built` must be observed `true`
/// (asserted as a fail-fast invariant in `ReadFromMemoryStorageStep`).
/// Pattern parallel to `CreatingSetsStep`'s `DelayedPortsProcessor`
/// gating.
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

    /// True iff the writer (`MaterializingCTETransform`) has finished
    /// committing the CTE's data to `storage` via `MemorySink::onFinish`.
    /// Non-blocking; pairs with the `memory_order_release` store performed
    /// by `MaterializingCTETransform::generate`. Used by external callers
    /// (planner, distributed executor) to decide whether the CTE's plan
    /// must still be built or whether the storage is safe to read.
    bool isBuilt() const
    {
        return is_built.load(std::memory_order_acquire);
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
    /// True once `MaterializingCTETransform::generate` has finished writing
    /// to `storage` and called `MemorySink::onFinish`. Set with
    /// `memory_order_release` by the writer; readers in
    /// `ReadFromMemoryStorageStep` load with `memory_order_acquire`. This is
    /// a fail-fast invariant: by the time a `MemorySource` is scheduled to
    /// produce a chunk, `DelayedPortsProcessor` (inserted by
    /// `MaterializingCTEsStep::updatePipeline` via `addPipelineBefore`)
    /// guarantees the writer has finished, so the load must observe `true`.
    /// If it ever observes `false`, the planner failed to wire the gate -
    /// fail loudly rather than block or read half-populated storage.
    std::atomic_bool is_built{false};
};

using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;
using MaterializedCTEWeakPtr = std::weak_ptr<MaterializedCTE>;

}
