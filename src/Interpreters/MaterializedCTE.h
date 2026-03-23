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

/// Owns a temporary Memory table used by a materialized CTE, and tracks whether
/// the table has already been populated. The `is_built` flag is checked
/// atomically in `MaterializingCTETransform` so that when the same CTE is
/// referenced from multiple places (e.g. two IN-subqueries or an IN-subquery
/// and the main plan), the table is written exactly once.
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

    bool hasPlanOrBuilt() const noexcept
    {
        return plan != nullptr || is_built;
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
    /// If true, the CTE has been materialized (i.e. the table has been populated and is ready for reads).
    std::atomic_bool is_built{false};
};

using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;

}
