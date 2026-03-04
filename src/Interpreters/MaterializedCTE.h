#pragma once

#include <atomic>
#include <memory>
#include <string>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

/// Owns a temporary Memory table used by a materialized CTE, and tracks whether
/// the table has already been populated. The `is_built` flag is checked
/// atomically in `MaterializingCTETransform` so that when the same CTE is
/// referenced from multiple places (e.g. two IN-subqueries or an IN-subquery
/// and the main plan), the table is written exactly once.
struct MaterializedCTE
{
    explicit MaterializedCTE(const StoragePtr & storage_, const std::string & cte_name_)
        : storage(storage_)
        , cte_name(cte_name_)
    {}

    MaterializedCTE(const MaterializedCTE &) = delete;
    MaterializedCTE & operator=(const MaterializedCTE &) = delete;

    /// Temporary table storage.
    StoragePtr storage;
    /// Name of the CTE.
    std::string cte_name;
    /// If true, query plan is built for the CTE (i.e. the table is being populated, but is not ready for reads yet).
    std::atomic_bool is_planned{false};
    /// If true, the CTE has been materialized (i.e. the table has been populated and is ready for reads).
    std::atomic_bool is_built{false};
};

using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;

}
