#pragma once

#include <Interpreters/DatabaseCatalog.h>

#include <atomic>
#include <memory>

namespace DB
{

/// Owns a temporary Memory table used by a materialized CTE, and tracks whether
/// the table has already been populated. The `is_materialized` flag is checked
/// atomically in `MaterializingCTETransform` so that when the same CTE is
/// referenced from multiple places (e.g. two IN-subqueries or an IN-subquery
/// and the main plan), the table is written exactly once.
struct MaterializedCTE
{
    explicit MaterializedCTE(TemporaryTableHolder holder_) : holder(std::move(holder_)) {}

    MaterializedCTE(const MaterializedCTE &) = delete;
    MaterializedCTE & operator=(const MaterializedCTE &) = delete;

    TemporaryTableHolder holder;
    std::atomic_bool is_materialized{false};
};

using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;

}
