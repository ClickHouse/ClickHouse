#pragma once

#include <Storages/IStorage.h>

namespace DB
{

/** `system.user_query_log` shows the current user their own query log records without requiring
  * access to the whole query log table. It reads the table configured by the `query_log.database` and
  * `query_log.table` server settings (`system.query_log` by default) under an internal full-access
  * context, filtered by the initiating user: rows where `if(initial_user != '', initial_user, user)`
  * is equal to `currentUser()`.
  *
  * A vetted subset of the outer query's predicate (comparisons of the partition / point-lookup
  * columns with constants) is pushed down into the internal query, so ordinary lookups keep
  * partition pruning and index analysis on the backing table instead of scanning the whole
  * retained log. See `ReadFromUserQueryLog` in the implementation file.
  */
class StorageSystemUserQueryLog final : public IStorage
{
public:
    StorageSystemUserQueryLog(const StorageID & table_id_, ColumnsDescription columns_);

    static ColumnsDescription getColumnsDescription();

    std::string getName() const override { return "SystemUserQueryLog"; }
    bool isSystemStorage() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;
};

}
