#pragma once

#include <Storages/StorageView.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

/// Special view for transposed representation of system.metric_log.
/// Can be used as compatibility layer, when you want to store transposed table, but your queries want wide table.
///
/// This view is not attached by default, it's attached by TransposedMetricLog, because
/// it depend on it.
class StorageSystemMetricLogView final : public IStorage
{
public:
    /// Order for elements in view
    static constexpr size_t EVENT_TIME_POSITION = 0;
    static constexpr size_t VALUE_POSITION = 1;
    static constexpr size_t METRIC_POSITION = 2;
    static constexpr size_t HOSTNAME_POSITION = 3;
    static constexpr size_t EVENT_DATE_POSITION = 4;
    static constexpr size_t EVENT_TIME_HOUR_POSITION = 5;

    explicit StorageSystemMetricLogView(const StorageID & table_id_, const StorageID & source_storage_id);

    std::string getName() const override { return "SystemMetricLogView"; }

    bool isSystemStorage() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    std::optional<String> addFilterByMetricNameStep(QueryPlan & query_plan, const Names & column_names, ContextPtr context);
private:
    StorageView internal_view;
};

}
