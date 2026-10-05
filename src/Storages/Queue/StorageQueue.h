#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Block_fwd.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStreamingStorage.h>

#include <functional>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>


namespace DB
{

class ASTCreateQuery;
class ColumnsDescription;

class StorageQueue final : public IStreamingStorage, WithContext
{
public:
    StorageQueue(
        const StorageID & table_id_,
        ContextPtr context_,
        LoadingStrictnessLevel mode_,
        const ASTCreateQuery & query_,
        const ColumnsDescription & columns_,
        const String & comment_,
        UInt64 retention_seconds_,
        UInt64 max_batch_size_,
        UInt64 polling_interval_ms_);

    ~StorageQueue() override;

    String getName() const override { return "Queue"; }

    bool noPushingToViewsOnInserts() const override { return true; }
    bool shouldPushToMaterializedView(const StorageID & view_id, ContextPtr query_context) const override;

    void startup() override;
    void shutdown(bool is_drop) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void addPostFilterStep(QueryPlan & query_plan, ContextPtr query_context) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;
    void checkTableSizeBelowDropLimit(ContextPtr query_context) const override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;
    Strings getDataPaths() const override;

    StoragePtr getInnerTable(ContextPtr local_context) const;
    const StorageID & getInnerTableID() const { return main_table_id; }

    Names getMaterializedViewSourceTrackingColumns(ContextPtr query_context) const;
    void trackMaterializedViewSourceRows(const StorageID & view_id, Block & block, ContextPtr query_context);

private:
    struct ViewAcknowledgementState;
    void scheduleStreamingTasksImpl() override;
    void threadFunc();
    bool streamToViews(const String & consumer_group, const StorageID & consumer_table_id, UInt64 cycle_epoch);
    void acknowledge(const StorageID & consumer_table_id, const Blocks & blocks, ContextMutablePtr queue_context);

    std::pair<String, bool> getConsumerSettingsForView(const StorageID & view_id, ContextPtr query_context) const;
    std::unordered_map<String, bool> getConsumerGroups(const std::vector<StorageID> & view_ids, ContextPtr query_context) const;
    StorageID ensureConsumerGroup(const String & consumer_group, bool start_at_latest);
    StorageID resetConsumerGroup(const String & consumer_group, bool start_at_latest);

    static String getMainTableName(const StorageID & queue_table_id);
    static String getConsumerTableName(const StorageID & queue_table_id, const String & consumer_group);
    static String getConsumerViewName(const StorageID & queue_table_id, const String & consumer_group);
    static StorageID createDataTable(
        const ASTCreateQuery & outer_query,
        const StorageID & table_id,
        ContextPtr local_context,
        LoadingStrictnessLevel mode,
        UInt64 retention_seconds,
        bool consumer_table);
    void createConsumerView(
        const StorageID & consumer_table_id,
        const StorageID & consumer_view_id,
        ContextPtr query_context) const;
    std::vector<StorageID> getInternalTables(ContextPtr query_context, std::string_view name_prefix) const;
    void dropInternalTableIfAny(const StorageID & table_id, bool sync, ContextPtr query_context) const;

    const ASTPtr outer_create_query;
    const StorageID main_table_id;
    const UInt64 retention_seconds;
    const UInt64 max_batch_size;
    const UInt64 polling_interval_ms;

    BackgroundSchedulePoolTaskHolder streaming_task;
    UInt64 last_seen_refresh_epoch = 0;
    LoggerPtr log;
    std::mutex consume_mutex;
    mutable std::shared_mutex consumer_groups_mutex;

    std::mutex post_filter_steps_mutex;
    std::unordered_map<String, std::function<void(QueryPlan &)>> post_filter_steps;

    mutable std::mutex view_acknowledgement_mutex;
    std::shared_ptr<ViewAcknowledgementState> view_acknowledgement_state;
};

}
