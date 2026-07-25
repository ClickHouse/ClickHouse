#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Block_fwd.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStreamingStorage.h>

#include <mutex>


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
    const StorageID & getInnerTableID() const { return inner_table_id; }

private:
    void scheduleStreamingTasksImpl() override;
    void threadFunc();
    bool streamToViews(UInt64 cycle_epoch);
    void acknowledge(const Blocks & blocks, ContextMutablePtr queue_context);

    static String getInnerTableName(const StorageID & queue_table_id);
    static StorageID createInnerTable(
        const ASTCreateQuery & outer_query,
        const StorageID & queue_table_id,
        ContextPtr local_context,
        LoadingStrictnessLevel mode,
        UInt64 retention_seconds);

    const StorageID inner_table_id;
    const UInt64 max_batch_size;
    const UInt64 polling_interval_ms;

    BackgroundSchedulePoolTaskHolder streaming_task;
    UInt64 last_seen_refresh_epoch = 0;
    LoggerPtr log;
    std::mutex consume_mutex;
};

}
