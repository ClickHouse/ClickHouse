#pragma once
#include "config.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/System/StorageSystemObjectStorageQueueSettings.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>


namespace DB
{
class ObjectStorageQueueMetadata;
struct ObjectStorageQueueSettings;

class StorageObjectStorageQueue : public IStorage, WithContext
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    StorageObjectStorageQueue(
        std::unique_ptr<ObjectStorageQueueSettings> queue_settings_,
        ConfigurationPtr configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        ASTStorage * engine_args,
        LoadingStrictnessLevel mode);

    String getName() const override { return engine_name; }

    ObjectStorageType getType() { return type; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /*query_info*/,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;

    void alter(
        const AlterCommands & commands,
        ContextPtr local_context,
        AlterLockHolder & table_lock_holder) override;

    const auto & getFormatName() const { return configuration->format; }

    const fs::path & getZooKeeperPath() const { return zk_path; }

    zkutil::ZooKeeperPtr getZooKeeper() const;

    ObjectStorageQueueSettings getSettings() const;

private:
    friend class ReadFromObjectStorageQueue;
    using FileIterator = ObjectStorageQueueSource::FileIterator;
    using CommitSettings = ObjectStorageQueueSource::CommitSettings;

    ObjectStorageType type;
    const std::string engine_name;
    const fs::path zk_path;
    const bool enable_logging_to_queue_log;
    UInt64 polling_min_timeout_ms;
    UInt64 polling_max_timeout_ms;
    UInt64 polling_backoff_ms;
    const CommitSettings commit_settings;

    std::shared_ptr<ObjectStorageQueueMetadata> files_metadata;
    ConfigurationPtr configuration;
    ObjectStoragePtr object_storage;

    const std::optional<FormatSettings> format_settings;

    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};
    UInt64 reschedule_processing_interval_ms;

    std::atomic<bool> mv_attached = false;
    std::atomic<bool> shutdown_called = false;
    std::atomic<bool> table_is_being_dropped = false;

    LoggerPtr log;

    void startup() override;
    void shutdown(bool is_drop) override;
    void drop() override;
    bool supportsSubsetOfColumns(const ContextPtr & context_) const;
    bool supportsSubcolumns() const override { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }
    bool supportsDynamicSubcolumns() const override { return true; }
    const ObjectStorageQueueTableMetadata & getTableMetadata() const { return files_metadata->getTableMetadata(); }

    std::shared_ptr<FileIterator> createFileIterator(ContextPtr local_context, const ActionsDAG::Node * predicate);
    std::shared_ptr<ObjectStorageQueueSource> createSource(
        size_t processor_id,
        const ReadFromFormatInfo & info,
        std::shared_ptr<StorageObjectStorageQueue::FileIterator> file_iterator,
        size_t max_block_size,
        ContextPtr local_context,
        bool commit_once_processed);

    bool hasDependencies(const StorageID & table_id);
    bool streamToViews();
    void threadFunc();
};

}
