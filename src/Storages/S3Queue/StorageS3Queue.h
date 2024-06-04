#pragma once
#include "config.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueSource.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <Storages/StorageFactory.h>


namespace DB
{
class S3QueueMetadata;

class StorageS3Queue : public IStorage, WithContext
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    StorageS3Queue(
        std::unique_ptr<S3QueueSettings> s3queue_settings_,
        ConfigurationPtr configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        ASTStorage * engine_args,
        LoadingStrictnessLevel mode);

    String getName() const override { return "S3Queue"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /*query_info*/,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    const auto & getFormatName() const { return configuration->format; }

    const fs::path & getZooKeeperPath() const { return zk_path; }

    zkutil::ZooKeeperPtr getZooKeeper() const;

private:
    friend class ReadFromS3Queue;
    using FileIterator = StorageS3QueueSource::FileIterator;

    const std::unique_ptr<S3QueueSettings> s3queue_settings;
    const fs::path zk_path;

    std::shared_ptr<S3QueueMetadata> files_metadata;
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
    bool supportsDynamicSubcolumns() const override { return true; }

    std::shared_ptr<FileIterator> createFileIterator(ContextPtr local_context, const ActionsDAG::Node * predicate);
    std::shared_ptr<StorageS3QueueSource> createSource(
        size_t processor_id,
        const ReadFromFormatInfo & info,
        std::shared_ptr<StorageS3Queue::FileIterator> file_iterator,
        size_t max_block_size,
        ContextPtr local_context);

    bool hasDependencies(const StorageID & table_id);
    bool streamToViews();
    void threadFunc();
};

}
