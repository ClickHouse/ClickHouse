#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Core/Types.h>

#    include <Compression/CompressionInfo.h>
#    include <Common/ZooKeeper/ZooKeeper.h>

#    include <Core/BackgroundSchedulePool.h>
#    include <Storages/IStorage.h>
#    include <Storages/S3Queue/S3QueueFilesMetadata.h>
#    include <Storages/S3Queue/S3QueueSettings.h>
#    include <Storages/S3Queue/S3QueueSource.h>
#    include <Storages/StorageS3Settings.h>

#    include <IO/CompressionMethod.h>
#    include <IO/S3/getObjectInfo.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/threadPoolCallbackRunner.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <Processors/ISource.h>
#    include <Storages/Cache/SchemaCache.h>
#    include <Storages/StorageConfiguration.h>
#    include <Storages/StorageS3.h>
#    include <Poco/URI.h>
#    include <Common/logger_useful.h>

namespace Aws::S3
{
class Client;
}

namespace DB
{


class StorageS3Queue : public IStorage, WithContext
{
public:
    using Configuration = typename StorageS3::Configuration;

    StorageS3Queue(
        std::unique_ptr<S3QueueSettings> s3queue_settings_,
        const Configuration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        ASTPtr partition_by_ = nullptr);

    String getName() const override { return "S3Queue"; }

    Pipe read(
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

    void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*local_context*/,
        TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    const auto & getFormatName() const { return configuration.format; }

    const String & getZooKeeperPath() const { return zk_path; }

    zkutil::ZooKeeperPtr getZooKeeper() const;

private:
    const std::unique_ptr<S3QueueSettings> s3queue_settings;
    const S3QueueAction after_processing;

    std::shared_ptr<S3QueueFilesMetadata> files_metadata;
    Configuration configuration;
    NamesAndTypesList virtual_columns;
    UInt64 reschedule_processing_interval_ms;

    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;

    String zk_path;
    mutable zkutil::ZooKeeperPtr zk_client;
    mutable std::mutex zk_mutex;

    std::atomic<bool> mv_attached = false;
    std::atomic<bool> shutdown_called{false};
    Poco::Logger * log;

    bool supportsSubcolumns() const override;
    bool withGlobs() const { return configuration.url.key.find_first_of("*?{") != std::string::npos; }

    void threadFunc();
    size_t getTableDependentCount() const;
    bool hasDependencies(const StorageID & table_id);

    void startup() override;
    void shutdown() override;
    void drop() override;

    struct TaskContext
    {
        BackgroundSchedulePool::TaskHolder holder;
        std::atomic<bool> stream_cancelled{false};
        explicit TaskContext(BackgroundSchedulePool::TaskHolder && task_) : holder(std::move(task_)) { }
    };
    std::shared_ptr<TaskContext> task;

    bool supportsSubsetOfColumns(const ContextPtr & context_) const;

    const UInt32 zk_create_table_retries = 1000;
    bool createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot);
    void checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot);

    using KeysWithInfo = StorageS3QueueSource::KeysWithInfo;

    std::shared_ptr<StorageS3QueueSource::IIterator>
    createFileIterator(ContextPtr local_context, ASTPtr query);

    void streamToViews();
    Configuration updateConfigurationAndGetCopy(ContextPtr local_context);
};

}

#endif
