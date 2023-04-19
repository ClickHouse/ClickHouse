#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/S3Queue/S3QueueSettings.h>

#include <Processors/ISource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/StorageS3.h>

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
        const String & zookeper_path_,
        const String & mode_,
        const Configuration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
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

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Write is not supported by storage {}", getName());
    }

    void truncate(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*local_context*/, TableExclusiveLockHolder &) override {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported by storage {}", getName());
    }

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    static ColumnsDescription getTableStructureFromData(
        Configuration & configuration, const std::optional<FormatSettings> & format_settings, ContextPtr ctx)
    {
        return StorageS3::getTableStructureFromData(configuration, format_settings, ctx);
    }

    const auto & getFormatName() const { return format_name; }

private:

    Configuration s3_configuration;
    std::vector<String> keys;
    NamesAndTypesList virtual_columns;
    Block virtual_block;
    uint64_t milliseconds_to_wait = 10000;

    String format_name;
    String compression_method;
    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;
    bool is_key_with_globs = false;

    bool supportsSubcolumns() const override;

    void threadFunc();
    size_t getTableDependentCount() const;
    std::atomic<bool> mv_attached = false;
    bool hasDependencies(const StorageID & table_id);
    std::atomic<bool> shutdown_called{false};
    Poco::Logger * log;


    void startup() override;
    void shutdown() override;

    struct TaskContext
    {
        BackgroundSchedulePool::TaskHolder holder;
        std::atomic<bool> stream_cancelled {false};
        explicit TaskContext(BackgroundSchedulePool::TaskHolder&& task_) : holder(std::move(task_))
        {
        }
    };
    std::shared_ptr<TaskContext> task;

    bool supportsSubsetOfColumns() const override;
    static Names getVirtualColumnNames();

    const String mode;

    static const String default_zookeeper_name;
    const String zookeeper_name;
    const String zookeeper_path;
    const String replica_name;
    const String replica_path;

    zkutil::ZooKeeperPtr current_zookeeper;
    mutable std::mutex current_zookeeper_mutex;

    void setZooKeeper();
    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    bool createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot);
    // Return default or custom zookeeper name for table
    const String & getZooKeeperName() const { return zookeeper_name; }
    const String & getZooKeeperPath() const { return zookeeper_path; }

};

}

#endif
