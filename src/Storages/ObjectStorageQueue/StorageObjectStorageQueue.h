#pragma once
#include "config.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Storages/IStorage.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/System/StorageSystemObjectStorageQueueSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageFactory.h>
#include <base/defines.h>


namespace DB
{
class ObjectStorageQueueMetadata;
struct ObjectStorageQueueSettings;

class StorageObjectStorageQueue : public IStorage, WithContext
{
public:
    StorageObjectStorageQueue(
        std::unique_ptr<ObjectStorageQueueSettings> queue_settings_,
        StorageObjectStorageConfigurationPtr configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        ASTStorage * engine_args,
        LoadingStrictnessLevel mode,
        bool keep_data_in_keeper_);

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

    void renameInMemory(const StorageID & new_table_id) override;

    const auto & getFormatName() const { return configuration->format; }

    const fs::path & getZooKeeperPath() const { return zk_path; }

    zkutil::ZooKeeperPtr getZooKeeper() const;

    ObjectStorageQueueSettings getSettings() const;

    /// Can setting be changed via ALTER TABLE MODIFY SETTING query.
    static bool isSettingChangeable(const std::string & name, ObjectStorageQueueMode mode);

    /// Generate id for the S3(Azure/etc)Queue commit.
    /// Used for system.s3(azure/etc)_queue_log.
    static UInt64 generateCommitID();

    static String chooseZooKeeperPath(
        const ContextPtr & context_,
        const StorageID & table_id,
        const Settings & settings,
        const ObjectStorageQueueSettings & queue_settings,
        UUID database_uuid = UUIDHelpers::Nil,
        String * result_zookeeper_name = nullptr);

    static constexpr auto engine_names = {"S3Queue", "AzureQueue"};

    void checkTableCanBeRenamed(const StorageID & new_name) const override;

private:
    friend class ReadFromObjectStorageQueue;
    using FileIterator = ObjectStorageQueueSource::FileIterator;
    using CommitSettings = ObjectStorageQueueSource::CommitSettings;
    using ProcessingProgress = ObjectStorageQueueSource::ProcessingProgress;
    using ProcessingProgressPtr = ObjectStorageQueueSource::ProcessingProgressPtr;
    using LastProcessedFileInfoMap = ObjectStorageQueueIFileMetadata::LastProcessedFileInfoMap;
    using LastProcessedFileInfoMapPtr = ObjectStorageQueueIFileMetadata::LastProcessedFileInfoMapPtr;
    using AfterProcessingSettings = ObjectStorageQueuePostProcessor::AfterProcessingSettings;
    using PartitionLastProcessedFileInfoMap = ObjectStorageQueueIFileMetadata::PartitionLastProcessedFileInfoMap;

    ObjectStorageType type;
    const std::string engine_name;
    std::string zookeeper_name;
    fs::path zk_path;
    const bool enable_logging_to_queue_log;
    mutable std::mutex mutex;
    UInt64 polling_min_timeout_ms TSA_GUARDED_BY(mutex);
    UInt64 polling_max_timeout_ms TSA_GUARDED_BY(mutex);
    UInt64 polling_backoff_ms TSA_GUARDED_BY(mutex);
    UInt64 list_objects_batch_size TSA_GUARDED_BY(mutex);
    bool enable_hash_ring_filtering TSA_GUARDED_BY(mutex);
    CommitSettings commit_settings TSA_GUARDED_BY(mutex);
    /// The after_processing action itself is handled the old way for compatibility:
    /// it needs to be available in Keeper metadata for older server versions.
    /// Therefore it is not in AfterProcessingSettings.
    AfterProcessingSettings after_processing_settings TSA_GUARDED_BY(mutex);
    bool commit_on_select TSA_GUARDED_BY(mutex);
    bool deduplication_v2 TSA_GUARDED_BY(mutex);

    size_t min_insert_block_size_rows_for_materialized_views TSA_GUARDED_BY(mutex);
    size_t min_insert_block_size_bytes_for_materialized_views TSA_GUARDED_BY(mutex);

    std::unique_ptr<ObjectStorageQueueMetadata> temp_metadata;
    std::shared_ptr<ObjectStorageQueueMetadata> files_metadata;
    StorageObjectStorageConfigurationPtr configuration;
    ObjectStoragePtr object_storage;

    const std::optional<FormatSettings> format_settings;

    UInt64 reschedule_processing_interval_ms TSA_GUARDED_BY(mutex);

    std::atomic<bool> shutdown_called = false;
    std::atomic<bool> startup_finished = false;
    std::atomic<bool> table_is_being_dropped = false;

    mutable std::mutex streaming_mutex;
    std::shared_ptr<StorageObjectStorageQueue::FileIterator> streaming_file_iterator;
    std::vector<BackgroundSchedulePoolTaskHolder> streaming_tasks;

    LoggerPtr log;

    void startup() override;
    void shutdown(bool is_drop) override;

    bool supportsSubsetOfColumns(const ContextPtr & context_) const;
    bool supportsSubcolumns() const override { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }
    bool supportsDynamicSubcolumns() const override { return true; }

    const ObjectStorageQueueTableMetadata & getTableMetadata() const;

    std::shared_ptr<FileIterator> createFileIterator(ContextPtr local_context, const ActionsDAG::Node * predicate);
    std::shared_ptr<ObjectStorageQueueSource> createSource(
        size_t processor_id,
        const ReadFromFormatInfo & info,
        FormatParserSharedResourcesPtr parser_shared_resources,
        ProcessingProgressPtr progress_,
        std::shared_ptr<StorageObjectStorageQueue::FileIterator> file_iterator,
        size_t max_block_size,
        ContextPtr local_context,
        bool commit_once_processed);

    /// Get number of dependent materialized views.
    size_t getDependencies() const;
    /// A background thread function,
    /// executing the whole process of reading from object storage
    /// and pushing result to dependent tables.
    void threadFunc(size_t streaming_tasks_index);
    /// A subset of logic executed by threadFunc.
    bool streamToViews(size_t streaming_tasks_index);
    /// Apply after_processing action to successfully processed files.
    void postProcess(const StoredObjects & successful_objects) const;
    /// Commit processed files to keeper as either successful or unsuccessful.
    void commit(
        bool insert_succeeded,
        size_t inserted_rows,
        std::vector<std::shared_ptr<ObjectStorageQueueSource>> & sources,
        time_t transaction_start_time,
        const std::string & exception_message = {},
        int error_code = 0) const;

    const bool can_be_moved_between_databases;
    const bool keep_data_in_keeper;

    const bool use_hive_partitioning;

    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
    NamesAndTypesList file_columns;
};

}
