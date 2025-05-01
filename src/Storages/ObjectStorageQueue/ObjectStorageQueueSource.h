#pragma once
#include "config.h"

#include <Interpreters/ObjectStorageQueueLog.h>
#include <Processors/ISource.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <base/defines.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace Poco { class Logger; }

namespace DB
{

struct ObjectMetadata;

class ObjectStorageQueueSource : public ISource, WithContext
{
public:
    using Storage = StorageObjectStorage;
    using Source = StorageObjectStorageSource;
    using BucketHolderPtr = ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr;
    using BucketHolder = ObjectStorageQueueOrderedFileMetadata::BucketHolder;
    using FileMetadataPtr = ObjectStorageQueueMetadata::FileMetadataPtr;

    struct ObjectStorageQueueObjectInfo : public ObjectInfo
    {
        ObjectStorageQueueObjectInfo(
            const ObjectInfo & object_info,
            FileMetadataPtr file_metadata_);

        FileMetadataPtr file_metadata;
    };

    class FileIterator : public IObjectIterator, private WithContext
    {
    public:
        FileIterator(
            std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
            ObjectStoragePtr object_storage_,
            ConfigurationPtr configuration_,
            const StorageID & storage_id_,
            size_t list_objects_batch_size_,
            const ActionsDAG::Node * predicate_,
            const NamesAndTypesList & virtual_columns_,
            ContextPtr context_,
            LoggerPtr logger_,
            bool enable_hash_ring_filtering_,
            bool file_deletion_on_processed_enabled_,
            std::atomic<bool> & shutdown_called_);

        bool isFinished();

        ObjectInfoPtr next(size_t processor) override;

        size_t estimatedKeysCount() override;

        /// If the key was taken from iterator via next() call,
        /// we might later want to return it back for retrying.
        void returnForRetry(ObjectInfoPtr object_info, FileMetadataPtr file_metadata);

        /// Release hold buckets.
        /// In fact, they could be released in destructors of BucketHolder,
        /// but we anyway try to release them explicitly,
        /// because we want to be able to rethrow exceptions if they might happen.
        void releaseFinishedBuckets();

    private:
        using Bucket = ObjectStorageQueueMetadata::Bucket;
        using Processor = ObjectStorageQueueMetadata::Processor;

        const std::shared_ptr<ObjectStorageQueueMetadata> metadata;
        const ObjectStoragePtr object_storage;
        const ConfigurationPtr configuration;
        const NamesAndTypesList virtual_columns;
        const bool file_deletion_on_processed_enabled;
        const ObjectStorageQueueMode mode;
        const bool enable_hash_ring_filtering;
        const StorageID storage_id;

        ObjectStorageIteratorPtr object_storage_iterator;
        std::unique_ptr<re2::RE2> matcher;
        ExpressionActionsPtr filter_expr;
        bool recursive{false};

        Source::ObjectInfos object_infos TSA_GUARDED_BY(next_mutex);
        std::vector<FileMetadataPtr> file_metadatas;
        bool is_finished = false;
        std::mutex next_mutex;
        size_t index = 0;

        std::pair<ObjectInfoPtr, FileMetadataPtr> next();
        void filterProcessableFiles(Source::ObjectInfos & objects);
        void filterOutProcessedAndFailed(Source::ObjectInfos & objects);

        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        LoggerPtr log;

        struct ListedKeys
        {
            std::deque<std::pair<ObjectInfoPtr, FileMetadataPtr>> keys;
            std::optional<Processor> processor;
        };
        /// A cache of keys which were iterated via glob_iterator, but not taken for processing.
        std::unordered_map<Bucket, ListedKeys> listed_keys_cache TSA_GUARDED_BY(mutex);

        /// We store a vector of holders, because we cannot release them until processed files are committed.
        std::unordered_map<size_t, std::vector<BucketHolderPtr>> bucket_holders TSA_GUARDED_BY(mutex);

        /// Is glob_iterator finished?
        std::atomic_bool iterator_finished = false;

        /// Only for processing without buckets.
        std::deque<std::pair<ObjectInfoPtr, FileMetadataPtr>> objects_to_retry TSA_GUARDED_BY(mutex);

        struct NextKeyFromBucket
        {
            ObjectInfoPtr object_info;
            FileMetadataPtr file_metadata;
            ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info;
        };
        NextKeyFromBucket getNextKeyFromAcquiredBucket(size_t processor) TSA_REQUIRES(mutex);
        bool hasKeysForProcessor(const Processor & processor) const;
    };

    struct CommitSettings
    {
        size_t max_processed_files_before_commit;
        size_t max_processed_rows_before_commit;
        size_t max_processed_bytes_before_commit;
        size_t max_processing_time_sec_before_commit;
    };

    struct ProcessingProgress
    {
        std::atomic<size_t> processed_files = 0;
        std::atomic<size_t> processed_rows = 0;
        std::atomic<size_t> processed_bytes = 0;
        Stopwatch elapsed_time{CLOCK_MONOTONIC_COARSE};
    };
    using ProcessingProgressPtr = std::shared_ptr<ProcessingProgress>;

    ObjectStorageQueueSource(
        String name_,
        size_t processor_id_,
        std::shared_ptr<FileIterator> file_iterator_,
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ProcessingProgressPtr progress_,
        const ReadFromFormatInfo & read_from_format_info_,
        const std::optional<FormatSettings> & format_settings_,
        const CommitSettings & commit_settings_,
        std::shared_ptr<ObjectStorageQueueMetadata> files_metadata_,
        ContextPtr context_,
        size_t max_block_size_,
        const std::atomic<bool> & shutdown_called_,
        const std::atomic<bool> & table_is_being_dropped_,
        std::shared_ptr<ObjectStorageQueueLog> system_queue_log_,
        const StorageID & storage_id_,
        LoggerPtr log_,
        bool commit_once_processed_);

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

    /// Commit files after insertion into storage finished.
    /// `success` defines whether insertion was successful or not.
    void prepareCommitRequests(
        Coordination::Requests & requests,
        bool insert_succeeded,
        StoredObjects & successful_files,
        const std::string & exception_message = {},
        int error_code = 0);

    /// Do some work after Processed/Failed files were successfully committed to keeper.
    void finalizeCommit(bool insert_succeeded, const std::string & exception_message = {});

private:
    Chunk generateImpl();
    /// Log to system.s3(azure)_queue_log.
    void appendLogElement(const FileMetadataPtr & file_metadata_, bool processed);
    /// Commit processed files.
    /// This method is only used for SELECT query, not for streaming to materialized views.
    /// Which is defined by passing a flag commit_once_processed.
    void commit(bool insert_succeeded, const std::string & exception_message = {});

    const String name;
    const size_t processor_id;
    const std::shared_ptr<FileIterator> file_iterator;
    const ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    const ProcessingProgressPtr progress;
    ReadFromFormatInfo read_from_format_info;
    const std::optional<FormatSettings> format_settings;
    const CommitSettings commit_settings;
    const std::shared_ptr<ObjectStorageQueueMetadata> files_metadata;
    const size_t max_block_size;
    const ObjectStorageQueueMode mode;

    const std::atomic<bool> & shutdown_called;
    const std::atomic<bool> & table_is_being_dropped;
    const std::shared_ptr<ObjectStorageQueueLog> system_queue_log;
    const StorageID storage_id;
    const bool commit_once_processed;

    LoggerPtr log;

    enum class FileState
    {
        Processing,
        ErrorOnRead,
        Cancelled,
        Processed,
    };
    struct ProcessedFile
    {
        explicit ProcessedFile(FileMetadataPtr metadata_)
            : state(FileState::Processing), metadata(metadata_) {}

        FileState state;
        FileMetadataPtr metadata;
        std::string exception_during_read;
    };
    std::vector<ProcessedFile> processed_files;
    Source::ReaderHolder reader;
};

}
