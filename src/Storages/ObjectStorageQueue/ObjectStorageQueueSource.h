#pragma once
#include "config.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Processors/ISource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/ObjectStorageQueueLog.h>


namespace Poco { class Logger; }

namespace DB
{

struct ObjectMetadata;

class ObjectStorageQueueSource : public ISource, WithContext
{
public:
    using Storage = StorageObjectStorage;
    using Source = StorageObjectStorageSource;
    using RemoveFileFunc = std::function<void(std::string)>;
    using BucketHolderPtr = ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr;
    using BucketHolder = ObjectStorageQueueOrderedFileMetadata::BucketHolder;

    struct ObjectStorageQueueObjectInfo : public Source::ObjectInfo
    {
        ObjectStorageQueueObjectInfo(
            const Source::ObjectInfo & object_info,
            ObjectStorageQueueMetadata::FileMetadataPtr file_metadata_);

        ObjectStorageQueueMetadata::FileMetadataPtr file_metadata;
    };

    class FileIterator : public StorageObjectStorageSource::IIterator
    {
    public:
        FileIterator(
            std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
            std::unique_ptr<Source::GlobIterator> glob_iterator_,
            std::atomic<bool> & shutdown_called_,
            LoggerPtr logger_);

        bool isFinished() const;

        /// Note:
        /// List results in s3 are always returned in UTF-8 binary order.
        /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)
        Source::ObjectInfoPtr nextImpl(size_t processor) override;

        size_t estimatedKeysCount() override;

        /// If the key was taken from iterator via next() call,
        /// we might later want to return it back for retrying.
        void returnForRetry(Source::ObjectInfoPtr object_info);

        /// Release hold buckets.
        /// In fact, they could be released in destructors of BucketHolder,
        /// but we anyway try to release them explicitly,
        /// because we want to be able to rethrow exceptions if they might happen.
        void releaseFinishedBuckets();

    private:
        using Bucket = ObjectStorageQueueMetadata::Bucket;
        using Processor = ObjectStorageQueueMetadata::Processor;

        const std::shared_ptr<ObjectStorageQueueMetadata> metadata;
        const std::unique_ptr<Source::GlobIterator> glob_iterator;

        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        LoggerPtr log;

        struct ListedKeys
        {
            std::deque<Source::ObjectInfoPtr> keys;
            std::optional<Processor> processor;
        };
        /// A cache of keys which were iterated via glob_iterator, but not taken for processing.
        std::unordered_map<Bucket, ListedKeys> listed_keys_cache;

        /// We store a vector of holders, because we cannot release them until processed files are committed.
        std::unordered_map<size_t, std::vector<BucketHolderPtr>> bucket_holders;

        /// Is glob_iterator finished?
        std::atomic_bool iterator_finished = false;

        /// Only for processing without buckets.
        std::deque<Source::ObjectInfoPtr> objects_to_retry;

        std::pair<Source::ObjectInfoPtr, ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr> getNextKeyFromAcquiredBucket(size_t processor);
        bool hasKeysForProcessor(const Processor & processor) const;
    };

    ObjectStorageQueueSource(
        String name_,
        size_t processor_id_,
        const Block & header_,
        std::unique_ptr<StorageObjectStorageSource> internal_source_,
        std::shared_ptr<ObjectStorageQueueMetadata> files_metadata_,
        const ObjectStorageQueueAction & action_,
        RemoveFileFunc remove_file_func_,
        const NamesAndTypesList & requested_virtual_columns_,
        ContextPtr context_,
        const std::atomic<bool> & shutdown_called_,
        const std::atomic<bool> & table_is_being_dropped_,
        std::shared_ptr<ObjectStorageQueueLog> system_queue_log_,
        const StorageID & storage_id_,
        LoggerPtr log_,
        size_t max_processed_files_before_commit_,
        size_t max_processed_rows_before_commit_,
        size_t max_processed_bytes_before_commit_,
        size_t max_processing_time_sec_before_commit_,
        bool commit_once_processed_);

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

    /// Commit files after insertion into storage finished.
    /// `success` defines whether insertion was successful or not.
    void commit(bool success, const std::string & exception_message = {});

private:
    const String name;
    const size_t processor_id;
    const ObjectStorageQueueAction action;
    const std::shared_ptr<ObjectStorageQueueMetadata> files_metadata;
    const std::shared_ptr<StorageObjectStorageSource> internal_source;
    const NamesAndTypesList requested_virtual_columns;
    const std::atomic<bool> & shutdown_called;
    const std::atomic<bool> & table_is_being_dropped;
    const std::shared_ptr<ObjectStorageQueueLog> system_queue_log;
    const StorageID storage_id;
    const size_t max_processed_files_before_commit;
    const size_t max_processed_rows_before_commit;
    const size_t max_processed_bytes_before_commit;
    const size_t max_processing_time_sec_before_commit;
    const bool commit_once_processed;

    RemoveFileFunc remove_file_func;
    LoggerPtr log;

    std::vector<ObjectStorageQueueMetadata::FileMetadataPtr> processed_files;
    std::vector<ObjectStorageQueueMetadata::FileMetadataPtr> failed_during_read_files;

    Source::ReaderHolder reader;
    std::future<Source::ReaderHolder> reader_future;
    std::atomic<bool> initialized{false};

    size_t processed_rows_from_file = 0;
    size_t total_processed_rows = 0;
    size_t total_processed_bytes = 0;

    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    Chunk generateImpl();
    void applyActionAfterProcessing(const String & path);
    void appendLogElement(const std::string & filename, ObjectStorageQueueMetadata::FileStatus & file_status_, size_t processed_rows, bool processed);
    void lazyInitialize(size_t processor);
};

}
