#pragma once
#include "config.h"

#if USE_AWS_S3
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Processors/ISource.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/S3QueueLog.h>


namespace Poco { class Logger; }

namespace DB
{

struct ObjectMetadata;

class StorageS3QueueSource : public ISource, WithContext
{
public:
    using Storage = StorageObjectStorage;
    using ConfigurationPtr = Storage::ConfigurationPtr;
    using GlobIterator = StorageObjectStorageSource::GlobIterator;
    using ZooKeeperGetter = std::function<zkutil::ZooKeeperPtr()>;
    using RemoveFileFunc = std::function<void(std::string)>;
    using FileStatusPtr = S3QueueFilesMetadata::FileStatusPtr;
    using ReaderHolder = StorageObjectStorageSource::ReaderHolder;
    using Metadata = S3QueueFilesMetadata;
    using ObjectInfo = StorageObjectStorageSource::ObjectInfo;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    struct S3QueueObjectInfo : public ObjectInfo
    {
        S3QueueObjectInfo(
            const ObjectInfo & object_info,
            Metadata::FileMetadataPtr processing_holder_);

        Metadata::FileMetadataPtr processing_holder;
    };

    class FileIterator : public StorageObjectStorageSource::IIterator
    {
    public:
        FileIterator(
            std::shared_ptr<S3QueueFilesMetadata> metadata_,
            std::unique_ptr<GlobIterator> glob_iterator_,
            std::atomic<bool> & shutdown_called_,
            LoggerPtr logger_);

        ~FileIterator() override;

        /// Note:
        /// List results in s3 are always returned in UTF-8 binary order.
        /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)
        ObjectInfoPtr nextImpl() override;

        size_t estimatedKeysCount() override;

    private:
        using Bucket = S3QueueFilesMetadata::Bucket;
        using Processor = S3QueueFilesMetadata::Processor;

        const std::shared_ptr<S3QueueFilesMetadata> metadata;
        const std::unique_ptr<GlobIterator> glob_iterator;
        const Processor current_processor;

        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        LoggerPtr log;

        std::optional<Bucket> current_bucket;
        OrderedFileMetadata::BucketHolderPtr bucket_holder;
        std::mutex buckets_mutex;
        struct ListedKeys
        {
            std::deque<ObjectInfoPtr> keys;
            std::optional<Processor> processor;
        };
        std::unordered_map<Bucket, ListedKeys> listed_keys_cache;
        bool iterator_finished = false;

        ObjectInfoPtr getNextKeyFromAcquiredBucket();
        void releaseAndResetCurrentBucket();
    };

    StorageS3QueueSource(
        String name_,
        const Block & header_,
        std::unique_ptr<StorageObjectStorageSource> internal_source_,
        std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
        const S3QueueAction & action_,
        RemoveFileFunc remove_file_func_,
        const NamesAndTypesList & requested_virtual_columns_,
        ContextPtr context_,
        const std::atomic<bool> & shutdown_called_,
        const std::atomic<bool> & table_is_being_dropped_,
        std::shared_ptr<S3QueueLog> s3_queue_log_,
        const StorageID & storage_id_,
        LoggerPtr log_);

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

private:
    const String name;
    const S3QueueAction action;
    const std::shared_ptr<S3QueueFilesMetadata> files_metadata;
    const std::shared_ptr<StorageObjectStorageSource> internal_source;
    const NamesAndTypesList requested_virtual_columns;
    const std::atomic<bool> & shutdown_called;
    const std::atomic<bool> & table_is_being_dropped;
    const std::shared_ptr<S3QueueLog> s3_queue_log;
    const StorageID storage_id;

    RemoveFileFunc remove_file_func;
    LoggerPtr log;

    ReaderHolder reader;
    std::future<ReaderHolder> reader_future;
    std::atomic<bool> initialized{false};
    size_t processed_rows_from_file = 0;

    void applyActionAfterProcessing(const String & path);
    void appendLogElement(const std::string & filename, S3QueueFilesMetadata::FileStatus & file_status_, size_t processed_rows, bool processed);
    void lazyInitialize();
};

}
#endif
