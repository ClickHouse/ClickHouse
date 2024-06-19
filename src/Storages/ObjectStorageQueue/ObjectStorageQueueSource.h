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
    using ConfigurationPtr = Storage::ConfigurationPtr;
    using GlobIterator = StorageObjectStorageSource::GlobIterator;
    using ZooKeeperGetter = std::function<zkutil::ZooKeeperPtr()>;
    using RemoveFileFunc = std::function<void(std::string)>;
    using FileStatusPtr = ObjectStorageQueueMetadata::FileStatusPtr;
    using ReaderHolder = StorageObjectStorageSource::ReaderHolder;
    using Metadata = ObjectStorageQueueMetadata;
    using ObjectInfo = StorageObjectStorageSource::ObjectInfo;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    struct ObjectStorageQueueObjectInfo : public ObjectInfo
    {
        ObjectStorageQueueObjectInfo(
            const ObjectInfo & object_info,
            Metadata::FileMetadataPtr processing_holder_);

        Metadata::FileMetadataPtr processing_holder;
    };

    class FileIterator : public StorageObjectStorageSource::IIterator
    {
    public:
        FileIterator(
            std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
            std::unique_ptr<GlobIterator> glob_iterator_,
            std::atomic<bool> & shutdown_called_,
            LoggerPtr logger_);

        ObjectInfoPtr nextImpl(size_t processor) override;

        size_t estimatedKeysCount() override;

    private:
        using Bucket = ObjectStorageQueueMetadata::Bucket;
        using Processor = ObjectStorageQueueMetadata::Processor;

        const std::shared_ptr<ObjectStorageQueueMetadata> metadata;
        const std::unique_ptr<GlobIterator> glob_iterator;

        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        LoggerPtr log;

        std::mutex buckets_mutex;
        struct ListedKeys
        {
            std::deque<ObjectInfoPtr> keys;
            std::optional<Processor> processor;
        };
        std::unordered_map<Bucket, ListedKeys> listed_keys_cache;
        bool iterator_finished = false;
        std::unordered_map<size_t, ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr> bucket_holders;

        std::pair<ObjectInfoPtr, ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr> getNextKeyFromAcquiredBucket(size_t processor);
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
        LoggerPtr log_);

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

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

    RemoveFileFunc remove_file_func;
    LoggerPtr log;

    ReaderHolder reader;
    std::future<ReaderHolder> reader_future;
    std::atomic<bool> initialized{false};
    size_t processed_rows_from_file = 0;

    ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr current_bucket_holder;

    void applyActionAfterProcessing(const String & path);
    void appendLogElement(const std::string & filename, ObjectStorageQueueMetadata::FileStatus & file_status_, size_t processed_rows, bool processed);
    void lazyInitialize(size_t processor);
};

}
