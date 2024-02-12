#pragma once
#include "config.h"

#if USE_AWS_S3
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Processors/ISource.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Interpreters/S3QueueLog.h>


namespace Poco { class Logger; }

namespace DB
{

struct ObjectMetadata;

class StorageS3QueueSource : public ISource, WithContext
{
public:
    using Storage = StorageObjectStorage<S3StorageSettings>;

    using ConfigurationPtr = Storage::ConfigurationPtr;
    using GlobIterator = StorageObjectStorageSource::GlobIterator;
    using ZooKeeperGetter = std::function<zkutil::ZooKeeperPtr()>;
    using RemoveFileFunc = std::function<void(std::string)>;
    using FileStatusPtr = S3QueueFilesMetadata::FileStatusPtr;
    using ReaderHolder = StorageObjectStorageSource::ReaderHolder;
    using Metadata = S3QueueFilesMetadata;
    using ObjectInfo = RelativePathWithMetadata;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    struct S3QueueObjectInfo : public ObjectInfo
    {
        S3QueueObjectInfo(
            const std::string & key_,
            const ObjectMetadata & object_metadata_,
            Metadata::ProcessingNodeHolderPtr processing_holder_);

        Metadata::ProcessingNodeHolderPtr processing_holder;
    };

    class FileIterator : public StorageObjectStorageSource::IIterator
    {
    public:
        FileIterator(
            std::shared_ptr<S3QueueFilesMetadata> metadata_,
            std::unique_ptr<GlobIterator> glob_iterator_,
            size_t current_shard_,
            std::atomic<bool> & shutdown_called_);

        /// Note:
        /// List results in s3 are always returned in UTF-8 binary order.
        /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)
        ObjectInfoPtr next(size_t processor) override;

        size_t estimatedKeysCount() override;

    private:
        const std::shared_ptr<S3QueueFilesMetadata> metadata;
        const std::unique_ptr<GlobIterator> glob_iterator;
        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        Poco::Logger * log;

        const bool sharded_processing;
        const size_t current_shard;
        std::unordered_map<size_t, std::deque<ObjectInfoPtr>> sharded_keys;
        std::mutex sharded_keys_mutex;
    };

    StorageS3QueueSource(
        String name_,
        const Block & header_,
        std::unique_ptr<StorageObjectStorageSource> internal_source_,
        std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
        size_t processing_id_,
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
    const size_t processing_id;
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
};

}
#endif
