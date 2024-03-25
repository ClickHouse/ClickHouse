#pragma once
#include "config.h"

#if USE_AWS_S3
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Processors/ISource.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/StorageS3.h>
#include <Interpreters/S3QueueLog.h>


namespace Poco { class Logger; }

namespace DB
{

class StorageS3QueueSource : public ISource, WithContext
{
public:
    using IIterator = StorageS3Source::IIterator;
    using KeyWithInfoPtr = StorageS3Source::KeyWithInfoPtr;
    using GlobIterator = StorageS3Source::DisclosedGlobIterator;
    using ZooKeeperGetter = std::function<zkutil::ZooKeeperPtr()>;
    using RemoveFileFunc = std::function<void(std::string)>;
    using FileStatusPtr = S3QueueFilesMetadata::FileStatusPtr;
    using Metadata = S3QueueFilesMetadata;

    struct S3QueueKeyWithInfo : public StorageS3Source::KeyWithInfo
    {
        S3QueueKeyWithInfo(
                const std::string & key_,
                std::optional<S3::ObjectInfo> info_,
                Metadata::ProcessingNodeHolderPtr processing_holder_);

        Metadata::ProcessingNodeHolderPtr processing_holder;
    };

    class FileIterator : public IIterator
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
        KeyWithInfoPtr next(size_t idx) override;

        size_t estimatedKeysCount() override;

    private:
        const std::shared_ptr<S3QueueFilesMetadata> metadata;
        const std::unique_ptr<GlobIterator> glob_iterator;
        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        Poco::Logger * log;

        const bool sharded_processing;
        const size_t current_shard;
        std::unordered_map<size_t, std::deque<KeyWithInfoPtr>> sharded_keys;
        std::mutex sharded_keys_mutex;
    };

    StorageS3QueueSource(
        String name_,
        const Block & header_,
        std::unique_ptr<StorageS3Source> internal_source_,
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

    ~StorageS3QueueSource() override;

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

private:
    const String name;
    const S3QueueAction action;
    const size_t processing_id;
    const std::shared_ptr<S3QueueFilesMetadata> files_metadata;
    const std::shared_ptr<StorageS3Source> internal_source;
    const NamesAndTypesList requested_virtual_columns;
    const std::atomic<bool> & shutdown_called;
    const std::atomic<bool> & table_is_being_dropped;
    const std::shared_ptr<S3QueueLog> s3_queue_log;
    const StorageID storage_id;

    RemoveFileFunc remove_file_func;
    LoggerPtr log;

    using ReaderHolder = StorageS3Source::ReaderHolder;
    ReaderHolder reader;
    std::future<ReaderHolder> reader_future;
    std::atomic<bool> initialized{false};
    size_t processed_rows_from_file = 0;

    void lazyInitialize();
    void applyActionAfterProcessing(const String & path);
    void appendLogElement(const std::string & filename, S3QueueFilesMetadata::FileStatus & file_status_, size_t processed_rows, bool processed);
};

}
#endif
