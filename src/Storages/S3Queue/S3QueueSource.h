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
    using Metadata = S3QueueFilesMetadata;

    struct S3QueueKeyWithInfo : public StorageS3Source::KeyWithInfo
    {
        S3QueueKeyWithInfo(
                const std::string & key_,
                std::optional<S3::ObjectInfo> info_,
                std::unique_ptr<Metadata::ProcessingHolder> processing_holder_,
                std::shared_ptr<Metadata::FileStatus> file_status_);

        std::unique_ptr<Metadata::ProcessingHolder> processing_holder;
        std::shared_ptr<Metadata::FileStatus> file_status;
    };

    class FileIterator : public IIterator
    {
    public:
        FileIterator(std::shared_ptr<S3QueueFilesMetadata> metadata_, std::unique_ptr<GlobIterator> glob_iterator_);

        KeyWithInfoPtr next() override;

        size_t estimatedKeysCount() override;

    private:
        const std::shared_ptr<S3QueueFilesMetadata> metadata;
        const std::unique_ptr<GlobIterator> glob_iterator;
        std::mutex mutex;
    };

    StorageS3QueueSource(
        String name_,
        const Block & header_,
        std::unique_ptr<StorageS3Source> internal_source_,
        std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
        const S3QueueAction & action_,
        RemoveFileFunc remove_file_func_,
        const NamesAndTypesList & requested_virtual_columns_,
        ContextPtr context_,
        const std::atomic<bool> &  shutdown_called_,
        std::shared_ptr<S3QueueLog> s3_queue_log_,
        const StorageID & storage_id_);

    ~StorageS3QueueSource() override;

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

private:
    const String name;
    const S3QueueAction action;
    const std::shared_ptr<S3QueueFilesMetadata> files_metadata;
    const std::shared_ptr<StorageS3Source> internal_source;
    const NamesAndTypesList requested_virtual_columns;
    const std::atomic<bool> & shutdown_called;
    const std::shared_ptr<S3QueueLog> s3_queue_log;
    const StorageID storage_id;
    const std::string s3_queue_user_id;

    RemoveFileFunc remove_file_func;
    Poco::Logger * log;

    using ReaderHolder = StorageS3Source::ReaderHolder;
    ReaderHolder reader;
    std::future<ReaderHolder> reader_future;
    size_t processed_rows_from_file = 0;
    std::shared_ptr<S3QueueFilesMetadata::FileStatus> file_status;

    void applyActionAfterProcessing(const String & path);
    void appendLogElement(const std::string & filename, const S3QueueFilesMetadata::FileStatus & file_status_, size_t processed_rows, bool processed);
};

}
#endif
