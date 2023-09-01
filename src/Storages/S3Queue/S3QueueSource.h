#pragma once
#include "config.h"

#if USE_AWS_S3

#    include <Core/Types.h>

#    include <Compression/CompressionInfo.h>

#    include <Storages/IStorage.h>
#    include <Storages/S3Queue/S3QueueFilesMetadata.h>
#    include <Storages/StorageS3.h>
#    include <Storages/StorageS3Settings.h>
#    include <Storages/prepareReadingFromFormat.h>

#    include <IO/CompressionMethod.h>
#    include <IO/S3/getObjectInfo.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/threadPoolCallbackRunner.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <Processors/ISource.h>
#    include <Storages/Cache/SchemaCache.h>
#    include <Storages/StorageConfiguration.h>
#    include <Poco/URI.h>
#    include <Common/ZooKeeper/ZooKeeper.h>
#    include <Common/logger_useful.h>


namespace DB
{


class StorageS3QueueSource : public ISource, WithContext
{
public:
    using IIterator = StorageS3Source::IIterator;
    using DisclosedGlobIterator = StorageS3Source::DisclosedGlobIterator;
    using KeysWithInfo = StorageS3Source::KeysWithInfo;
    using KeyWithInfo = StorageS3Source::KeyWithInfo;
    class QueueGlobIterator : public IIterator
    {
    public:
        QueueGlobIterator(
            const S3::Client & client_,
            const S3::URI & globbed_uri_,
            ASTPtr query,
            const NamesAndTypesList & virtual_columns,
            ContextPtr context,
            UInt64 & max_poll_size_,
            const S3Settings::RequestSettings & request_settings_ = {});

        KeyWithInfo next() override;

        Strings
        filterProcessingFiles(const S3QueueMode & engine_mode, std::unordered_set<String> & exclude_keys, const String & max_file = "");

    private:
        UInt64 max_poll_size;
        KeysWithInfo keys_buf;
        KeysWithInfo processing_keys;
        mutable std::mutex mutex;
        std::unique_ptr<DisclosedGlobIterator> glob_iterator;
        std::vector<KeyWithInfo>::iterator processing_iterator;

        Poco::Logger * log = &Poco::Logger::get("StorageS3QueueSourceIterator");
    };

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    StorageS3QueueSource(
        const ReadFromFormatInfo & info,
        const String & format,
        String name_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        UInt64 max_block_size_,
        const S3Settings::RequestSettings & request_settings_,
        String compression_hint_,
        const std::shared_ptr<const S3::Client> & client_,
        const String & bucket,
        const String & version_id,
        const String & url_host_and_port,
        std::shared_ptr<IIterator> file_iterator_,
        std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
        const S3QueueAction & action_,
        size_t download_thread_num);

    ~StorageS3QueueSource() override;

    String getName() const override;

    Chunk generate() override;


private:
    String name;
    String bucket;
    String version_id;
    String format;
    ColumnsDescription columns_desc;
    S3Settings::RequestSettings request_settings;
    std::shared_ptr<const S3::Client> client;

    std::shared_ptr<S3QueueFilesMetadata> files_metadata;
    using ReaderHolder = StorageS3Source::ReaderHolder;
    ReaderHolder reader;

    NamesAndTypesList requested_virtual_columns;
    NamesAndTypesList requested_columns;
    std::shared_ptr<IIterator> file_iterator;
    const S3QueueAction action;

    Poco::Logger * log = &Poco::Logger::get("StorageS3QueueSource");

    std::future<ReaderHolder> reader_future;

    mutable std::mutex mutex;

    std::shared_ptr<StorageS3Source> internal_source;
    void deleteProcessedObject(const String & file_path);
    void applyActionAfterProcessing(const String & file_path);
};

}
#endif
