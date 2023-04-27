#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageS3.h>

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
#include <Common/ZooKeeper/ZooKeeper.h>


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
            const Block & virtual_header,
            ContextPtr context,
            KeysWithInfo * read_keys_ = nullptr,
            const S3Settings::RequestSettings & request_settings_ = {});

        KeyWithInfo next() override;
        size_t getTotalSize() const override;

        Strings setProcessing(String & mode, std::unordered_set<String> & exclude_keys);
    private:
        size_t max_poll_size = 10;
        const String bucket;
        KeysWithInfo keys_buf;
        KeysWithInfo processing_keys;
        mutable std::mutex mutex;
        std::unique_ptr<DisclosedGlobIterator> glob_iterator;
        std::vector<KeyWithInfo>::iterator processing_iterator;

        Poco::Logger * log = &Poco::Logger::get("StorageS3QueueSourceIterator");
    };

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    StorageS3QueueSource(
        const std::vector<NameAndTypePair> & requested_virtual_columns_,
        const String & format,
        String name_,
        const Block & sample_block,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const ColumnsDescription & columns_,
        UInt64 max_block_size_,
        const S3Settings::RequestSettings & request_settings_,
        String compression_hint_,
        const std::shared_ptr<const S3::Client> & client_,
        const String & bucket,
        const String & version_id,
        std::shared_ptr<IIterator> file_iterator_,
        zkutil::ZooKeeperPtr current_zookeeper,
        const String & zookeeper_path_,
        size_t download_thread_num);

    ~StorageS3QueueSource() override;

    String getName() const override;

    Chunk generate() override;

    static std::unordered_set<String> parseCollection(String & files);


private:
    String name;
    String bucket;
    String version_id;
    String format;
    ColumnsDescription columns_desc;
    UInt64 max_block_size;
    S3Settings::RequestSettings request_settings;
    String compression_hint;
    std::shared_ptr<const S3::Client> client;
    Block sample_block;
    std::optional<FormatSettings> format_settings;

    using ReaderHolder = StorageS3Source::ReaderHolder;
    ReaderHolder reader;

    std::vector<NameAndTypePair> requested_virtual_columns;
    std::shared_ptr<IIterator> file_iterator;
    size_t download_thread_num = 1;

    Poco::Logger * log = &Poco::Logger::get("StorageS3QueueSource");

    zkutil::ZooKeeperPtr zookeeper;
    const String zookeeper_path;

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    UInt64 total_rows_approx_max = 0;
    size_t total_rows_count_times = 0;
    UInt64 total_rows_approx_accumulated = 0;

    mutable std::mutex mutex;


    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createS3ReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size);

    void setFileProcessed(const String & file_path);
    void setFileFailed(const String & file_path);

};

}
#endif
