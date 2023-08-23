#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>

#include <Processors/ISource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Poco/URI.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/CompressionMethod.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/prepareReadingFromFormat.h>

namespace Aws::S3
{
    class Client;
}

namespace DB
{

class PullingPipelineExecutor;
class NamedCollection;

class StorageS3Source : public ISource, WithContext
{
public:

    struct KeyWithInfo
    {
        KeyWithInfo() = default;
        KeyWithInfo(String key_, std::optional<S3::ObjectInfo> info_)
            : key(std::move(key_)), info(std::move(info_))
        {
        }

        String key;
        std::optional<S3::ObjectInfo> info;
    };

    using KeysWithInfo = std::vector<KeyWithInfo>;

    class IIterator
    {
    public:
        virtual ~IIterator() = default;
        virtual KeyWithInfo next() = 0;

        KeyWithInfo operator ()() { return next(); }
    };

    class DisclosedGlobIterator : public IIterator
    {
    public:
        DisclosedGlobIterator(
            const S3::Client & client_,
            const S3::URI & globbed_uri_,
            ASTPtr query,
            const NamesAndTypesList & virtual_columns,
            ContextPtr context,
            KeysWithInfo * read_keys_ = nullptr,
            const S3Settings::RequestSettings & request_settings_ = {},
            std::function<void(FileProgress)> progress_callback_ = {});

        KeyWithInfo next() override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class KeysIterator : public IIterator
    {
    public:
        explicit KeysIterator(
            const S3::Client & client_,
            const std::string & version_id_,
            const std::vector<String> & keys_,
            const String & bucket_,
            const S3Settings::RequestSettings & request_settings_,
            ASTPtr query,
            const NamesAndTypesList & virtual_columns,
            ContextPtr context,
            KeysWithInfo * read_keys = nullptr,
            std::function<void(FileProgress)> progress_callback_ = {});

        KeyWithInfo next() override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class ReadTaskIterator : public IIterator
    {
    public:
        explicit ReadTaskIterator(const ReadTaskCallback & callback_) : callback(callback_) {}

        KeyWithInfo next() override { return {callback(), {}}; }

    private:
        ReadTaskCallback callback;
    };

    StorageS3Source(
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
        size_t max_parsing_threads,
        bool need_only_count_,
        std::optional<SelectQueryInfo> query_info);

    ~StorageS3Source() override;

    String getName() const override;

    Chunk generate() override;

private:
    friend class StorageS3QueueSource;

    String name;
    String bucket;
    String version_id;
    String url_host_and_port;
    String format;
    ColumnsDescription columns_desc;
    NamesAndTypesList requested_columns;
    UInt64 max_block_size;
    S3Settings::RequestSettings request_settings;
    String compression_hint;
    std::shared_ptr<const S3::Client> client;
    Block sample_block;
    std::optional<FormatSettings> format_settings;
    std::optional<SelectQueryInfo> query_info;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            KeyWithInfo key_with_info_,
            String bucket_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<ISource> source_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : key_with_info(std::move(key_with_info_))
            , bucket(std::move(bucket_))
            , read_buf(std::move(read_buf_))
            , source(std::move(source_))
            , pipeline(std::move(pipeline_))
            , reader(std::move(reader_))
        {
        }

        ReaderHolder() = default;
        ReaderHolder(const ReaderHolder & other) = delete;
        ReaderHolder & operator=(const ReaderHolder & other) = delete;

        ReaderHolder(ReaderHolder && other) noexcept
        {
            *this = std::move(other);
        }

        ReaderHolder & operator=(ReaderHolder && other) noexcept
        {
            /// The order of destruction is important.
            /// reader uses pipeline, pipeline uses read_buf.
            reader = std::move(other.reader);
            pipeline = std::move(other.pipeline);
            source = std::move(other.source);
            read_buf = std::move(other.read_buf);
            key_with_info = std::move(other.key_with_info);
            bucket = std::move(other.bucket);
            return *this;
        }

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        String getPath() const { return fs::path(bucket) / key_with_info.key; }
        const String & getFile() const { return key_with_info.key; }
        const KeyWithInfo & getKeyWithInfo() const { return key_with_info; }

        const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

    private:
        KeyWithInfo key_with_info;
        String bucket;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<ISource> source;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;

    NamesAndTypesList requested_virtual_columns;
    std::shared_ptr<IIterator> file_iterator;
    size_t max_parsing_threads = 1;
    bool need_only_count;

    Poco::Logger * log = &Poco::Logger::get("StorageS3Source");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    size_t total_rows_in_file = 0;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createS3ReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size);

    void addNumRowsToCache(const String & key, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const KeyWithInfo & key_with_info);
};

/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public IStorage
{
public:
    struct Configuration : public StatelessTableEngineConfiguration
    {
        Configuration() = default;

        String getPath() const { return url.key; }

        bool update(ContextPtr context);

        void connect(ContextPtr context);

        bool withGlobs() const { return url.key.find_first_of("*?{") != std::string::npos; }

        bool withWildcard() const
        {
            static const String PARTITION_ID_WILDCARD = "{_partition_id}";
            return url.bucket.find(PARTITION_ID_WILDCARD) != String::npos
                || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
        }

        S3::URI url;
        S3::AuthSettings auth_settings;
        S3Settings::RequestSettings request_settings;
        /// If s3 configuration was passed from ast, then it is static.
        /// If from config - it can be changed with config reload.
        bool static_configuration = true;
        /// Headers from ast is a part of static configuration.
        HTTPHeaderEntries headers_from_ast;

        std::shared_ptr<const S3::Client> client;
        std::shared_ptr<const S3::Client> client_with_long_timeout;
        std::vector<String> keys;
    };

    StorageS3(
        const Configuration & configuration_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr);

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    static void processNamedCollectionResult(StorageS3::Configuration & configuration, const NamedCollection & collection);

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static StorageS3::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file = true);

    static ColumnsDescription getTableStructureFromData(
        const StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

    using KeysWithInfo = StorageS3Source::KeysWithInfo;

    static std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const KeysWithInfo::const_iterator & begin,
        const KeysWithInfo::const_iterator & end,
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    static void addColumnsToCache(
        const KeysWithInfo & keys,
        const Configuration & configuration,
        const ColumnsDescription & columns,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    bool supportsTrivialCountOptimization() const override { return true; }

protected:
    virtual Configuration updateConfigurationAndGetCopy(ContextPtr local_context);

    virtual void updateConfiguration(ContextPtr local_context);

    void useConfiguration(const Configuration & new_configuration);

    const Configuration & getConfiguration();

private:
    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;
    friend class StorageS3Queue;

    Configuration configuration;
    std::mutex configuration_update_mutex;
    NamesAndTypesList virtual_columns;

    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;

    static std::shared_ptr<StorageS3Source::IIterator> createFileIterator(
        const Configuration & configuration,
        bool distributed_processing,
        ContextPtr local_context,
        ASTPtr query,
        const NamesAndTypesList & virtual_columns,
        KeysWithInfo * read_keys = nullptr,
        std::function<void(FileProgress)> progress_callback = {});

    static ColumnsDescription getTableStructureFromDataImpl(
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

    bool supportsSubcolumns() const override { return true; }

    bool supportsSubsetOfColumns() const override;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;
};

}

#endif
