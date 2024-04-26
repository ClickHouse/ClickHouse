#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Compression/CompressionInfo.h>
#include <Core/Types.h>
#include <IO/CompressionMethod.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/SourceWithKeyCondition.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Poco/URI.h>
#include <Common/threadPoolCallbackRunner.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

class PullingPipelineExecutor;
class NamedCollection;

class StorageS3Source : public SourceWithKeyCondition, WithContext
{
public:

    struct KeyWithInfo
    {
        KeyWithInfo() = default;

        explicit KeyWithInfo(String key_, std::optional<S3::ObjectInfo> info_ = std::nullopt)
            : key(std::move(key_)), info(std::move(info_)) {}

        virtual ~KeyWithInfo() = default;

        String key;
        std::optional<S3::ObjectInfo> info;
    };
    using KeyWithInfoPtr = std::shared_ptr<KeyWithInfo>;

    using KeysWithInfo = std::vector<KeyWithInfoPtr>;

    class IIterator
    {
    public:
        virtual ~IIterator() = default;
        virtual KeyWithInfoPtr next(size_t idx = 0) = 0; /// NOLINT

        /// Estimates how many streams we need to process all files.
        /// If keys count >= max_threads_count, the returned number may not represent the actual number of the keys.
        /// Intended to be called before any next() calls, may underestimate otherwise
        /// fixme: May underestimate if the glob has a strong filter, so there are few matches among the first 1000 ListObjects results.
        virtual size_t estimatedKeysCount() = 0;

        KeyWithInfoPtr operator ()() { return next(); }
    };

    class DisclosedGlobIterator : public IIterator
    {
    public:
        DisclosedGlobIterator(
            const S3::Client & client_,
            const S3::URI & globbed_uri_,
            const ActionsDAG::Node * predicate,
            const NamesAndTypesList & virtual_columns,
            const ContextPtr & context,
            KeysWithInfo * read_keys_ = nullptr,
            const S3Settings::RequestSettings & request_settings_ = {},
            std::function<void(FileProgress)> progress_callback_ = {});

        KeyWithInfoPtr next(size_t idx = 0) override; /// NOLINT
        size_t estimatedKeysCount() override;

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
            KeysWithInfo * read_keys = nullptr,
            std::function<void(FileProgress)> progress_callback_ = {});

        KeyWithInfoPtr next(size_t idx = 0) override; /// NOLINT
        size_t estimatedKeysCount() override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class ReadTaskIterator : public IIterator
    {
    public:
        explicit ReadTaskIterator(const ReadTaskCallback & callback_, size_t max_threads_count);

        KeyWithInfoPtr next(size_t idx = 0) override; /// NOLINT
        size_t estimatedKeysCount() override;

    private:
        KeysWithInfo buffer;
        std::atomic_size_t index = 0;

        ReadTaskCallback callback;
    };

    StorageS3Source(
        const ReadFromFormatInfo & info,
        const String & format,
        String name_,
        const ContextPtr & context_,
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
        bool need_only_count_);

    ~StorageS3Source() override;

    String getName() const override;

    void setKeyCondition(const ActionsDAGPtr & filter_actions_dag, ContextPtr context_) override
    {
        setKeyConditionImpl(filter_actions_dag, context_, sample_block);
    }

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

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            KeyWithInfoPtr key_with_info_,
            String bucket_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<ISource> source_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : key_with_info(key_with_info_)
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
        String getPath() const { return fs::path(bucket) / key_with_info->key; }
        const String & getFile() const { return key_with_info->key; }
        const KeyWithInfo & getKeyWithInfo() const { return *key_with_info; }
        std::optional<size_t> getFileSize() const { return key_with_info->info ? std::optional(key_with_info->info->size) : std::nullopt; }

        const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

    private:
        KeyWithInfoPtr key_with_info;
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

    LoggerPtr log = getLogger("StorageS3Source");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunnerUnsafe<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;
    std::atomic<bool> initialized{false};

    size_t total_rows_in_file = 0;

    /// Notice: we should initialize reader and future_reader lazily in generate to make sure key_condition
    /// is set before createReader is invoked for key_condition is read in createReader.
    void lazyInitialize(size_t idx = 0);

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader(size_t idx = 0);
    std::future<ReaderHolder> createReaderAsync(size_t idx = 0);

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

        bool update(const ContextPtr & context);

        void connect(const ContextPtr & context);

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
        std::vector<String> keys;
    };

    StorageS3(
        const Configuration & configuration_,
        const ContextPtr & context_,
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

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    bool supportsPartitionBy() const override;

    static void processNamedCollectionResult(StorageS3::Configuration & configuration, const NamedCollection & collection);

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static StorageS3::Configuration getConfiguration(ASTs & engine_args, const ContextPtr & local_context, bool get_format_from_file = true);

    static ColumnsDescription getTableStructureFromData(
        const StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromData(
        const StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    using KeysWithInfo = StorageS3Source::KeysWithInfo;

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

protected:
    virtual Configuration updateConfigurationAndGetCopy(const ContextPtr & local_context);

    virtual void updateConfiguration(const ContextPtr & local_context);

    void useConfiguration(const Configuration & new_configuration);

    const Configuration & getConfiguration();

private:
    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;
    friend class StorageS3Queue;
    friend class ReadFromStorageS3Step;

    Configuration configuration;
    std::mutex configuration_update_mutex;

    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromDataImpl(
        std::optional<String> format,
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    bool supportsSubcolumns() const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;
};

}

#endif
