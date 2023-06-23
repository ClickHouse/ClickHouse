#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <re2/re2.h>
#include <Storages/IStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/NamedCollectionsHelpers.h>

namespace DB
{

class StorageAzureBlob : public IStorage
{
public:

    using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
    using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

    struct Configuration : public StatelessTableEngineConfiguration
    {
        Configuration() = default;

        String getPath() const { return blob_path; }

        bool update(ContextPtr context);

        void connect(ContextPtr context);

        bool withGlobs() const { return blob_path.find_first_of("*?{") != std::string::npos; }

        bool withWildcard() const
        {
            static const String PARTITION_ID_WILDCARD = "{_partition_id}";
            return blobs_paths.back().find(PARTITION_ID_WILDCARD) != String::npos;
        }

        Poco::URI getConnectionURL() const;

        std::string connection_url;
        bool is_connection_string;

        std::optional<std::string> account_name;
        std::optional<std::string> account_key;

        std::string container;
        std::string blob_path;
        std::vector<String> blobs_paths;
    };

    StorageAzureBlob(
        const Configuration & configuration_,
        std::unique_ptr<AzureObjectStorage> && object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        ASTPtr partition_by_);

    static StorageAzureBlob::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);
    static AzureClientPtr createClient(StorageAzureBlob::Configuration configuration);

    static AzureObjectStorage::SettingsPtr createSettings(ContextPtr local_context);

    static void processNamedCollectionResult(StorageAzureBlob::Configuration & configuration, const NamedCollection & collection);

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names &,
        const StorageSnapshotPtr &,
        SelectQueryInfo &,
        ContextPtr,
        QueryProcessingStage::Enum,
        size_t,
        size_t) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr context, bool /*async_insert*/) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    bool supportsSubcolumns() const override;

    bool supportsSubsetOfColumns() const override;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static ColumnsDescription getTableStructureFromData(
        AzureObjectStorage * object_storage,
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

private:
    std::string name;
    Configuration configuration;
    std::unique_ptr<AzureObjectStorage> object_storage;
    NamesAndTypesList virtual_columns;
    Block virtual_block;

    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;


    static std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const RelativePathsWithMetadata::const_iterator & begin,
        const RelativePathsWithMetadata::const_iterator & end,
        const StorageAzureBlob::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    static void addColumnsToCache(
        const RelativePathsWithMetadata & keys,
        const ColumnsDescription & columns,
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const String & format_name,
        const ContextPtr & ctx);


};

class StorageAzureBlobSource : public ISource, WithContext
{
public:
    class IIterator : public WithContext
    {
    public:
        IIterator(ContextPtr context_):WithContext(context_) {}
        virtual ~IIterator() = default;
        virtual RelativePathWithMetadata next() = 0;
        virtual size_t getTotalSize() const = 0;

        RelativePathWithMetadata operator ()() { return next(); }
    };

    class GlobIterator : public IIterator
    {
    public:
        GlobIterator(
            AzureObjectStorage * object_storage_,
            const std::string & container_,
            String blob_path_with_globs_,
            ASTPtr query_,
            const Block & virtual_header_,
            ContextPtr context_,
            RelativePathsWithMetadata * outer_blobs_);

        RelativePathWithMetadata next() override;
        size_t getTotalSize() const override;
        ~GlobIterator() override = default;

     private:
        AzureObjectStorage * object_storage;
        std::string container;
        String blob_path_with_globs;
        ASTPtr query;
        ASTPtr filter_ast;
        Block virtual_header;

        std::atomic<size_t> index = 0;
        std::atomic<size_t> total_size = 0;

        RelativePathsWithMetadata blobs_with_metadata;
        RelativePathsWithMetadata * outer_blobs;
        ObjectStorageIteratorPtr object_storage_iterator;
        bool recursive{false};

        std::unique_ptr<re2::RE2> matcher;

        void createFilterAST(const String & any_key);
        std::atomic<bool> is_finished = false;
        std::atomic<bool> is_initialized = false;
        std::mutex next_mutex;
    };

    class KeysIterator : public IIterator
    {
    public:
        KeysIterator(
            AzureObjectStorage * object_storage_,
            const std::string & container_,
            Strings keys_,
            ASTPtr query_,
            const Block & virtual_header_,
            ContextPtr context_,
            RelativePathsWithMetadata * outer_blobs_);

        RelativePathWithMetadata next() override;
        size_t getTotalSize() const override;
        ~KeysIterator() override = default;

    private:
        AzureObjectStorage * object_storage;
        std::string container;
        RelativePathsWithMetadata keys;

        ASTPtr query;
        ASTPtr filter_ast;
        Block virtual_header;

        std::atomic<size_t> index = 0;
        std::atomic<size_t> total_size = 0;

        RelativePathsWithMetadata * outer_blobs;
    };

    StorageAzureBlobSource(
        const std::vector<NameAndTypePair> & requested_virtual_columns_,
        const String & format_,
        String name_,
        const Block & sample_block_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const ColumnsDescription & columns_,
        UInt64 max_block_size_,
        String compression_hint_,
        AzureObjectStorage * object_storage_,
        const String & container_,
        std::shared_ptr<IIterator> file_iterator_);

    ~StorageAzureBlobSource() override;

    Chunk generate() override;

    String getName() const override;

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

private:
    std::vector<NameAndTypePair> requested_virtual_columns;
    String format;
    String name;
    Block sample_block;
    std::optional<FormatSettings> format_settings;
    ColumnsDescription columns_desc;
    UInt64 max_block_size;
    String compression_hint;
    AzureObjectStorage * object_storage;
    String container;
    std::shared_ptr<IIterator> file_iterator;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            String path_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : path(std::move(path_))
            , read_buf(std::move(read_buf_))
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
            read_buf = std::move(other.read_buf);
            path = std::move(other.path);
            return *this;
        }

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        const String & getPath() const { return path; }

    private:
        String path;
        std::unique_ptr<ReadBuffer> read_buf;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;

    Poco::Logger * log = &Poco::Logger::get("StorageAzureBlobSource");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    UInt64 total_rows_approx_max = 0;
    size_t total_rows_count_times = 0;
    UInt64 total_rows_approx_accumulated = 0;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createAzureReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncAzureReadBuffer(
        const String & key, const ReadSettings & read_settings, size_t object_size);
};

}

#endif
