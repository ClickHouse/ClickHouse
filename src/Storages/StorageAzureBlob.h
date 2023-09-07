#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <re2/re2.h>
#include <Storages/IStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/SelectQueryInfo.h>

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
        bool distributed_processing_,
        ASTPtr partition_by_);

    static StorageAzureBlob::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);
    static AzureClientPtr createClient(StorageAzureBlob::Configuration configuration, bool is_read_only);

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

    bool supportsSubcolumns() const override { return true; }

    bool supportsSubsetOfColumns() const override;

    bool supportsTrivialCountOptimization() const override { return true; }

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static ColumnsDescription getTableStructureFromData(
        AzureObjectStorage * object_storage,
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx,
        bool distributed_processing = false);

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

private:
    std::string name;
    Configuration configuration;
    std::unique_ptr<AzureObjectStorage> object_storage;
    NamesAndTypesList virtual_columns;

    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;
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
            const NamesAndTypesList & virtual_columns_,
            ContextPtr context_,
            RelativePathsWithMetadata * outer_blobs_,
            std::function<void(FileProgress)> file_progress_callback_ = {});

        RelativePathWithMetadata next() override;
        ~GlobIterator() override = default;

    private:
        AzureObjectStorage * object_storage;
        std::string container;
        String blob_path_with_globs;
        ASTPtr query;
        ASTPtr filter_ast;
        NamesAndTypesList virtual_columns;

        size_t index = 0;

        RelativePathsWithMetadata blobs_with_metadata;
        RelativePathsWithMetadata * outer_blobs;
        ObjectStorageIteratorPtr object_storage_iterator;
        bool recursive{false};

        std::unique_ptr<re2::RE2> matcher;

        void createFilterAST(const String & any_key);
        bool is_finished = false;
        bool is_initialized = false;
        std::mutex next_mutex;

        std::function<void(FileProgress)> file_progress_callback;
    };

    class ReadIterator : public IIterator
    {
    public:
        explicit ReadIterator(ContextPtr context_,
                              const ReadTaskCallback & callback_)
            : IIterator(context_), callback(callback_) { }
        RelativePathWithMetadata next() override
        {
            return {callback(), {}};
        }

    private:
        ReadTaskCallback callback;
    };

    class KeysIterator : public IIterator
    {
    public:
        KeysIterator(
            AzureObjectStorage * object_storage_,
            const std::string & container_,
            const Strings & keys_,
            ASTPtr query_,
            const NamesAndTypesList & virtual_columns_,
            ContextPtr context_,
            RelativePathsWithMetadata * outer_blobs,
            std::function<void(FileProgress)> file_progress_callback = {});

        RelativePathWithMetadata next() override;
        ~KeysIterator() override = default;

    private:
        AzureObjectStorage * object_storage;
        std::string container;
        RelativePathsWithMetadata keys;

        ASTPtr query;
        NamesAndTypesList virtual_columns;

        std::atomic<size_t> index = 0;
    };

    StorageAzureBlobSource(
        const ReadFromFormatInfo & info,
        const String & format_,
        String name_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        UInt64 max_block_size_,
        String compression_hint_,
        AzureObjectStorage * object_storage_,
        const String & container_,
        const String & connection_url_,
        std::shared_ptr<IIterator> file_iterator_,
        bool need_only_count_,
        const SelectQueryInfo & query_info_);
    ~StorageAzureBlobSource() override;

    Chunk generate() override;

    String getName() const override;

private:
    void addNumRowsToCache(const String & path, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const RelativePathWithMetadata & path_with_metadata);

    NamesAndTypesList requested_columns;
    NamesAndTypesList requested_virtual_columns;
    String format;
    String name;
    Block sample_block;
    std::optional<FormatSettings> format_settings;
    ColumnsDescription columns_desc;
    UInt64 max_block_size;
    String compression_hint;
    AzureObjectStorage * object_storage;
    String container;
    String connection_url;
    std::shared_ptr<IIterator> file_iterator;
    bool need_only_count;
    size_t total_rows_in_file = 0;
    SelectQueryInfo query_info;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            RelativePathWithMetadata relative_path_with_metadata_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<ISource> source_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : relative_path_with_metadata(std::move(relative_path_with_metadata_))
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
            relative_path_with_metadata = std::move(other.relative_path_with_metadata);
            return *this;
        }

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        const String & getRelativePath() const { return relative_path_with_metadata.relative_path; }
        const RelativePathWithMetadata & getRelativePathWithMetadata() const { return relative_path_with_metadata; }
        const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

    private:
        RelativePathWithMetadata relative_path_with_metadata;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<ISource> source;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;

    Poco::Logger * log = &Poco::Logger::get("StorageAzureBlobSource");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createAzureReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncAzureReadBuffer(
        const String & key, const ReadSettings & read_settings, size_t object_size);
};

}

#endif
