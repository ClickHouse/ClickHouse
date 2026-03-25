#pragma once

#include <optional>
#include <boost/noncopyable.hpp>
#include <Interpreters/Context_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/FormatFilterInfo.h>
#include <Common/threadPoolCallbackRunner.h>

namespace DB
{

class SchemaCache;
class IcebergMetadata;

class IcebergSource : public ISource
{
public:
    IcebergSource(
        String name_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration,
        const StorageObjectStorageTableOptions & table_options_,
        StorageSnapshotPtr storage_snapshot_,
        const ReadFromFormatInfo & info,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_,
        UInt64 max_block_size_,
        std::shared_ptr<IObjectIterator> file_iterator_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        FormatFilterInfoPtr format_filter_info_,
        bool need_only_count_,
        IcebergMetadata * metadata_);

    ~IcebergSource() override;

    String getName() const override { return name; }

    Chunk generate() override;

    void onFinish() override { parser_shared_resources->finishStream(); }

    static std::string getUniqueStoragePathIdentifier(
        const StorageObjectStorageConfiguration & configuration,
        const ObjectInfo & object_info,
        bool include_connection_info = true);

protected:
    const String name;
    ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationPtr configuration;
    StorageObjectStorageTableOptions table_options;
    StorageSnapshotPtr storage_snapshot;
    const ContextPtr read_context;
    const std::optional<FormatSettings> format_settings;
    const UInt64 max_block_size;
    const bool need_only_count;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;

    ReadFromFormatInfo read_from_format_info;
    const std::shared_ptr<ThreadPool> create_reader_pool;

    std::shared_ptr<IObjectIterator> file_iterator;
    SchemaCache & schema_cache;
    bool initialized = false;
    size_t total_rows_in_file = 0;
    size_t total_files_read = 0;
    LoggerPtr log = getLogger("IcebergSource");

    IcebergMetadata * metadata;

    struct ReaderHolder : private boost::noncopyable
    {
    public:
        ReaderHolder(
            ObjectInfoPtr object_info_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<ISource> source_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_);

        ReaderHolder() = default;
        ReaderHolder(ReaderHolder && other) noexcept { *this = std::move(other); }
        ReaderHolder & operator=(ReaderHolder && other) noexcept;

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }

        ObjectInfoPtr getObjectInfo() const { return object_info; }
        const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

    private:
        ObjectInfoPtr object_info;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<ISource> source;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;
    ThreadPoolCallbackRunnerUnsafe<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    /// Recreate ReadBuffer and Pipeline for each file.
    static ReaderHolder createReader(
        size_t processor,
        const std::shared_ptr<IObjectIterator> & file_iterator,
        const StorageObjectStorageConfigurationPtr & configuration,
        const StorageObjectStorageTableOptions & table_options,
        const ObjectStoragePtr & object_storage,
        ReadFromFormatInfo & read_from_format_info,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context_,
        SchemaCache * schema_cache,
        const LoggerPtr & log,
        size_t max_block_size,
        FormatParserSharedResourcesPtr parser_shared_resources,
        FormatFilterInfoPtr format_filter_info,
        bool need_only_count,
        IcebergMetadata * metadata);

    ReaderHolder createReader();

    std::future<ReaderHolder> createReaderAsync();

    void addNumRowsToCache(const ObjectInfo & object_info, size_t num_rows);
    void lazyInitialize();
};

}
