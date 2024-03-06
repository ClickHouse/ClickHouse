#pragma once
#include <Processors/SourceWithKeyCondition.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Storages/ObjectStorage/StorageObjectStorage_fwd_internal.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB
{

class SchemaCache;

class StorageObjectStorageSource : public SourceWithKeyCondition, WithContext
{
    friend class StorageS3QueueSource;
public:
    class IIterator;
    class ReadTaskIterator;
    class GlobIterator;
    class KeysIterator;

    StorageObjectStorageSource(
        String name_,
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration,
        const ReadFromFormatInfo & info,
        std::optional<FormatSettings> format_settings_,
        const StorageObjectStorageSettings & query_settings_,
        ContextPtr context_,
        UInt64 max_block_size_,
        std::shared_ptr<IIterator> file_iterator_,
        bool need_only_count_,
        SchemaCache & schema_cache_,
        std::shared_ptr<ThreadPool> reader_pool_,
        CurrentMetrics::Metric metric_threads_,
        CurrentMetrics::Metric metric_threads_active_,
        CurrentMetrics::Metric metric_threads_scheduled_);

    ~StorageObjectStorageSource() override;

    String getName() const override { return name; }

    void setKeyCondition(const ActionsDAGPtr & filter_actions_dag, ContextPtr context_) override
    {
        setKeyConditionImpl(filter_actions_dag, context_, read_from_format_info.source_header);
    }

    Chunk generate() override;

    static std::shared_ptr<IIterator> createFileIterator(
        ConfigurationPtr configuration,
        ObjectStoragePtr object_storage,
        bool distributed_processing,
        const ContextPtr & local_context,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns,
        ObjectInfos * read_keys,
        size_t list_object_keys_size,
        CurrentMetrics::Metric metric_threads_,
        CurrentMetrics::Metric metric_threads_active_,
        CurrentMetrics::Metric metric_threads_scheduled_,
        std::function<void(FileProgress)> file_progress_callback = {});

protected:
    const String name;
    ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const StorageObjectStorageSettings query_settings;
    const UInt64 max_block_size;
    const bool need_only_count;
    const ReadFromFormatInfo read_from_format_info;
    const std::shared_ptr<ThreadPool> create_reader_pool;
    ColumnsDescription columns_desc;
    std::shared_ptr<IIterator> file_iterator;
    SchemaCache & schema_cache;
    bool initialized = false;

    const CurrentMetrics::Metric metric_threads;
    const CurrentMetrics::Metric metric_threads_active;
    const CurrentMetrics::Metric metric_threads_scheduled;

    size_t total_rows_in_file = 0;
    LoggerPtr log = getLogger("StorageObjectStorageSource");

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

        const String & getRelativePath() const { return object_info->relative_path; }
        const ObjectInfo & getObjectInfo() const { return *object_info; }
        const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

    private:
        ObjectInfoPtr object_info;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<ISource> source;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader(size_t processor = 0);
    std::future<ReaderHolder> createReaderAsync(size_t processor = 0);
    std::unique_ptr<ReadBuffer> createReadBuffer(const String & key, size_t object_size);

    void addNumRowsToCache(const String & path, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const ObjectInfoPtr & object_info);
    void lazyInitialize(size_t processor);
};

class StorageObjectStorageSource::IIterator
{
public:
    virtual ~IIterator() = default;

    virtual size_t estimatedKeysCount() = 0;
    virtual ObjectInfoPtr next(size_t processor) = 0;
};

class StorageObjectStorageSource::ReadTaskIterator : public IIterator
{
public:
    ReadTaskIterator(
        const ReadTaskCallback & callback_,
        size_t max_threads_count,
        CurrentMetrics::Metric metric_threads_,
        CurrentMetrics::Metric metric_threads_active_,
        CurrentMetrics::Metric metric_threads_scheduled_);

    size_t estimatedKeysCount() override { return buffer.size(); }

    ObjectInfoPtr next(size_t) override;

private:
    ReadTaskCallback callback;
    ObjectInfos buffer;
    std::atomic_size_t index = 0;
};

class StorageObjectStorageSource::GlobIterator : public IIterator, WithContext
{
public:
    GlobIterator(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns_,
        ContextPtr context_,
        ObjectInfos * read_keys_,
        size_t list_object_keys_size,
        std::function<void(FileProgress)> file_progress_callback_ = {});

    ~GlobIterator() override = default;

    size_t estimatedKeysCount() override { return object_infos.size(); }

    ObjectInfoPtr next(size_t processor) override;

private:
    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;
    ActionsDAGPtr filter_dag;
    NamesAndTypesList virtual_columns;

    size_t index = 0;

    ObjectInfos object_infos;
    ObjectInfos * read_keys;
    ObjectStorageIteratorPtr object_storage_iterator;
    bool recursive{false};

    std::unique_ptr<re2::RE2> matcher;

    void createFilterAST(const String & any_key);
    bool is_finished = false;
    std::mutex next_mutex;

    std::function<void(FileProgress)> file_progress_callback;
};

class StorageObjectStorageSource::KeysIterator : public IIterator
{
public:
    KeysIterator(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        const NamesAndTypesList & virtual_columns_,
        ObjectInfos * read_keys_,
        std::function<void(FileProgress)> file_progress_callback = {});

    ~KeysIterator() override = default;

    size_t estimatedKeysCount() override { return keys.size(); }

    ObjectInfoPtr next(size_t processor) override;

private:
    const ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    const NamesAndTypesList virtual_columns;
    const std::function<void(FileProgress)> file_progress_callback;
    const std::vector<String> keys;
    std::atomic<size_t> index = 0;
};
}
