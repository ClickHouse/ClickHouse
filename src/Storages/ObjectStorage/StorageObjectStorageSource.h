#pragma once
#include <Processors/ISource.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>


namespace DB
{
template <typename StorageSettings>
class StorageObjectStorageSource : public ISource, WithContext
{
    friend class StorageS3QueueSource;
public:
    using Source = StorageObjectStorageSource<StorageSettings>;
    using Storage = StorageObjectStorage<StorageSettings>;
    using ObjectInfo = Storage::ObjectInfo;
    using ObjectInfoPtr = Storage::ObjectInfoPtr;
    using ObjectInfos = Storage::ObjectInfos;

    class IIterator : public WithContext
    {
    public:
        virtual ~IIterator() = default;

        virtual size_t estimatedKeysCount() = 0;
        virtual ObjectInfoPtr next(size_t processor) = 0;
    };

    class ReadTaskIterator;
    class GlobIterator;
    class KeysIterator;

    StorageObjectStorageSource(
        String name_,
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration,
        const ReadFromFormatInfo & info,
        std::optional<FormatSettings> format_settings_,
        ContextPtr context_,
        UInt64 max_block_size_,
        std::shared_ptr<IIterator> file_iterator_,
        bool need_only_count_);

    ~StorageObjectStorageSource() override;

    String getName() const override { return name; }

    Chunk generate() override;

    static std::shared_ptr<IIterator> createFileIterator(
        Storage::ConfigurationPtr configuration,
        ObjectStoragePtr object_storage,
        bool distributed_processing,
        const ContextPtr & local_context,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns,
        ObjectInfos * read_keys,
        std::function<void(FileProgress)> file_progress_callback = {});

protected:
    void addNumRowsToCache(const String & path, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const ObjectInfoPtr & object_info);

    const String name;
    ObjectStoragePtr object_storage;
    const Storage::ConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const UInt64 max_block_size;
    const bool need_only_count;
    const ReadFromFormatInfo read_from_format_info;

    ColumnsDescription columns_desc;
    std::shared_ptr<IIterator> file_iterator;
    size_t total_rows_in_file = 0;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            ObjectInfoPtr object_info_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<ISource> source_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : object_info(std::move(object_info_))
            , read_buf(std::move(read_buf_))
            , source(std::move(source_))
            , pipeline(std::move(pipeline_))
            , reader(std::move(reader_))
        {
        }

        ReaderHolder() = default;
        ReaderHolder(const ReaderHolder & other) = delete;
        ReaderHolder & operator=(const ReaderHolder & other) = delete;
        ReaderHolder(ReaderHolder && other) noexcept { *this = std::move(other); }

        ReaderHolder & operator=(ReaderHolder && other) noexcept
        {
            /// The order of destruction is important.
            /// reader uses pipeline, pipeline uses read_buf.
            reader = std::move(other.reader);
            pipeline = std::move(other.pipeline);
            source = std::move(other.source);
            read_buf = std::move(other.read_buf);
            object_info = std::move(other.object_info);
            return *this;
        }

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
    LoggerPtr log = getLogger("StorageObjectStorageSource");
    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader(size_t processor = 0);
    std::future<ReaderHolder> createReaderAsync(size_t processor = 0);

    std::unique_ptr<ReadBuffer> createReadBuffer(const String & key, size_t object_size);
};

template <typename StorageSettings>
class StorageObjectStorageSource<StorageSettings>::ReadTaskIterator : public IIterator
{
public:
    explicit ReadTaskIterator(const ReadTaskCallback & callback_) : callback(callback_) {}

    size_t estimatedKeysCount() override { return 0; } /// TODO FIXME

    ObjectInfoPtr next(size_t) override { return std::make_shared<ObjectInfo>( callback(), ObjectMetadata{} ); }

private:
    ReadTaskCallback callback;
};

template <typename StorageSettings>
class StorageObjectStorageSource<StorageSettings>::GlobIterator : public IIterator
{
public:
    GlobIterator(
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration_,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns_,
        ObjectInfos * read_keys_,
        std::function<void(FileProgress)> file_progress_callback_ = {});

    ~GlobIterator() override = default;

    size_t estimatedKeysCount() override { return object_infos.size(); }

    ObjectInfoPtr next(size_t processor) override;

private:
    ObjectStoragePtr object_storage;
    Storage::ConfigurationPtr configuration;
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

template <typename StorageSettings>
class StorageObjectStorageSource<StorageSettings>::KeysIterator : public IIterator
{
public:
    KeysIterator(
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration_,
        const NamesAndTypesList & virtual_columns_,
        ObjectInfos * read_keys_,
        std::function<void(FileProgress)> file_progress_callback = {});

    ~KeysIterator() override = default;

    size_t estimatedKeysCount() override { return keys.size(); }

    ObjectInfoPtr next(size_t processor) override;

private:
    const ObjectStoragePtr object_storage;
    const Storage::ConfigurationPtr configuration;
    const NamesAndTypesList virtual_columns;
    const std::function<void(FileProgress)> file_progress_callback;
    const std::vector<String> keys;
    std::atomic<size_t> index = 0;
};
}
