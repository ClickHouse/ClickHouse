#pragma once
#include <Common/re2.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Archives/IArchiveReader.h>
#include <Processors/SourceWithKeyCondition.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/PartitionColumns.h>


namespace DB
{

class SchemaCache;

class StorageObjectStorageSource : public SourceWithKeyCondition, WithContext
{
    friend class ObjectStorageQueueSource;
public:
    using Configuration = StorageObjectStorage::Configuration;
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;
    using ObjectInfo = StorageObjectStorage::ObjectInfo;
    using ObjectInfos = StorageObjectStorage::ObjectInfos;
    using ObjectInfoPtr = StorageObjectStorage::ObjectInfoPtr;

    class IIterator;
    class ReadTaskIterator;
    class GlobIterator;
    class KeysIterator;
    class ArchiveIterator;

    StorageObjectStorageSource(
        String name_,
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration,
        const ReadFromFormatInfo & info,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_,
        UInt64 max_block_size_,
        std::shared_ptr<IIterator> file_iterator_,
        size_t max_parsing_threads_,
        bool need_only_count_);

    ~StorageObjectStorageSource() override;

    String getName() const override { return name; }

    void setKeyCondition(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context_) override;

    Chunk generate() override;

    static std::shared_ptr<IIterator> createFileIterator(
        ConfigurationPtr configuration,
        const StorageObjectStorage::QuerySettings & query_settings,
        ObjectStoragePtr object_storage,
        bool distributed_processing,
        const ContextPtr & local_context,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns,
        ObjectInfos * read_keys,
        std::function<void(FileProgress)> file_progress_callback = {});

    static std::string getUniqueStoragePathIdentifier(
        const Configuration & configuration,
        const ObjectInfo & object_info,
        bool include_connection_info = true);

protected:
    const String name;
    ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const UInt64 max_block_size;
    const bool need_only_count;
    const size_t max_parsing_threads;
    ReadFromFormatInfo read_from_format_info;
    const std::shared_ptr<ThreadPool> create_reader_pool;

    std::shared_ptr<IIterator> file_iterator;
    SchemaCache & schema_cache;
    bool initialized = false;
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
        const std::shared_ptr<IIterator> & file_iterator,
        const ConfigurationPtr & configuration,
        const ObjectStoragePtr & object_storage,
        ReadFromFormatInfo & read_from_format_info,
        const std::optional<FormatSettings> & format_settings,
        const std::shared_ptr<const KeyCondition> & key_condition_,
        const ContextPtr & context_,
        SchemaCache * schema_cache,
        const LoggerPtr & log,
        size_t max_block_size,
        size_t max_parsing_threads,
        bool need_only_count);

    ReaderHolder createReader();

    std::future<ReaderHolder> createReaderAsync();
    static std::unique_ptr<ReadBuffer> createReadBuffer(
        const ObjectInfo & object_info,
        const ObjectStoragePtr & object_storage,
        const ContextPtr & context_,
        const LoggerPtr & log);

    void addNumRowsToCache(const ObjectInfo & object_info, size_t num_rows);
    void lazyInitialize();
};

class StorageObjectStorageSource::IIterator
{
public:
    explicit IIterator(const std::string & logger_name_);

    virtual ~IIterator() = default;

    virtual size_t estimatedKeysCount() = 0;

    ObjectInfoPtr next(size_t processor);

protected:
    virtual ObjectInfoPtr nextImpl(size_t processor) = 0;
    LoggerPtr logger;
};

class StorageObjectStorageSource::ReadTaskIterator : public IIterator
{
public:
    ReadTaskIterator(const ReadTaskCallback & callback_, size_t max_threads_count);

    size_t estimatedKeysCount() override { return buffer.size(); }

private:
    ObjectInfoPtr nextImpl(size_t) override;

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
        bool throw_on_zero_files_match_,
        std::function<void(FileProgress)> file_progress_callback_ = {});

    ~GlobIterator() override = default;

    size_t estimatedKeysCount() override;

private:
    ObjectInfoPtr nextImpl(size_t processor) override;
    ObjectInfoPtr nextImplUnlocked(size_t processor);
    void createFilterAST(const String & any_key);
    void fillBufferForKey(const std::string & uri_key);

    const ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    const NamesAndTypesList virtual_columns;
    const bool throw_on_zero_files_match;

    size_t index = 0;

    ObjectInfos object_infos;
    ObjectInfos * read_keys;
    ExpressionActionsPtr filter_expr;
    ObjectStorageIteratorPtr object_storage_iterator;
    bool recursive{false};
    std::vector<String> expanded_keys;
    std::vector<String>::iterator expanded_keys_iter;

    std::unique_ptr<re2::RE2> matcher;

    bool is_finished = false;
    bool first_iteration = true;
    std::mutex next_mutex;
    const ContextPtr local_context;

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
        bool ignore_non_existent_files_,
        std::function<void(FileProgress)> file_progress_callback = {});

    ~KeysIterator() override = default;

    size_t estimatedKeysCount() override { return keys.size(); }

private:
    ObjectInfoPtr nextImpl(size_t processor) override;

    const ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    const NamesAndTypesList virtual_columns;
    const std::function<void(FileProgress)> file_progress_callback;
    const std::vector<String> keys;
    std::atomic<size_t> index = 0;
    bool ignore_non_existent_files;
};

/*
 * An archives iterator.
 * Allows to iterate files inside one or many archives.
 * `archives_iterator` is an iterator which iterates over different archives.
 * There are two ways to read files in archives:
 * 1. When we want to read one concete file in each archive.
 *    In this case we go through all archives, check if this certain file
 *    exists within this archive and read it if it exists.
 * 2. When we have a certain pattern of files we want to read in each archive.
 *    For this purpose we create a filter defined as IArchiveReader::NameFilter.
 */
class StorageObjectStorageSource::ArchiveIterator : public IIterator, private WithContext
{
public:
    explicit ArchiveIterator(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        std::unique_ptr<IIterator> archives_iterator_,
        ContextPtr context_,
        ObjectInfos * read_keys_);

    size_t estimatedKeysCount() override;

    struct ObjectInfoInArchive : public ObjectInfo
    {
        ObjectInfoInArchive(
            ObjectInfoPtr archive_object_,
            const std::string & path_in_archive_,
            std::shared_ptr<IArchiveReader> archive_reader_,
            IArchiveReader::FileInfo && file_info_);

        std::string getFileName() const override
        {
            return path_in_archive;
        }

        std::string getPath() const override
        {
            return archive_object->getPath() + "::" + path_in_archive;
        }

        std::string getPathToArchive() const override
        {
            return archive_object->getPath();
        }

        bool isArchive() const override { return true; }

        size_t fileSizeInArchive() const override { return file_info.uncompressed_size; }

        const ObjectInfoPtr archive_object;
        const std::string path_in_archive;
        const std::shared_ptr<IArchiveReader> archive_reader;
        const IArchiveReader::FileInfo file_info;
    };

private:
    ObjectInfoPtr nextImpl(size_t processor) override;
    std::shared_ptr<IArchiveReader> createArchiveReader(ObjectInfoPtr object_info) const;

    const ObjectStoragePtr object_storage;
    const bool is_path_in_archive_with_globs;
    /// Iterator which iterates through different archives.
    const std::unique_ptr<IIterator> archives_iterator;
    /// Used when files inside archive are defined with a glob
    const IArchiveReader::NameFilter filter = {};
    /// Current file inside the archive.
    std::string path_in_archive = {};
    /// Read keys of files inside archives.
    ObjectInfos * read_keys;
    /// Object pointing to archive (NOT path within archive).
    ObjectInfoPtr archive_object;
    /// Reader of the archive.
    std::shared_ptr<IArchiveReader> archive_reader;
    /// File enumerator inside the archive.
    std::unique_ptr<IArchiveReader::FileEnumerator> file_enumerator;

    std::mutex next_mutex;
};

}
