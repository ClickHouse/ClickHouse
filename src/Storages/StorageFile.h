#pragma once

#include <Storages/Cache/SchemaCache.h>
#include <Storages/IStorage.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/FileRenamer.h>
#include <IO/Archives/IArchiveReader.h>
#include <Processors/SourceWithKeyCondition.h>

#include <atomic>
#include <shared_mutex>
#include <sys/stat.h>

namespace DB
{

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class IInputFormat;
using InputFormatPtr = std::shared_ptr<IInputFormat>;

class PullingPipelineExecutor;

class StorageFile final : public IStorage
{
public:
    struct CommonArguments : public WithContext
    {
        StorageID table_id;
        std::string format_name;
        std::optional<FormatSettings> format_settings;
        std::string compression_method;
        const ColumnsDescription & columns;
        const ConstraintsDescription & constraints;
        const String & comment;
        const std::string rename_after_processing;
        std::string path_to_archive;
    };

    /// From file descriptor
    StorageFile(int table_fd_, CommonArguments args);

    /// From user's file
    StorageFile(const std::string & table_path_, const std::string & user_files_path, CommonArguments args);

    StorageFile(const std::string & table_path_, const std::string & user_files_path, bool distributed_processing_, CommonArguments args);

    /// From table in database
    StorageFile(const std::string & relative_table_dir_path, CommonArguments args);

    explicit StorageFile(CommonArguments args);

    std::string getName() const override { return "File"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context,
        bool async_insert) override;

    void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* context */,
        TableExclusiveLockHolder &) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    bool storesDataOnDisk() const override;
    Strings getDataPaths() const override;

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

    static Names getVirtualColumnNames();

    static Strings getPathsList(const String & table_path, const String & user_files_path, ContextPtr context, size_t & total_bytes_to_read);

    /// Check if the format supports reading only some subset of columns.
    /// Is is useful because such formats could effectively skip unknown columns
    /// So we can create a header of only required columns in read method and ask
    /// format to read only them. Note: this hack cannot be done with ordinary formats like TSV.
    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool supportsSubcolumns() const override { return true; }

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    bool supportsPartitionBy() const override { return true; }

    struct ArchiveInfo
    {
        std::vector<std::string> paths_to_archives;
        std::string path_in_archive; // used when reading a single file from archive
        IArchiveReader::NameFilter filter = {}; // used when files inside archive are defined with a glob

        bool isSingleFileRead() const
        {
            return !filter;
        }
    };

    ColumnsDescription getTableStructureFromFileDescriptor(ContextPtr context);

    static ColumnsDescription getTableStructureFromFile(
        const String & format,
        const std::vector<String> & paths,
        const String & compression_method,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        const std::optional<ArchiveInfo> & archive_info = std::nullopt);

    static SchemaCache & getSchemaCache(const ContextPtr & context);

    static void parseFileSource(String source, String & filename, String & path_to_archive);

    static ArchiveInfo getArchiveInfo(
        const std::string & path_to_archive,
        const std::string & file_in_archive,
        const std::string & user_files_path,
        ContextPtr context,
        size_t & total_bytes_to_read);

    bool supportsTrivialCountOptimization() const override { return true; }

protected:
    friend class StorageFileSource;
    friend class StorageFileSink;
    friend class ReadFromFile;

private:
    void setStorageMetadata(CommonArguments args);

    std::string format_name;
    // We use format settings from global context + CREATE query for File table
    // function -- in this case, format_settings is set.
    // For `file` table function, we use format settings from current user context,
    // in this case, format_settings is not set.
    std::optional<FormatSettings> format_settings;

    int table_fd = -1;
    String compression_method;

    std::string base_path;
    std::vector<std::string> paths;

    std::optional<ArchiveInfo> archive_info;

    bool is_db_table = true;        /// Table is stored in real database, not user's file
    bool use_table_fd = false;      /// Use table_fd instead of path

    mutable std::shared_timed_mutex rwlock;

    Poco::Logger * log = &Poco::Logger::get("StorageFile");

    /// Total number of bytes to read (sums for multiple files in case of globs). Needed for progress bar.
    size_t total_bytes_to_read = 0;

    String path_for_partitioned_write;

    bool is_path_with_globs = false;

    /// These buffers are needed for schema inference when data source
    /// is file descriptor. See getTableStructureFromFileDescriptor.
    std::unique_ptr<ReadBuffer> read_buffer_from_fd;
    std::unique_ptr<ReadBuffer> peekable_read_buffer_from_fd;
    std::atomic<bool> has_peekable_read_buffer_from_fd = false;

    // Counts the number of readers
    std::atomic<int32_t> readers_counter = 0;
    FileRenamer file_renamer;
    bool was_renamed = false;

    NamesAndTypesList virtual_columns;
    bool distributed_processing = false;
};

class StorageFileSource : public SourceWithKeyCondition
{
public:
    class FilesIterator
    {
    public:
        explicit FilesIterator(
            const Strings & files_,
            std::optional<StorageFile::ArchiveInfo> archive_info_,
            const ActionsDAG::Node * predicate,
            const NamesAndTypesList & virtual_columns,
            ContextPtr context_,
            bool distributed_processing_ = false);

        String next();

        bool isReadFromArchive() const
        {
            return archive_info.has_value();
        }

        bool validFileInArchive(const std::string & path) const
        {
            return archive_info->filter(path);
        }

        bool isSingleFileReadFromArchive() const
        {
            return archive_info->isSingleFileRead();
        }

        const String & getFileNameInArchive();
private:
        std::vector<std::string> files;

        std::optional<StorageFile::ArchiveInfo> archive_info;

        std::atomic<size_t> index = 0;

        bool distributed_processing;

        ContextPtr context;
    };

    using FilesIteratorPtr = std::shared_ptr<FilesIterator>;

    StorageFileSource(
        const ReadFromFormatInfo & info,
        std::shared_ptr<StorageFile> storage_,
        ContextPtr context_,
        UInt64 max_block_size_,
        FilesIteratorPtr files_iterator_,
        std::unique_ptr<ReadBuffer> read_buf_,
        bool need_only_count_);


    /**
      * If specified option --rename_files_after_processing and files created by TableFunctionFile
      * Last reader will rename files according to specified pattern if desctuctor of reader was called without uncaught exceptions
      */
    void beforeDestroy();

    ~StorageFileSource() override;

    String getName() const override
    {
        return storage->getName();
    }

    void setKeyCondition(const ActionsDAG::NodeRawConstPtrs & nodes, ContextPtr context_) override;

    bool tryGetCountFromCache(const struct stat & file_stat);

    Chunk generate() override;

    void addNumRowsToCache(const String & path, size_t num_rows) const;

    std::optional<size_t> tryGetNumRowsFromCache(const String & path, time_t last_mod_time) const;

    std::shared_ptr<StorageFile> storage;
    FilesIteratorPtr files_iterator;
    String current_path;
    std::optional<size_t> current_file_size;
    struct stat current_archive_stat;
    std::optional<String> filename_override;
    Block sample_block;
    std::unique_ptr<ReadBuffer> read_buf;
    InputFormatPtr input_format;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;

    std::shared_ptr<IArchiveReader> archive_reader;
    std::unique_ptr<IArchiveReader::FileEnumerator> file_enumerator = nullptr;

    ColumnsDescription columns_description;
    NamesAndTypesList requested_columns;
    NamesAndTypesList requested_virtual_columns;
    Block block_for_format;

    ContextPtr context;    /// TODO Untangle potential issues with context lifetime.
    UInt64 max_block_size;

    bool finished_generate = false;
    bool need_only_count = false;
    size_t total_rows_in_file = 0;

    std::shared_lock<std::shared_timed_mutex> shared_lock;
};

}
