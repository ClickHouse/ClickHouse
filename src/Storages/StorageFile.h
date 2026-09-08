#pragma once

#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/Archives/IArchiveReader.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/LazilyReadFromFile.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/IStorage.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/FileRenamer.h>
#include <Common/Logger.h>

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

struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;

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
    };

    struct ArchiveInfo
    {
        std::vector<std::string> paths_to_archives;
        std::string path_in_archive; // used when reading a single file from archive
        IArchiveReader::NameFilter filter; // used when files inside archive are defined with a glob

        bool isSingleFileRead() const { return !filter; }
    };

    /// One or multiple files specified by user.
    /// Multiple files can be specified by using globs (e.g. "file*.parquet").
    /// The archive syntax is also supported (e.g. "archive*.zip::*.parquet").
    struct FileSource
    {
        Strings paths;
        bool with_globs = false;
        size_t total_bytes_to_read = 0;
        String path_for_partitioned_write;
        std::optional<String> format_from_filenames; /// Set if we managed to figure out which file format is used from the names of the file(s).
        std::optional<ArchiveInfo> archive_info; /// Set if the archive syntax is used.

        static FileSource parse(const String & source, const ContextPtr & context, std::optional<bool> allow_archive_path_syntax = {});
    };

    /// From file descriptor
    StorageFile(int table_fd_, CommonArguments args);

    /// From user's file
    StorageFile(FileSource file_source_, CommonArguments args);
    StorageFile(FileSource file_source_, bool distributed_processing_, CommonArguments args);

    /// From table in database
    StorageFile(const std::string & relative_table_dir_path, CommonArguments args);

    explicit StorageFile(CommonArguments args);

    std::string getName() const override { return "File"; }

    /// The concrete data format resolved for this table (after schema/format inference).
    /// Used by the unified `URL` engine to persist the delegate's inferred format.
    const String & getFormatName() const { return format_name; }

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

    /// Check if the format supports reading only some subset of columns.
    /// Is is useful because such formats could effectively skip unknown columns
    /// So we can create a header of only required columns in read method and ask
    /// format to read only them. Note: this hack cannot be done with ordinary formats like TSV.
    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    /// Things required for PREWHERE.
    bool supportsPrewhere() const override;
    bool canMoveConditionsToPrewhere() const override;
    std::optional<NameSet> supportedPrewhereColumns() const override;
    ColumnSizeByName getColumnSizes() const override;

    bool supportsSubcolumns() const override { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }
    /// Unlike `.null`/`.size0`, a tuple element is a real leaf in the file, so the format can serve
    /// `t.x` on its own and prune on it.
    bool supportsOptimizationToTupleElementSubcolumns() const override { return true; }

    bool supportsColumnsWithDynamicStructure() const override { return true; }

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;
    size_t getMaxReadStreams(size_t num_streams, ContextPtr) override;

    bool supportsPartitionBy() const override { return true; }

    static ColumnsDescription getTableStructureFromFile(
        const String & format,
        const std::vector<String> & paths,
        const String & compression_method,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context,
        const std::optional<ArchiveInfo> & archive_info = std::nullopt);

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromFile(
        const std::vector<String> & paths,
        const String & compression_method,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context,
        const std::optional<ArchiveInfo> & archive_info = std::nullopt);

    static SchemaCache & getSchemaCache(const ContextPtr & context);

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    void addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const override;

    /// Lazy materialization (see LazilyReadFromFile): creates a source that reads only the
    /// specified rows of the specified files, in the given file order, rows within a file in
    /// ascending order. Fails close if a file does not match the captured generation token.
    static std::shared_ptr<ISource> createLazyRowsSource(
        std::shared_ptr<StorageFile> storage,
        const ReadFromFormatInfo & info,
        const ContextPtr & context,
        size_t max_block_size,
        std::vector<FileLazyMaterializingRows::FileRows> files);

protected:
    friend class StorageFileSource;
    friend class StorageFileSink;
    friend class ReadFromFile;
    friend class StorageFileLazyRowsSource;

private:
    std::pair<ColumnsDescription, String> getTableStructureAndFormatFromFileDescriptor(std::optional<String> format, const ContextPtr & context);

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromFileImpl(
        std::optional<String> format,
        const std::vector<String> & paths,
        const String & compression_method,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context,
        const std::optional<ArchiveInfo> & archive_info = std::nullopt);

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

    bool supports_prewhere = false;

    mutable std::shared_timed_mutex rwlock;

    LoggerPtr log = getLogger("StorageFile");

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
    bool distributed_processing = false;
    NamesAndTypesList file_columns;
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
};

class StorageFileSource final : public ISource, WithContext
{
public:
    class FilesIterator : WithContext
    {
    public:
        explicit FilesIterator(
            const Strings & files_,
            std::optional<StorageFile::ArchiveInfo> archive_info_,
            const ActionsDAG::Node * predicate,
            const NamesAndTypesList & virtual_columns_,
            const NamesAndTypesList & hive_columns_,
            const ContextPtr & context_,
            bool distributed_processing_ = false,
            String archive_member_path_ = {});

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

        /// A `_path` / `_file` filter that could not be applied while the iterator was created,
        /// because a set in it was not ready yet. It is applied in `next`, when the pipeline runs,
        /// before a file is opened.
        ExpressionActionsPtr deferred_filter_actions;
        NamesAndTypesList virtual_columns;
        NamesAndTypesList hive_columns;
        /// A known archive member is part of the user-visible `_path` / `_file` value, although
        /// this iterator must open the outer archive file.
        const String archive_member_path;
    };

    using FilesIteratorPtr = std::shared_ptr<FilesIterator>;

    StorageFileSource(
        const ReadFromFormatInfo & info,
        std::shared_ptr<StorageFile> storage_,
        const ContextPtr & context_,
        UInt64 max_block_size_,
        FilesIteratorPtr files_iterator_,
        std::unique_ptr<ReadBuffer> read_buf_,
        bool need_only_count_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        FormatFilterInfoPtr format_filter_info_,
        LazyFileRegistryPtr lazy_row_index_registry_ = nullptr);

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

    bool tryGetCountFromCache(const struct stat & file_stat);

    Chunk generate() override;

    void onFinish() override;

    void addNumRowsToCache(const String & path, size_t num_rows) const;

    std::optional<size_t> tryGetNumRowsFromCache(const String & path, time_t last_mod_time) const;

    std::shared_ptr<StorageFile> storage;
    FilesIteratorPtr files_iterator;
    String current_path;
    std::optional<size_t> current_file_size;
    std::optional<Poco::Timestamp> current_file_last_modified;
    /// Sub-second precision token for the cache key, derived from the same `stat`
    /// as `current_file_last_modified`. Kept separate so the user-visible
    /// `_last_modified` virtual column keeps its existing second resolution while
    /// the format metadata cache (e.g. Parquet footer cache) is invalidated even
    /// for in-place rewrites within the same wall-clock second.
    std::optional<String> current_file_cache_version;
    /// Whether `current_file_cache_version` can be trusted as a rewrite-proof version
    /// token: filesystem timestamps are coarser than the wall clock, so the token only
    /// proves a rewrite once the last modification is comfortably in the past (see the
    /// settle check in `generate`). The query condition cache skips data based on the
    /// token, so it must fail close and stay bypassed while this is false.
    bool current_file_version_settled = false;
    struct stat current_archive_stat{};
    std::optional<String> filename_override;
    Block sample_block;
    std::unique_ptr<ReadBuffer> read_buf;
    InputFormatPtr input_format;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;

    std::shared_ptr<IArchiveReader> archive_reader;
    std::unique_ptr<IArchiveReader::FileEnumerator> file_enumerator;

    ColumnsDescription columns_description;
    NamesAndTypesList requested_columns;
    NamesAndTypesList requested_virtual_columns;
    Block block_for_format;
    SerializationInfoByName serialization_hints;
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;

    UInt64 max_block_size;

    bool finished_generate = false;
    bool need_only_count = false;
    size_t total_rows_in_file = 0;

    /// Lazy materialization: when set, a `__global_row_index` column is appended to every chunk
    /// (the header must contain it), and every file is registered in the registry so that the
    /// lazy branch can find it by index. See LazilyReadFromFile.
    LazyFileRegistryPtr lazy_row_index_registry;
    /// The registry index of the file currently being read. Assigned on the first chunk.
    std::optional<UInt64> current_file_index;

    std::shared_lock<std::shared_timed_mutex> shared_lock;
};

class ReadFromFile : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromFile"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;
    bool canUpdatePrewhereInfoMultipleTimes() const override { return false; }

    ReadFromFile(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        std::shared_ptr<StorageFile> storage_,
        ReadFromFormatInfo info_,
        const bool need_only_count_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(std::make_shared<const Block>(info_.source_header), column_names_, query_info_, storage_snapshot_, context_)
        , storage(std::move(storage_))
        , info(std::move(info_))
        , need_only_count(need_only_count_)
        , max_block_size(max_block_size_)
        , max_num_streams(num_streams_)
    {
    }

    /// Lazy materialization support (see optimizeLazyMaterialization2).
    bool canUseLazyMaterialization() const;

    /// Reduces the set of columns this step reads to `required_names` (plus the columns the
    /// PREWHERE / row-level filter needs, virtual columns and hive partition columns), makes the
    /// step append a `__global_row_index` column to the output, and returns a step that lazily
    /// reads the removed columns. Returns nullptr if there is nothing to defer.
    std::unique_ptr<LazilyReadFromFile> keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names);

    LazyFileRegistryPtr getLazyRowIndexRegistry() const { return lazy_row_index_registry; }

private:
    std::shared_ptr<StorageFile> storage;
    ReadFromFormatInfo info;
    const bool need_only_count;

    size_t max_block_size;
    const size_t max_num_streams;

    std::shared_ptr<StorageFileSource::FilesIterator> files_iterator;

    /// Lazy materialization: set iff keepOnlyRequiredColumnsAndCreateLazyReadStep was called.
    LazyFileRegistryPtr lazy_row_index_registry;

    void createIterator(const ActionsDAG::Node * predicate);
};

}
