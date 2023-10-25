#pragma once

#include <Storages/Cache/SchemaCache.h>
#include <Storages/IStorage.h>
#include <Common/FileRenamer.h>
#include <IO/Archives/IArchiveReader.h>

#include <atomic>
#include <shared_mutex>

namespace DB
{

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

    /// From table in database
    StorageFile(const std::string & relative_table_dir_path, CommonArguments args);

    explicit StorageFile(CommonArguments args);

    std::string getName() const override { return "File"; }

    Pipe read(
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

private:
    void setStorageMetadata(CommonArguments args);

    static std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const Strings & paths, const String & format_name, const std::optional<FormatSettings> & format_settings, ContextPtr context);

    static void addColumnsToCache(
        const Strings & paths,
        const ColumnsDescription & columns,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

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
};

}
