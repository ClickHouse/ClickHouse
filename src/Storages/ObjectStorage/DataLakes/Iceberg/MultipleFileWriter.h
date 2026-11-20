#pragma once

#include <Formats/FormatFilterInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

#include <deque>

namespace DB
{

#if USE_AVRO

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

/// Buffers of the finished data files of one INSERT stream after `preFinalize`, oldest first.
class DataFileUploads
{
public:
    explicit DataFileUploads(size_t max_files_in_upload_);

    void add(std::unique_ptr<WriteBufferFromFileBase> && buffer);
    void finalize();
    /// For S3, waits for the uploads in flight, so that their objects can be removed afterwards.
    void cancel() noexcept;

private:
    void finalizeUntilAtMost(size_t max_files);

    const size_t max_files_in_upload;
    std::deque<std::unique_ptr<WriteBufferFromFileBase>> buffers;
};

class MultipleFileWriter
{
public:
    explicit MultipleFileWriter(
        UInt64 max_data_file_num_rows_,
        UInt64 max_data_file_num_bytes_,
        Poco::JSON::Array::Ptr schema,
        FileNamesGenerator & filename_generator_,
        DataFileUploads & data_file_uploads_,
        std::shared_ptr<const Iceberg::IcebergPathResolver> path_resolver_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const std::optional<FormatSettings> & format_settings_,
        const String & write_format_,
        SharedHeader sample_block_);

    void consume(const Chunk & chunk);
    void startNewFile();
    /// Does not wait for the upload of the file; `data_file_uploads` does.
    void finalize();
    void release();
    void cancel();
    /// The files of a cancelled writer must not be committed.
    [[nodiscard]] bool isCancelled() const { return cancelled; }
    void clearAllDataFiles() const;

    [[nodiscard]] UInt64 getResultBytes() const;

    [[nodiscard]] const std::vector<Iceberg::IcebergPathFromMetadata> & getDataFiles() const
    {
        return data_file_names;
    }

    [[nodiscard]] const std::vector<UInt64> & getDataFileRowCounts() const
    {
        return data_file_row_counts;
    }

    [[nodiscard]] const std::vector<UInt64> & getDataFileByteCounts() const
    {
        return data_file_byte_counts;
    }

    [[nodiscard]] const DataFileStatistics & getResultStatistics() const
    {
        return stats;
    }

    [[nodiscard]] const std::vector<DataFileStatisticsPtr> & getPerFileStatistics() const
    {
        return completed_file_stats;
    }

private:
    UInt64 max_data_file_num_rows;
    UInt64 max_data_file_num_bytes;
    /// Pre-built ColumnMapper for `startNewFile`. Traversing the Iceberg schema is invariant
    /// for the lifetime of the writer, so we compute the mapping once and reuse it across
    /// every rolled-over data file instead of recomputing it on each rollover.
    ColumnMapperPtr column_mapper;
    std::unordered_set<Int64> required_field_ids;
    DataFileStatistics stats;
    DataFileStatisticsPtr current_file_stats;
    std::vector<DataFileStatisticsPtr> completed_file_stats;
    std::optional<size_t> current_file_num_rows = std::nullopt;
    std::vector<Iceberg::IcebergPathFromMetadata> data_file_names;
    std::vector<UInt64> data_file_row_counts;
    std::vector<UInt64> data_file_byte_counts;
    std::unique_ptr<WriteBufferFromFileBase> buffer;
    OutputFormatPtr output_format;
    FileNamesGenerator & filename_generator;
    /// Owned by the sink and used only by `finalize`, which only the sink calls.
    DataFileUploads & data_file_uploads;
    /// Shared: `clearAllDataFiles` may run after the sink is destroyed, see `IcebergWriteSession`.
    std::shared_ptr<const Iceberg::IcebergPathResolver> path_resolver;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    const String& write_format;
    SharedHeader sample_block;
    UInt64 total_bytes = 0;
    bool cancelled = false;
};

#endif

}
