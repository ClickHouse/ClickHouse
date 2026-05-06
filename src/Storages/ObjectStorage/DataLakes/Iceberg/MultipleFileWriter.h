#pragma once

#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataFileEntry.h>

namespace DB
{

#if USE_AVRO

class MultipleFileWriter
{
public:
    explicit MultipleFileWriter(
        UInt64 max_data_file_num_rows_,
        UInt64 max_data_file_num_bytes_,
        Poco::JSON::Array::Ptr schema,
        FileNamesGenerator & filename_generator_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const std::optional<FormatSettings> & format_settings_,
        const String & write_format_,
        SharedHeader sample_block_,
        std::function<void(const std::string &)> new_file_path_callback_ = {});

    void consume(const Chunk & chunk);
    void startNewFile();
    void finalize();
    void release();
    void cancel();
    void clearAllDataFiles() const;

    UInt64 getResultBytes() const;

    const std::vector<String> & getDataFiles() const
    {
        return data_file_names;
    }

    const DataFileStatistics & getResultStatistics() const
    {
        return aggregate_stats;
    }

    /// Returns one entry per written data file, with the accurate row count, byte size,
    /// and per-file column statistics collected during finalization.
    /// Must be called only after finalize().
    std::vector<IcebergDataFileEntry> getDataFileEntries() const;

private:
    UInt64 max_data_file_num_rows;
    UInt64 max_data_file_num_bytes;
    DataFileStatistics aggregate_stats;   /// accumulates across all files
    DataFileStatistics current_file_stats; /// accumulates for the current file only
    std::optional<size_t> current_file_num_rows = std::nullopt;
    std::optional<size_t> current_file_num_bytes = std::nullopt;
    std::vector<String> data_file_names;
    std::vector<Int64> per_file_record_counts;
    std::vector<Int64> per_file_byte_sizes;
    std::vector<DataFileStatistics> per_file_stats_list;
    std::unique_ptr<WriteBufferFromFileBase> buffer;
    OutputFormatPtr output_format;
    FileNamesGenerator & filename_generator;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    const String& write_format;
    SharedHeader sample_block;
    UInt64 total_bytes = 0;
    Poco::JSON::Array::Ptr schema_fields_json;
    std::function<void(const std::string &)> new_file_path_callback;
};

#endif

}
