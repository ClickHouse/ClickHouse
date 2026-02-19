#pragma once

#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

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
        StorageObjectStorageConfigurationPtr configuration_,
        SharedHeader sample_block_);

    void consume(const Chunk & chunk);
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
        return stats;
    }

private:
    UInt64 max_data_file_num_rows;
    UInt64 max_data_file_num_bytes;
    DataFileStatistics stats;
    std::optional<size_t> current_file_num_rows = std::nullopt;
    std::optional<size_t> current_file_num_bytes = std::nullopt;
    std::vector<String> data_file_names;
    std::unique_ptr<WriteBufferFromFileBase> buffer;
    OutputFormatPtr output_format;
    FileNamesGenerator & filename_generator;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    StorageObjectStorageConfigurationPtr configuration;
    SharedHeader sample_block;
    UInt64 total_bytes = 0;
};

#endif

}
