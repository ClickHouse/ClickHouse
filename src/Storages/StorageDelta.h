#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Storages/IStorage.h>
#    include <Storages/StorageS3.h>

#    include <unordered_map>
#    include <base/JSON.h>

namespace Poco
{
class Logger;
}

namespace Aws::S3
{
class S3Client;
}

namespace DB
{

// class to parse json deltalake metadata and find files needed for query in table
class DeltaLakeMetadata
{
public:
    DeltaLakeMetadata() = default;

    void setLastModifiedTime(const String & filename, uint64_t timestamp);
    void remove(const String & filename, uint64_t timestamp);

    std::vector<String> ListCurrentFiles() &&;

private:
    std::unordered_map<String, uint64_t> file_update_time;
};

// class to get deltalake log json files and read json from them
class JsonMetadataGetter
{
public:
    JsonMetadataGetter(StorageS3::S3Configuration & configuration_, const String & table_path_, ContextPtr context);

    std::vector<String> getFiles() { return std::move(metadata).ListCurrentFiles(); }

private:
    void Init(ContextPtr context);

    std::vector<String> getJsonLogFiles();

    std::shared_ptr<ReadBuffer> createS3ReadBuffer(const String & key, ContextPtr context);

    void handleJSON(const JSON & json);

    StorageS3::S3Configuration base_configuration;
    String table_path;
    DeltaLakeMetadata metadata;
};

class StorageDelta : public IStorage
{
public:
    // 1. Parses internal file structure of table
    // 2. Finds out parts with latest version
    // 3. Creates url for underlying StorageS3 enigne to handle reads
    StorageDelta(
        const StorageS3Configuration & configuration_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_);

    String getName() const override { return "DeltaLake"; }

    // Reads latest version of DeltaLake table
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    void Init();

    // DeltaLake stores data in parts in different files
    // keys is vector of parts with latest version
    // generateQueryFromKeys constructs query from parts filenames for
    // underlying StorageS3 engine
    static String generateQueryFromKeys(std::vector<String> && keys);

    StorageS3::S3Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
    String table_path;
};

}

#endif
