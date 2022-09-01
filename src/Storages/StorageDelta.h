#pragma once

#include "config_core.h"

#include <Storages/IStorage.h>
#include <Storages/StorageS3.h>

#include <unordered_map>
#include <base/JSON.h>

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
class DeltaLakeMetadata {
public:
    DeltaLakeMetadata() = default;

public:
    void add(const String & filename, uint64_t timestamp);
    void remove(const String & filename, uint64_t timestamp);

public:
    std::vector<String> ListCurrentFiles() &&;

private:
    std::unordered_map<String, uint64_t> file_update_time;
};

// class to get deltalake log json files and read json from them
class JsonMetadataGetter 
{
public:
    JsonMetadataGetter(StorageS3::S3Configuration & configuration_,
                        const String & table_path_,
                        Poco::Logger * log_
                        );

private:
    void Init();

    std::vector<String> getJsonLogFiles();

private:
    std::unique_ptr<ReadBuffer> createS3ReadBuffer(const String & key);

public:
    std::vector<String> getFiles() { return std::move(metadata).ListCurrentFiles(); }

private:
    StorageS3::S3Configuration base_configuration;
    String table_path;
    DeltaLakeMetadata metadata;
    Poco::Logger * log;
};

class StorageDelta : public IStorage
{
public:
    StorageDelta(
        const S3::URI & uri_,
        const String & access_key_,
        const String & secret_access_key_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_);

    String getName() const override { return "DeltaLake"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    void Init();
    static void updateS3Configuration(ContextPtr, StorageS3::S3Configuration &);
    

private:
    String generateQueryFromKeys(std::vector<String> && keys);

private:
    StorageS3::S3Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
    String table_path;
};

}
