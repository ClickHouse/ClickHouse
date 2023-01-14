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

// Class to parse iceberg metadata and find files needed for query in table
// Iceberg table directory outlook:
// table/
//      data/
//      metadata/
// The metadata has three layers: metadata -> manifest list -> manifest files
class IcebergMetaParser
{
public:
    IcebergMetaParser(const StorageS3Configuration & configuration_, const String & table_path_, ContextPtr context_);

    void parseMeta();

    String getNewestMetaFile();
    String getManiFestList(String metadata);
    std::vector<String> getManifestFiles(String manifest_list);
    void getFilesForRead(const std::vector<String> manifest_files);

    auto getFiles() const {return keys};

private:
    std::vector<String> keys;

    StorageS3Configuration base_configuration;
    String table_path;
    ContextPtr context;
};

// class to get deltalake log json files and read json from them
class JsonMetadataGetter
{
public:
    JsonMetadataGetter(StorageS3::S3Configuration & configuration_, const String & table_path_, ContextPtr context);

    std::vector<String> getFiles() { return std::move(metadata).listCurrentFiles(); }

private:
    void init(ContextPtr context);

    std::vector<String> getJsonLogFiles();

    std::shared_ptr<ReadBuffer> createS3ReadBuffer(const String & key, ContextPtr context);

    void handleJSON(const JSON & json);

    StorageS3::S3Configuration base_configuration;
    String table_path;
    DeltaLakeMetadata metadata;
};

class StorageIceberg : public IStorage
{
public:
    // 1. Parses internal file structure of table
    // 2. Finds out parts with latest version
    // 3. Creates url for underlying StorageS3 enigne to handle reads
    StorageIceberg(
        const StorageS3Configuration & configuration_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_);

    String getName() const override { return "Iceberg"; }

    // Reads latest version of Iceberg table
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static ColumnsDescription getTableStructureFromData(
        const StorageS3Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);
private:

    StorageS3::S3Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
    String table_path;
};

}

#endif
