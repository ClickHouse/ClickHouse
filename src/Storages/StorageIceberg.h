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
    IcebergMetaParser(const StorageS3::S3Configuration & configuration_, const String & table_path_, ContextPtr context_);

    std::vector<String> getFiles() const;

private:
    static constexpr auto metadata_directory = "metadata";
    StorageS3::S3Configuration base_configuration;
    String table_path;
    ContextPtr context;

    /// Just get file name
    String getNewestMetaFile() const;
    String getManiFestList(const String & metadata_name) const;
    std::vector<String> getManifestFiles(const String & manifest_list) const;
    std::vector<String> getFilesForRead(const std::vector<String> & manifest_files) const;

    std::shared_ptr<ReadBuffer> createS3ReadBuffer(const String & key) const;
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
