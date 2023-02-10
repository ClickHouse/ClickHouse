#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Storages/IStorageDataLake.h>

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
    IcebergMetaParser(const StorageS3::Configuration & configuration_, const String & table_path_, ContextPtr context_);

    std::vector<String> getFiles() const;

    static String generateQueryFromKeys(const std::vector<String> & keys, const String & format);

private:
    static constexpr auto metadata_directory = "metadata";
    StorageS3::Configuration base_configuration;
    String table_path;
    ContextPtr context;

    /// Just get file name
    String getNewestMetaFile() const;
    String getManiFestList(const String & metadata_name) const;
    std::vector<String> getManifestFiles(const String & manifest_list) const;
    std::vector<String> getFilesForRead(const std::vector<String> & manifest_files) const;

    std::shared_ptr<ReadBuffer> createS3ReadBuffer(const String & key) const;
};

struct StorageIcebergName
{
    static constexpr auto name = "Iceberg";
    static constexpr auto data_directory_prefix = "data";
};

using StorageIceberg = IStorageDataLake<StorageIcebergName, IcebergMetaParser>;
}

#endif
