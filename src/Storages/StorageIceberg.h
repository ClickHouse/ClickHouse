#pragma once

#include "config.h"

// StorageIceberg depending on Avro to parse metadata with Avro format.
#if USE_AWS_S3 && USE_AVRO

#    include <Storages/IStorageDataLake.h>
#    include <Storages/S3DataLakeMetadataReadHelper.h>
#    include <Storages/StorageS3.h>

namespace DB
{

// Class to parse iceberg metadata and find files needed for query in table
// Iceberg table directory outlook:
// table/
//      data/
//      metadata/
// The metadata has three layers: metadata -> manifest list -> manifest files
template <typename Configuration, typename MetadataReadHelper>
class IcebergMetadataParser
{
public:
    IcebergMetadataParser(const Configuration & configuration_, ContextPtr context_);

    std::vector<String> getFiles() const;

    static String generateQueryFromKeys(const std::vector<String> & keys, const String & format);

private:
    static constexpr auto metadata_directory = "metadata";
    Configuration base_configuration;
    ContextPtr context;

    String fetchMetadataFile() const;
    String getManifestList(const String & metadata_name) const;
    std::vector<String> getManifestFiles(const String & manifest_list) const;
    std::vector<String> getFilesForRead(const std::vector<String> & manifest_files) const;
};

struct StorageIcebergName
{
    static constexpr auto name = "Iceberg";
    static constexpr auto data_directory_prefix = "data";
};

using StorageIceberg
    = IStorageDataLake<StorageS3, StorageIcebergName, IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
}

#endif
