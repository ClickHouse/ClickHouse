#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Storages/IStorageDataLake.h>
#    include <Storages/S3DataLakeMetadataReadHelper.h>
#    include <Storages/StorageS3.h>

#    include <base/JSON.h>

namespace DB
{

// class to parse json deltalake metadata and find files needed for query in table
class DeltaLakeMetadata
{
public:
    DeltaLakeMetadata() = default;

    void setLastModifiedTime(const String & filename, uint64_t timestamp);
    void remove(const String & filename, uint64_t timestamp);

    std::vector<String> listCurrentFiles() &&;

private:
    std::unordered_map<String, uint64_t> file_update_time;
};

// class to get deltalake log json files and read json from them
template <typename Configuration, typename MetadataReadHelper>
class DeltaLakeMetadataParser
{
public:
    DeltaLakeMetadataParser(const Configuration & configuration_, ContextPtr context);

    std::vector<String> getFiles() { return std::move(metadata).listCurrentFiles(); }

    static String generateQueryFromKeys(const std::vector<String> & keys, const String & format);

private:
    void init(ContextPtr context);

    std::vector<String> getJsonLogFiles() const;

    void handleJSON(const JSON & json);

    Configuration base_configuration;
    DeltaLakeMetadata metadata;
};

struct StorageDeltaLakeName
{
    static constexpr auto name = "DeltaLake";
    static constexpr auto data_directory_prefix = "";
};

using StorageDeltaLake
    = IStorageDataLake<StorageS3, StorageDeltaLakeName, DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
}

#endif
