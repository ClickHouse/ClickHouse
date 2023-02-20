#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Storages/IStorage.h>
#    include <Storages/IStorageDataLake.h>
#    include <Storages/S3DataLakeMetadataReadHelper.h>
#    include <Storages/StorageS3.h>

namespace DB
{


template <typename Configuration, typename MetadataReadHelper>
class HudiMetadataParser
{
public:
    HudiMetadataParser(const Configuration & configuration_, ContextPtr context_);

    std::vector<String> getFiles() const;

    static String generateQueryFromKeys(const std::vector<String> & keys, const String & format);

private:
    Configuration configuration;
    ContextPtr context;
    Poco::Logger * log;
};

struct StorageHudiName
{
    static constexpr auto name = "Hudi";
    static constexpr auto data_directory_prefix = "";
};

using StorageHudi
    = IStorageDataLake<StorageS3, StorageHudiName, HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
}

#endif
