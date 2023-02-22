#pragma once

#include <config.h>

#if USE_AWS_S3

#    include <Storages/StorageS3.h>

class ReadBuffer;

namespace DB
{

struct S3DataLakeMetadataReadHelper
{
    static std::shared_ptr<ReadBuffer>
    createReadBuffer(const String & key, ContextPtr context, const StorageS3::Configuration & base_configuration);

    static std::vector<String>
    listFilesMatchSuffix(const StorageS3::Configuration & base_configuration, const String & directory, const String & suffix);

    static std::vector<String> listFiles(const StorageS3::Configuration & configuration);
};
}

#endif
