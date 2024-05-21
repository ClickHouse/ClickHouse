#pragma once

#include <config.h>

#if USE_AWS_S3

#include <Storages/StorageS3.h>

namespace DB
{

class ReadBuffer;

struct S3DataLakeMetadataReadHelper
{
    static std::shared_ptr<ReadBuffer> createReadBuffer(
        const String & key, ContextPtr context, const StorageS3::Configuration & base_configuration);

    static bool exists(const String & key, const StorageS3::Configuration & configuration);

    static std::vector<String> listFiles(const StorageS3::Configuration & configuration, const std::string & prefix = "", const std::string & suffix = "");
};
}

#endif
