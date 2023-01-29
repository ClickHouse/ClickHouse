#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Storages/IStorage.h>
#    include <Storages/IStorageDataLake.h>
#    include <Storages/StorageS3.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{

class HudiMetaParser
{
public:
    HudiMetaParser(const StorageS3::S3Configuration & configuration_, const String & table_path_, ContextPtr context_);

    std::vector<String> getFiles() const;

    static String generateQueryFromKeys(const std::vector<String> & keys, const String & format);

private:
    StorageS3::S3Configuration configuration;
    String table_path;
    ContextPtr context;
    Poco::Logger * log;
};

struct StorageHudiName
{
    static constexpr auto name = "Hudi";
};

using StorageHudi = IStorageDataLake<StorageHudiName, HudiMetaParser>;
}

#endif
