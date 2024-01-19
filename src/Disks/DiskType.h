#pragma once

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

enum class DataSourceType
{
    Local,
    RAM,
    S3,
    S3_Plain,
    HDFS,
    WebServer,
    AzureBlobStorage,
    LocalBlobStorage,
};

String toString(DataSourceType data_source_type);

struct DataSourceDescription
{
    DataSourceType type;
    std::string description;

    bool is_encrypted = false;
    bool is_cached = false;

    bool operator==(const DataSourceDescription & other) const;
    bool sameKind(const DataSourceDescription & other) const;
};

}
