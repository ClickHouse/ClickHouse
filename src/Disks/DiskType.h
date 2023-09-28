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

inline String toString(DataSourceType data_source_type)
{
    switch (data_source_type)
    {
        case DataSourceType::Local:
            return "local";
        case DataSourceType::RAM:
            return "memory";
        case DataSourceType::S3:
            return "s3";
        case DataSourceType::S3_Plain:
            return "s3_plain";
        case DataSourceType::HDFS:
            return "hdfs";
        case DataSourceType::WebServer:
            return "web";
        case DataSourceType::AzureBlobStorage:
            return "azure_blob_storage";
        case DataSourceType::LocalBlobStorage:
            return "local_blob_storage";
    }
    UNREACHABLE();
}

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
