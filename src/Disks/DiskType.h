#pragma once

#include <base/types.h>

namespace DB
{

enum class DataSourceType
{
    Local,
    RAM,
    S3,
    HDFS,
    WebServer,
    AzureBlobStorage,
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
        case DataSourceType::HDFS:
            return "hdfs";
        case DataSourceType::WebServer:
            return "web";
        case DataSourceType::AzureBlobStorage:
            return "azure_blob_storage";
    }
    __builtin_unreachable();
}

struct DataSourceDescription
{
    DataSourceType type;
    std::string description;
    bool is_encrypted;
    bool is_cached;

    bool operator==(const DataSourceDescription & o) const;
};

}
