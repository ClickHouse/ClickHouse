#include "DiskType.h"

namespace DB
{

String toString(DataSourceType data_source_type)
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
    std::unreachable;
}

bool DataSourceDescription::operator==(const DataSourceDescription & other) const
{
    return std::tie(type, description, is_encrypted) == std::tie(other.type, other.description, other.is_encrypted);
}

bool DataSourceDescription::sameKind(const DataSourceDescription & other) const
{
    return std::tie(type, description) == std::tie(other.type, other.description);
}

}
