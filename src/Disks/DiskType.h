#pragma once

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

enum class DataSourceType
{
    Local,
    RAM,
    ObjectStorage,
};

enum class ObjectStorageType
{
    None,
    S3,
    S3_Plain,
    Azure,
    HDFS,
    Web,
    Local,
};

enum class MetadataStorageType
{
    None,
    Local,
    Plain,
    StaticWeb,
    Memory,
};

String toString(DataSourceType data_source_type);

struct DataSourceDescription
{
    DataSourceType type;
    ObjectStorageType object_storage_type = ObjectStorageType::None;
    MetadataStorageType metadata_type = MetadataStorageType::None;

    std::string description;

    bool is_encrypted = false;
    bool is_cached = false;

    bool operator==(const DataSourceDescription & other) const;
    bool sameKind(const DataSourceDescription & other) const;

    std::string toString() const
    {
        switch (type)
        {
            case DataSourceType::Local:
                return "local";
            case DataSourceType::RAM:
                return "memory";
            case DataSourceType::ObjectStorage:
            {
                switch (object_storage_type)
                {
                    case ObjectStorageType::S3:
                        return "s3";
                    case ObjectStorageType::S3_Plain:
                        return "s3_plain";
                    case ObjectStorageType::HDFS:
                        return "hdfs";
                    case ObjectStorageType::Azure:
                        return "azure_blob_storage";
                    case ObjectStorageType::Local:
                        return "local_blob_storage";
                    case ObjectStorageType::Web:
                        return "web";
                    case ObjectStorageType::None:
                        return "none";
                }
            }
        }
    }
};

}
