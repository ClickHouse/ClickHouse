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
};

MetadataStorageType metadataTypeFromString(const String & type);
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

    std::string toString() const;
};

}
