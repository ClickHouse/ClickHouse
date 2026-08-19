#pragma once

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

enum class DataSourceType : uint8_t
{
    Local,
    RAM,
    ObjectStorage,
};

enum class ObjectStorageType : uint8_t
{
    None = 0,
    S3 = 1,
    Azure = 2,
    HDFS = 3,
    Web = 4,
    Local = 5,

    Max = 6,
};

enum class MetadataStorageType : uint8_t
{
    None,
    Local,
    Keeper,
    Plain,
    PlainRewritable,
    StaticWeb,
    Memory,
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

    std::string zookeeper_name;

    bool operator==(const DataSourceDescription & other) const;
    bool sameKind(const DataSourceDescription & other) const;

    std::string toString() const;
};

bool canUseNativeCopy(const DataSourceDescription & left, const DataSourceDescription & right);

}
