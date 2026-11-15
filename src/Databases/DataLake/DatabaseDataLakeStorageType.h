#pragma once
#include <Core/Types.h>
#include <Disks/DiskType.h>

namespace DB
{

enum class DatabaseDataLakeStorageType : uint8_t
{
    S3,
    Azure,
    Local,
    HDFS,
    /// Fake storage in case when catalog store not only
    /// primary-type tables (DeltaLake or Iceberg), but, for
    /// example, something else like INFORMATION_SCHEMA.
    /// Such tables are unreadable, but at least we can show
    /// them in SHOW CREATE TABLE, as well we can show their
    /// schema.
    Other,
};

inline DatabaseDataLakeStorageType toDataLakeStorageType(ObjectStorageType type)
{
    switch (type)
    {
        case ObjectStorageType::S3:
            return DatabaseDataLakeStorageType::S3;
        case ObjectStorageType::Azure:
            return DatabaseDataLakeStorageType::Azure;
        case ObjectStorageType::HDFS:
            return DatabaseDataLakeStorageType::HDFS;
        case ObjectStorageType::Local:
            return DatabaseDataLakeStorageType::Local;
        case ObjectStorageType::None:
        case ObjectStorageType::Web:
        case ObjectStorageType::Max:
            return DatabaseDataLakeStorageType::Other;
    }
}

}
