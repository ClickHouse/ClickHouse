#pragma once
#include <Core/Types.h>

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

}
