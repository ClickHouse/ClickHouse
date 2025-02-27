#pragma once
#include <Core/Types.h>

namespace DB
{

enum class DatabaseIcebergStorageType : uint8_t
{
    S3,
    Azure,
    Local,
    HDFS,
    Other,
};

}
