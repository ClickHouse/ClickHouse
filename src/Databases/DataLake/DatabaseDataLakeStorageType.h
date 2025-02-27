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
    Other,
};

}
