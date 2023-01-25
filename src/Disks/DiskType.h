#pragma once

#include <base/types.h>

namespace DB
{

enum class DiskType
{
    Local,
    RAM,
    S3,
    HDFS,
    Encrypted,
    WebServer,
    AzureBlobStorage,
};

inline String toString(DiskType disk_type)
{
    switch (disk_type)
    {
        case DiskType::Local:
            return "local";
        case DiskType::RAM:
            return "memory";
        case DiskType::S3:
            return "s3";
        case DiskType::HDFS:
            return "hdfs";
        case DiskType::Encrypted:
            return "encrypted";
        case DiskType::WebServer:
            return "web";
        case DiskType::AzureBlobStorage:
            return "azure_blob_storage";
    }
    __builtin_unreachable();
}

}
