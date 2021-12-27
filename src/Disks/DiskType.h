#pragma once

#include <common/types.h>

namespace DB
{

struct DiskType
{
    enum class Type
    {
        Local,
        RAM,
        S3,
        HDFS
    };
    static String toString(Type disk_type)
    {
        switch (disk_type)
        {
            case Type::Local:
                return "local";
            case Type::RAM:
                return "memory";
            case Type::S3:
                return "s3";
            case Type::HDFS:
                return "hdfs";
        }
        __builtin_unreachable();
    }
};

}
