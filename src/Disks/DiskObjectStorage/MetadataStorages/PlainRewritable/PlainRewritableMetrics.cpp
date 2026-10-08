#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Common/Exception.h>

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableAzureDirectoryCreated;
    extern const Event DiskPlainRewritableAzureDirectoryRemoved;
    extern const Event DiskPlainRewritableLocalDirectoryCreated;
    extern const Event DiskPlainRewritableLocalDirectoryRemoved;
    extern const Event DiskPlainRewritableS3DirectoryCreated;
    extern const Event DiskPlainRewritableS3DirectoryRemoved;
}

namespace CurrentMetrics
{
    extern const Metric DiskPlainRewritableAzureDirectoryMapSize;
    extern const Metric DiskPlainRewritableAzureFileCount;
    extern const Metric DiskPlainRewritableLocalDirectoryMapSize;
    extern const Metric DiskPlainRewritableLocalFileCount;
    extern const Metric DiskPlainRewritableS3DirectoryMapSize;
    extern const Metric DiskPlainRewritableS3FileCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

std::shared_ptr<PlainRewritableMetrics> createPlainRewritableMetrics(ObjectStorageType object_storage_type)
{
    switch (object_storage_type)
    {
        case ObjectStorageType::S3:
        {
            return std::make_shared<PlainRewritableMetrics>(PlainRewritableMetrics{
                .directory_created = ProfileEvents::DiskPlainRewritableS3DirectoryCreated,
                .directory_removed = ProfileEvents::DiskPlainRewritableS3DirectoryRemoved,
                .directory_map_size = CurrentMetrics::DiskPlainRewritableS3DirectoryMapSize,
                .file_count = CurrentMetrics::DiskPlainRewritableS3FileCount,
            });
        }
        case ObjectStorageType::Azure:
        {
            return std::make_shared<PlainRewritableMetrics>(PlainRewritableMetrics{
                .directory_created = ProfileEvents::DiskPlainRewritableAzureDirectoryCreated,
                .directory_removed = ProfileEvents::DiskPlainRewritableAzureDirectoryRemoved,
                .directory_map_size = CurrentMetrics::DiskPlainRewritableAzureDirectoryMapSize,
                .file_count = CurrentMetrics::DiskPlainRewritableAzureFileCount,
            });
        }
        case ObjectStorageType::Local:
        {
            return std::make_shared<PlainRewritableMetrics>(PlainRewritableMetrics{
                .directory_created = ProfileEvents::DiskPlainRewritableLocalDirectoryCreated,
                .directory_removed = ProfileEvents::DiskPlainRewritableLocalDirectoryRemoved,
                .directory_map_size = CurrentMetrics::DiskPlainRewritableLocalDirectoryMapSize,
                .file_count = CurrentMetrics::DiskPlainRewritableLocalFileCount,
            });
        }
        case ObjectStorageType::None:
        case ObjectStorageType::HDFS:
        case ObjectStorageType::Web:
        case ObjectStorageType::Max:
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
        }
    }
}

}
