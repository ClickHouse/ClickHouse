#pragma once

#include "config.h"

#if USE_AWS_S3
#    include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#endif
#if USE_AZURE_BLOB_STORAGE
#    include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#endif
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageMetrics.h>

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
extern const Metric DiskPlainRewritableAzureUniqueFileNamesCount;
extern const Metric DiskPlainRewritableAzureFileCount;
extern const Metric DiskPlainRewritableLocalDirectoryMapSize;
extern const Metric DiskPlainRewritableLocalUniqueFileNamesCount;
extern const Metric DiskPlainRewritableLocalFileCount;
extern const Metric DiskPlainRewritableS3DirectoryMapSize;
extern const Metric DiskPlainRewritableS3UniqueFileNamesCount;
extern const Metric DiskPlainRewritableS3FileCount;
}

namespace DB
{

#if USE_AWS_S3
template <>
inline MetadataStorageMetrics MetadataStorageMetrics::create<S3ObjectStorage, MetadataStorageType::PlainRewritable>()
{
    return MetadataStorageMetrics{
        .directory_created = ProfileEvents::DiskPlainRewritableS3DirectoryCreated,
        .directory_removed = ProfileEvents::DiskPlainRewritableS3DirectoryRemoved,
        .directory_map_size = CurrentMetrics::DiskPlainRewritableS3DirectoryMapSize,
        .unique_filenames_count = CurrentMetrics::DiskPlainRewritableS3UniqueFileNamesCount,
        .file_count = CurrentMetrics::DiskPlainRewritableS3FileCount};
}
#endif

#if USE_AZURE_BLOB_STORAGE
template <>
inline MetadataStorageMetrics MetadataStorageMetrics::create<AzureObjectStorage, MetadataStorageType::PlainRewritable>()
{
    return MetadataStorageMetrics{
        .directory_created = ProfileEvents::DiskPlainRewritableAzureDirectoryCreated,
        .directory_removed = ProfileEvents::DiskPlainRewritableAzureDirectoryRemoved,
        .directory_map_size = CurrentMetrics::DiskPlainRewritableAzureDirectoryMapSize,
        .unique_filenames_count = CurrentMetrics::DiskPlainRewritableAzureUniqueFileNamesCount,
        .file_count = CurrentMetrics::DiskPlainRewritableAzureFileCount};
}
#endif

template <>
inline MetadataStorageMetrics MetadataStorageMetrics::create<LocalObjectStorage, MetadataStorageType::PlainRewritable>()
{
    return MetadataStorageMetrics{
        .directory_created = ProfileEvents::DiskPlainRewritableLocalDirectoryCreated,
        .directory_removed = ProfileEvents::DiskPlainRewritableLocalDirectoryRemoved,
        .directory_map_size = CurrentMetrics::DiskPlainRewritableLocalDirectoryMapSize,
        .unique_filenames_count = CurrentMetrics::DiskPlainRewritableLocalUniqueFileNamesCount,
        .file_count = CurrentMetrics::DiskPlainRewritableLocalFileCount};
}

}
