#pragma once

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace Iceberg
{

void compactIcebergTable(
    DB::ObjectStoragePtr object_storage_,
    DB::StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<DB::FormatSettings> & format_settings_,
    DB::SharedHeader sample_block_,
    DB::ContextPtr context_
);

}
