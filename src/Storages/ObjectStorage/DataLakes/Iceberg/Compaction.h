#pragma once

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>


namespace DB::Iceberg
{
#if USE_AVRO

void compactIcebergTable(
    IcebergHistory snapshots_info,
    const PersistentTableComponents & persistent_table_components,
    DB::ObjectStoragePtr object_storage_,
    DB::StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<DB::FormatSettings> & format_settings_,
    DB::SharedHeader sample_block_,
    DB::ContextPtr context_,
    DB::CompressionMethod compression_method_);

#endif
}
