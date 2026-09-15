#pragma once

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/Utils.h>


namespace DB::Iceberg
{
#if USE_AVRO

void compactIcebergTable(
    IcebergHistory snapshots_info,
    const PersistentTableComponents & persistent_table_components,
    DB::ObjectStoragePtr object_storage_,
    std::shared_ptr<SecondaryStorages> secondary_storages_,
    const DataLakeStorageSettings & data_lake_settings,
    const std::optional<DB::FormatSettings> & format_settings_,
    DB::SharedHeader sample_block_,
    DB::ContextPtr context_,
    const String & write_format);

void compactIcebergManifests(
    const PersistentTableComponents & persistent_table_components,
    DB::ObjectStoragePtr object_storage_,
    SecondaryStorages & secondary_storages,
    const DataLakeStorageSettings & data_lake_settings,
    DB::SharedHeader sample_block_,
    DB::ContextPtr context_,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id);

#endif
}
