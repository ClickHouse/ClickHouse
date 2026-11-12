#pragma once

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>


namespace DB::Iceberg
{
#if USE_AVRO && !CLICKHOUSE_CLOUD

/// Commits one `replace` snapshot on top of the current one, in which every data file with applicable position deletes is
/// rewritten without the deleted rows and the position deletes are dropped. Earlier snapshots and their files are kept.
void compactIcebergTable(
    const PersistentTableComponents & persistent_table_components,
    DB::ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const std::optional<DB::FormatSettings> & format_settings,
    DB::ContextPtr context,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id);

void compactIcebergManifests(
    const PersistentTableComponents & persistent_table_components,
    DB::ObjectStoragePtr object_storage_,
    const DataLakeStorageSettings & data_lake_settings,
    DB::SharedHeader sample_block_,
    DB::ContextPtr context_,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id);

#endif
}
