#pragma once
#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <Common/Exception.h>


#include <Core/NamesAndTypes.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

Iceberg::ManifestFileCacheableInfo getManifestFile(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const IcebergPathFromMetadata & filename,
    size_t bytes_size);

/// Creates a fully initialized ManifestFileIterator from a cache key.
/// All entries are drained so that aggregate methods (e.g. getRowsCountInAllFilesExcludingDeleted)
/// can be called on the returned iterator.
Iceberg::ManifestFileIterator::ManifestFileEntriesHandle getManifestFileEntriesHandle(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const ManifestFileCacheKey & cache_key,
    Int32 table_snapshot_schema_id);


ManifestFileCacheKeys getManifestList(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    const IcebergPathFromMetadata & filename,
    LoggerPtr log);

}

#endif
