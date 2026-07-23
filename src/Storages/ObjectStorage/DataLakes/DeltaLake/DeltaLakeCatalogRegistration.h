#pragma once

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Databases/DataLake/ICatalog.h>
#include <memory>

namespace DB
{

class ColumnsDescription;

/// Register a Delta table that `DeltaLakeMetadata::createInitial` just created or attached with a data
/// lake catalog (Unity): serializes the schema and location into the catalog `createTable` payload.
/// `created_fresh` tells whether commit 0 was just written (the declared `columns` are authoritative) or
/// an existing `_delta_log` was attached (the on-storage snapshot schema is used instead). If the catalog
/// rejects the registration, a freshly written commit 0 is rolled back so the failed CREATE leaves nothing behind.
void registerDeltaTableInCatalog(
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration_ptr,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context,
    const ColumnsDescription & columns,
    bool created_fresh,
    const StorageID & table_id);

}

#endif
