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

/// Register a Delta table (just created or attached by `DeltaLakeMetadata::createInitial`) with a data lake catalog (Unity).
void registerDeltaTableInCatalog(
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration_ptr,
    bool if_not_exists,
    const StorageID & table_id);

}

#endif
