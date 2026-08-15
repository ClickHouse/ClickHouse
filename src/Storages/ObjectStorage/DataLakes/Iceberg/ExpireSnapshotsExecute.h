#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExpireSnapshotsTypes.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB::Iceberg
{

ExpireSnapshotsResult expireSnapshots(
    const ExpireSnapshotsOptions & options,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name,
    SecondaryStorages & secondary_storages);

Pipe executeExpireSnapshots(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name,
    SecondaryStorages & secondary_storages);

}

#endif
