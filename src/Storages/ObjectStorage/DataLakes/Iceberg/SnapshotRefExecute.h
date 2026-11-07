#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

Pipe executeCreateSnapshotRef(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name);

Pipe executeDropSnapshotRef(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name);

}

#endif
