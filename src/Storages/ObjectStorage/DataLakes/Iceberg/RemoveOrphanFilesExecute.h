#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

Pipe executeRemoveOrphanFiles(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name);

}

#endif
