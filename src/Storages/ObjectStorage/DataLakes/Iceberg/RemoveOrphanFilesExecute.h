#pragma once

#include "config.h"

#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB::Iceberg
{

Pipe executeRemoveOrphanFiles(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components,
    SecondaryStorages & secondary_storages);

}

#endif
