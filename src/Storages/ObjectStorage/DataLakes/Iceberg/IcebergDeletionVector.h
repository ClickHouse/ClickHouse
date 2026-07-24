#pragma once
#include "config.h"

#if USE_AVRO

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

namespace DB::Iceberg
{

DataLakeObjectMetadata::ExcludedRowsPtr loadDeletionVector(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & expected_data_file,
    const std::optional<IcebergPathFromMetadata> & referenced_data_file,
    Int64 expected_cardinality,
    ContextPtr context,
    LoggerPtr log);

}

#endif
