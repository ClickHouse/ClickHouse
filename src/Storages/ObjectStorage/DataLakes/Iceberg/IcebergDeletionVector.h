#pragma once
#include "config.h"

#if USE_AVRO

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <span>
#include <vector>

namespace DB::Iceberg
{

/// Reject DV positions outside `[0, data_file_record_count)` and cardinality that cannot fit.
void validateDeletionVectorPositionsAgainstDataFile(
    std::span<const UInt64> deleted_positions,
    UInt64 expected_cardinality,
    Int64 data_file_record_count);

DataLakeObjectMetadata::ExcludedRowsPtr loadDeletionVector(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & expected_data_file,
    const std::optional<IcebergPathFromMetadata> & referenced_data_file,
    Int64 expected_cardinality,
    Int64 data_file_record_count,
    ContextPtr context,
    LoggerPtr log);

}

#endif
