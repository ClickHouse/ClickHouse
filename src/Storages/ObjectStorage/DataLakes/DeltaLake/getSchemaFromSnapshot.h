#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>

namespace ffi
{
struct SharedSnapshot;
struct SharedExternEngine;
}

namespace DeltaLake
{

/// Get table schema.
/// Represents table schema from DeltaLake metadata.
/// Contains partition columns.
DB::NamesAndTypesList getTableSchemaFromSnapshot(ffi::SharedSnapshot * snapshot);

/// Get read schema and partition columns.
/// Represents read schema based on data files.
/// Read schema does not contain partition columns,
/// therefore partition columns are passed separately.
std::pair<DB::NamesAndTypesList, DB::Names>
getReadSchemaAndPartitionColumnsFromSnapshot(ffi::SharedSnapshot * snapshot, ffi::SharedExternEngine * engine);

DB::Names getPartitionColumnsFromSnapshot(ffi::SharedSnapshot * snapshot, ffi::SharedExternEngine * engine);

}

#endif
