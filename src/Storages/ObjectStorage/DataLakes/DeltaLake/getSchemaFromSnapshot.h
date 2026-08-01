#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>
#include <delta_kernel_ffi.hpp>

namespace ffi
{
struct SharedSnapshot;
struct SharedGlobalScanState;
}

namespace DeltaLake
{

/// Get table schema and physical column map (logical name to physical name mapping).
/// Represents table schema from DeltaLake metadata.
/// Contains partition columns.
std::pair<DB::NamesAndTypesList, DB::NameToNameMap> getTableSchemaFromSnapshot(ffi::SharedSnapshot * snapshot);

/// Get read schema.
/// Represents read schema based on data files.
DB::NamesAndTypesList getReadSchemaFromSnapshot(ffi::SharedScan * scan);

DB::NamesAndTypesList getWriteSchema(ffi::SharedWriteContext * write_context);

/// Get list of partition columns.
/// Read schema does not contain partition columns,
/// therefore partition columns are passed separately.
DB::Names getPartitionColumnsFromSnapshot(ffi::SharedSnapshot * snapshot);

}

#endif
