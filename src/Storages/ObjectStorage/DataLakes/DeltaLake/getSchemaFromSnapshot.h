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
/// `engine` is required by `ffi::get_from_string_map` (v0.23.0).
std::pair<DB::NamesAndTypesList, DB::NameToNameMap> getTableSchemaFromSnapshot(
    ffi::SharedSnapshot * snapshot,
    ffi::SharedExternEngine * engine);

/// Get read schema.
/// Represents read schema based on data files.
DB::NamesAndTypesList getReadSchemaFromSnapshot(ffi::SharedScan * scan, ffi::SharedExternEngine * engine);

DB::NamesAndTypesList getWriteSchema(ffi::SharedWriteContext * write_context, ffi::SharedExternEngine * engine);

/// Get list of partition columns.
/// Read schema does not contain partition columns,
/// therefore partition columns are passed separately.
DB::Names getPartitionColumnsFromSnapshot(ffi::SharedSnapshot * snapshot);

DB::NamesAndTypesList convertToClickHouseSchema(ffi::SharedSchema * schema, ffi::SharedExternEngine * engine);

/// Builds a delta-kernel `EngineSchema` view over a ClickHouse `NamesAndTypesList`.
/// The returned struct stores a pointer to `schema_list`; the caller must keep
/// `schema_list` alive for the duration of any FFI call that uses the returned value
/// (e.g. `ffi::get_create_table_builder`).
ffi::EngineSchema buildKernelEngineSchema(const DB::NamesAndTypesList & schema_list);

}

#endif
