#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>
#include <delta_kernel_ffi.hpp>
#include <exception>

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

/// Validate that every column type round-trips through Delta metadata, throwing otherwise. Call before
/// the create-table FFI so unsupported types fail in C++, not inside the Rust visitor callback.
void validateSchemaForDeltaCreate(const DB::NamesAndTypesList & schema);

/// Caller-owned state for the kernel create-schema visitor: the schema to visit plus any exception it
/// raised, rethrown after the FFI returns (never through Rust frames). Mirrors `SchemaVisitorData`.
struct KernelCreateSchemaState
{
    const DB::NamesAndTypesList * schema_list = nullptr;
    std::exception_ptr exception;
};

/// Build a delta-kernel `EngineSchema` over `state.schema_list`; `state` must outlive the FFI call.
/// After the call, rethrow `state.exception` if set. Validate with `validateSchemaForDeltaCreate` first.
ffi::EngineSchema buildKernelEngineSchema(KernelCreateSchemaState & state);

}

#endif
