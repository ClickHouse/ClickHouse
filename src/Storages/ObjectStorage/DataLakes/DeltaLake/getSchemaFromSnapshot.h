#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>
#include <delta_kernel_ffi.hpp>
#include <Poco/JSON/Array.h>
#include <exception>
#include <unordered_map>
#include <unordered_set>

namespace ffi
{
struct SharedSnapshot;
struct SharedGlobalScanState;
}

namespace DeltaLake
{

/// Delta type behind a top-level column that ClickHouse reads as `DateTime64(6)`;
/// selects the kernel literal a comparison with that column is translated to.
enum class DeltaTimestampKind : uint8_t
{
    Timestamp,
    TimestampNtz,
};

struct DeltaTimestampColumn
{
    DeltaTimestampKind kind;
    /// Whether the Delta field is nullable: its NULLs are left out of the file statistics.
    bool nullable;
};

/// Top-level `timestamp` / `timestamp_ntz` columns by logical name, partition columns included.
using DeltaTimestampColumns = std::unordered_map<String, DeltaTimestampColumn>;

struct TableSchemaResult
{
    DB::NamesAndTypesList schema;
    /// Logical name to physical name mapping, for columnMapping.mode = 'name'.
    DB::NameToNameMap physical_names_map;
    /// Dotted paths of the `timestamp_ntz` leaves, spelled as the Parquet writer spells a column path:
    /// a struct field's own name, `element` for an array child, `key`/`value` for a map's.
    std::unordered_set<String> timestamp_ntz_paths;
    /// Columns whose comparisons the engine predicate translates to timestamp literals. Leaves out
    /// a column widened from another type (`delta.typeChanges`, e.g. `date` -> `timestamp_ntz`): files
    /// written before the change keep the old physical type and statistics, which ClickHouse and
    /// delta-kernel read differently.
    DeltaTimestampColumns timestamp_columns;
};

/// Get table schema and physical column map (logical name to physical name mapping).
/// Represents table schema from DeltaLake metadata.
/// Contains partition columns.
/// `engine` is required by `ffi::get_from_string_map` (v0.23.0).
TableSchemaResult getTableSchemaFromSnapshot(
    ffi::SharedSnapshot * snapshot,
    ffi::SharedExternEngine * engine);

/// Get read schema.
/// Represents read schema based on data files.
DB::NamesAndTypesList getReadSchemaFromSnapshot(ffi::SharedScan * scan, ffi::SharedExternEngine * engine);

struct WriteSchemaResult
{
    DB::NamesAndTypesList schema;
    std::unordered_set<String> timestamp_ntz_paths;
};

WriteSchemaResult getWriteSchema(ffi::SharedWriteContext * write_context, ffi::SharedExternEngine * engine);

/// Get list of partition columns.
/// Read schema does not contain partition columns,
/// therefore partition columns are passed separately.
DB::Names getPartitionColumnsFromSnapshot(ffi::SharedSnapshot * snapshot);

DB::NamesAndTypesList convertToClickHouseSchema(ffi::SharedSchema * schema, ffi::SharedExternEngine * engine);

/// Raw Delta `StructType.fields` JSON for the snapshot's logical schema, preserving the exact Delta types
/// (`binary`, `timestamp_ntz`, `decimal(p,s)`, nested array/map/struct) that `convertToClickHouseSchema`
/// collapses. Used for catalog registration so the registered schema matches the `_delta_log` on storage.
Poco::JSON::Array::Ptr getDeltaSchemaFieldsFromSnapshot(ffi::SharedSnapshot * snapshot);

/// Validate that every column type round-trips through Delta metadata (throwing otherwise) before the create-table FFI.
void validateSchemaForDeltaCreate(const DB::NamesAndTypesList & schema);

/// Caller-owned state for the kernel create-schema visitor: the schema to visit plus any exception it raised.
struct KernelCreateSchemaState
{
    const DB::NamesAndTypesList * schema_list = nullptr;
    std::exception_ptr exception;
};

/// Build a delta-kernel `EngineSchema` over `state.schema_list`; `state` must outlive the FFI call.
ffi::EngineSchema buildKernelEngineSchema(KernelCreateSchemaState & state);

}

#endif
