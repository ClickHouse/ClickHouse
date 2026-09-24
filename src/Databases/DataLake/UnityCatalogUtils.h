#pragma once
#include "config.h"

#if USE_PARQUET

#include <Core/Types.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

namespace DataLake
{

/// Delta schema fields -> Unity `ColumnInfo` array; `type_json` must match what the read path parses back.
Poco::JSON::Array::Ptr buildUnityColumnsFromDeltaSchema(const Poco::JSON::Array::Ptr & fields);

/// Body of `POST /tables` registering an external Delta table.
Poco::JSON::Object::Ptr buildUnityCreateTableBody(
    const String & catalog_name,
    const String & schema_name,
    const String & table_name,
    const String & storage_location,
    Poco::JSON::Array::Ptr columns);

/// Whether Unity reports this table as one whose data the catalog owns, from `table_type`.
/// An unlisted string value, and an absent or null field, mean not owned.
bool isManagedUnityTable(const Poco::JSON::Object::Ptr & table_json);

[[noreturn]] void throwUnityManagedTableWriteRefusal(const String & full_table_name);

}

#endif
