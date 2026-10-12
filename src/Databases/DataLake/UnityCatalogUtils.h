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

/// Throws unless Unity reports this table, by its `table_type`, as external, i.e. one whose log
/// may be committed directly. Managed tables are owned by the catalog.
void checkUnityDirectCommitIsAllowed(const Poco::JSON::Object::Ptr & table_json, const String & full_table_name);

/// Percent-encodes a dot-separated Unity full name (`catalog.schema[.table]`) as one URL path segment.
String encodeUnityFullName(const String & full_name);

}

#endif
