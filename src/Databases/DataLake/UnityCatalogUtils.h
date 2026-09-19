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

}

#endif
