#pragma once
#include "config.h"

#include <Core/Types.h>
#include <Poco/JSON/Object.h>

namespace DataLake
{

/// Databricks deletes the data of a dropped `MANAGED` table and keeps the data of an `EXTERNAL` one, whatever the client
/// asks for. Throws unless `unity_table`, the Unity `GET /tables/<full_name>` response, is one of them and agrees with `delete_data`.
void checkUnityTableDrop(const Poco::JSON::Object::Ptr & unity_table, const String & namespace_name, const String & table_name, bool delete_data);

}

#if USE_PARQUET

#include <Poco/JSON/Array.h>

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
