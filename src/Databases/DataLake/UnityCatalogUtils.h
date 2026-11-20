#pragma once
#include "config.h"

#if USE_PARQUET

#include <Core/Types.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/URI.h>

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

/// With these parameters, Databricks describes a streaming table or a materialized view as a managed table with a `storage_location`.
/// The owner of the table must enable external data access or Compatibility Mode for it.
/// See https://docs.databricks.com/aws/en/external-access/compatibility-mode
inline const Poco::URI::QueryParameters READ_AS_MANAGED_PARAMS = {
    {"read_streaming_table_as_managed", "true"},
    {"read_materialized_view_as_managed", "true"},
};

/// These table types get a `storage_location` only with `READ_AS_MANAGED_PARAMS`.
bool isStreamingTableOrMaterializedView(const Poco::JSON::Object::Ptr & table);
bool isStreamingTableOrMaterializedView(const String & table_type);

}

#endif
