#pragma once
#include "config.h"

#if USE_PARQUET

#include <unordered_set>
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

/// The key is present and its value is not null or empty.
bool hasValueAndItsNotNone(const std::string & value, const Poco::JSON::Object::Ptr & object);

/// `securable_kind` and `data_source_format` values that the Delta-only Unity catalog accepts.
inline const std::unordered_set<std::string> UNITY_DELTA_SECURABLE_KINDS = {"TABLE_DELTA", "TABLE_DELTA_EXTERNAL"};
inline constexpr auto UNITY_DELTA_DATA_SOURCE_FORMAT = "DELTA";

/// The rule of the Delta-only Unity catalog for a Delta table.
/// `securable_kind` and `data_source_format` must name Delta when present. At least one must be present.
bool isUnityDeltaTable(const Poco::JSON::Object::Ptr & table_json);

}

#endif
