#pragma once

#include <Databases/DataLake/ICatalog.h>
#include <Poco/JSON/Object.h>

namespace DataLake
{

/// Parses the labels of a table from the response of the Iceberg REST catalog to a table load (`LoadTableResult`):
///
///     "labels": {
///         "object-labels": {"owner": "team-a", "classification": "restricted"},
///         "fields": [
///             {"field-id": 2, "labels": {"pii": "true"}}
///         ]
///     }
///
/// `labels` and both of its members are optional. The names of the fields are resolved against
/// the current schema in `metadata` of the same response.
///
/// Labels are an optional enrichment which a client is free to ignore, so the parsing is lenient:
/// what does not follow the specification is skipped instead of failing the load of the table.
TableLabels parseIcebergRestTableLabels(const Poco::JSON::Object::Ptr & load_table_result);

}
