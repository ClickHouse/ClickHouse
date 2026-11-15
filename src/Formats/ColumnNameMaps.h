#pragma once

#include <base/types.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

namespace DB
{

/// Field ids of columns (for example, Iceberg field ids), by column name or path.
using ColumnNameToFieldId = UnorderedMapWithMemoryTracking<String, Int64>;

/// Column names by field id, the reverse of `ColumnNameToFieldId`.
using FieldIdToColumnName = UnorderedMapWithMemoryTracking<Int64, String>;

/// Column names in a file by ClickHouse column names, or the reverse.
using ColumnNameMap = UnorderedMapWithMemoryTracking<String, String>;

}
