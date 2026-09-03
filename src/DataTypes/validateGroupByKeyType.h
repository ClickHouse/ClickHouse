#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// Validates that the data type is allowed in GROUP BY keys (and semantically equivalent
/// operations like window PARTITION BY). Throws ILLEGAL_COLUMN if Dynamic or Variant types
/// are found and allow_suspicious_types is false.
void validateGroupByKeyType(const DataTypePtr & key_type, bool allow_suspicious_types);

}
