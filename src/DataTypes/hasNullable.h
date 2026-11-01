#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

bool hasNullable(const DataTypePtr & type);

/// Whether a type that can hold a NULL (Nullable, LowCardinality(Nullable), Variant, Dynamic)
/// appears anywhere in the type, including nested inside Array/Tuple/Map. Variant and Dynamic
/// can be NULL without a Nullable wrapper, so hasNullable() alone is not sufficient.
bool hasTypeThatCanContainNulls(const DataTypePtr & type);

}
