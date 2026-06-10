#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

bool hasNullable(const DataTypePtr & type);

/// Whether a Variant or Dynamic appears anywhere in the type, including nested inside
/// Array/Tuple/Map. Such types can hold a NULL without a Nullable wrapper, so hasNullable()
/// returns false for them even though the values may be NULL.
bool hasVariantOrDynamic(const DataTypePtr & type);

}
