#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

bool hasNullable(const DataTypePtr & type);

/// Whether a type that can hold a NULL (Nullable, LowCardinality(Nullable), Variant, Dynamic)
/// appears anywhere in the type, including nested inside Array/Tuple/Map. Variant and Dynamic
/// can be NULL without a Nullable wrapper, so hasNullable() alone is not sufficient.
bool hasTypeThatCanContainNulls(const DataTypePtr & type);

/// Whether a floating-point type appears anywhere in the type tree (see `anyInTypeTree`).
bool hasTypeThatCanContainFloat(const DataTypePtr & type);

/// Whether Object/JSON, Variant or Dynamic appears anywhere in the type tree. These hold values whose
/// type is only known at runtime, so a Field carrying one loses the discriminator their compareAt ranks by.
bool hasRuntimeTypedType(const DataTypePtr & type);

}
