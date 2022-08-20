#pragma once

#include <DataTypes/IDataType.h>
#include <functional>

namespace DB
{

/// Function that applies custom transformation functions to provided types recursively.
/// Implementation is similar to function getLeastSuperType:
/// If all types are Array/Map/Tuple/Nullable, this function will be called to nested types.
/// If not all types are the same complex type (Array/Map/Tuple), this function won't be called to nested types.
/// Function transform_simple_types will be applied to resulting simple types after all recursive calls.
/// Function transform_complex_types will be applied to complex types (Array/Map/Tuple) after recursive call to their nested types.
void transformTypesRecursively(DataTypes & types, std::function<void(DataTypes &)> transform_simple_types, std::function<void(DataTypes &)> transform_complex_types);

}
