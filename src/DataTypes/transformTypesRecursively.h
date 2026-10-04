#pragma once

#include <DataTypes/IDataType.h>
#include <functional>

namespace DB
{

struct FormatSettings;

/// Function that applies custom transformation functions to provided types recursively.
/// Implementation is similar to function getLeastSuperType:
/// If all types are Array/Map/Tuple/Nullable, this function will be called to nested types.
/// If not all types are the same complex type (Array/Map/Tuple), this function won't be called to nested types.
/// Function transform_simple_types will be applied to resulting simple types after all recursive calls.
/// Function transform_complex_types will be applied to complex types (Array/Map/Tuple) after recursive call to their nested types.
void transformTypesRecursively(
    DataTypes & types,
    std::function<void(DataTypes &, TypeIndexesSet &)> transform_simple_types,
    std::function<void(DataTypes &, TypeIndexesSet &)> transform_complex_types,
    const FormatSettings * format_settings = nullptr);

void callOnNestedSimpleTypes(DataTypePtr & type, std::function<void(DataTypePtr &)> callback);

/// Answers with the type to put in place of `left`, which may be `left` itself, or nullptr to refuse the pair.
using PairedLeafCallback = std::function<DataTypePtr(const DataTypePtr & left, const DataTypePtr & right)>;

/// Walks `left` and `right` together through Nullable, LowCardinality, Array, Map, Tuple and Variant, and
/// rebuilds `left` out of what `on_leaf` answers for the pairs of leaves the walk reaches. Returns `left`
/// itself when no leaf moved, a rebuilt type when one did, and nullptr when `on_leaf` refused a pair or the
/// two structures do not line up.
///
/// A level is rebuilt only if one of its children moved, because a rebuild loses that level's customization
/// (`Point` is a named `Tuple`). Where the two sides carry different custom names neither survives a rebuild,
/// so such a level resolves to `right` when the pair is `equals`-equal and every child cleared, and is
/// refused otherwise. A `Nullable` or `LowCardinality` present on `right` alone is stepped over, since
/// neither wrapper hides a leaf.
DataTypePtr replaceNestedTypesInPair(const DataTypePtr & left, const DataTypePtr & right, const PairedLeafCallback & on_leaf);

}
