#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class IDataType;

/// Returns true when the column type requires structured MergeTree substream names
/// to avoid collisions (for example Nullable(Array(Nullable(T)))).
bool needsStructuredSubstreamNames(const IDataType & type);

/// Suffix for a MergeTree substream file name (including leading dots) for types that
/// require structured naming. The suffix is derived from the full substream path.
String getStructuredSubstreamNameSuffix(const ISerialization::SubstreamPath & path);

/// Returns true when the substream path itself indicates structured naming is needed,
/// even if the static column type does not (e.g. Dynamic with a runtime variant
/// containing Nullable(Array(...))).
bool needsStructuredSubstreamNamesForPath(const ISerialization::SubstreamPath & path);

/// Returns true when the path can only have come from a column that needs structured substream
/// names, i.e. a `Nullable` wrapping an `Array` was descended through to produce it.
///
/// This is strictly weaker than "this path needs structured names": the naming of some paths depends
/// on the column type and cannot be decided from the path at all. It is only good for detecting that
/// a caller which supplied no column type has definitely lost information it needed.
bool pathRequiresStructuredSubstreamNames(const ISerialization::SubstreamPath & path);

}
