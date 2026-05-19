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

}
