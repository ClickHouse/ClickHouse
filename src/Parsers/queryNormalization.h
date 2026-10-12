#pragma once

#include <base/types.h>
#include <base/sanitizer_defs.h>
#include <Common/PODArray.h>


namespace DB
{

UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names);
UInt64 normalizedQueryHash(const String & query, bool keep_names);
/// Sums the per-token hashes, so it wraps by design - and the attribute has to be here rather than
/// on the out-of-line definition, where it would be silently ignored.
UInt64 NO_SANITIZE_UNSIGNED_OVERFLOW normalizedQueryHashUnordered(const char * begin, const char * end);
void normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data, bool keep_names);

}
