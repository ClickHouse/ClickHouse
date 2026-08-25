#pragma once

#include <base/types.h>
#include <Common/PODArray.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names);
UInt64 normalizedQueryHash(const String & query, bool keep_names);
void normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data, bool keep_names);

/// looks generated: has whitespace, more than two digits, or is 36+ bytes long
bool isComplexIdentifier(const char * begin, const char * end);

/// normalizedQueryHash that ignores the order inside every expression list, so SELECT a, b and SELECT b, a match
/// deliberately lossy - it merges ORDER BY a, b with ORDER BY b, a too, so group a workload with it, do not compare queries
UInt64 unorderedQueryHash(const IAST & ast);

}
