#pragma once

#include <base/types.h>
#include <Common/PODArray.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names);
UInt64 normalizedQueryHash(const String & query, bool keep_names);
void normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data, bool keep_names);

/// Looks generated: has whitespace, more than two digits, or is 36+ bytes long (a UUID).
bool isComplexIdentifier(const char * begin, const char * end);

/// Like `normalizedQueryHash`, but over the parsed query, so `SELECT a, b` and `SELECT b, a` match.
/// Groups a workload by shape - the two queries are not interchangeable, their columns come out in a different order.
UInt64 canonicalQueryHash(const IAST & ast);

}
