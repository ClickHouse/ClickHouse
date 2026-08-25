#pragma once

#include <base/types.h>
#include <Common/PODArray.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names);
UInt64 normalizedQueryHash(const String & query, bool keep_names);
void normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data, bool keep_names);

/// An identifier is complex if it contains whitespace or more than two digits,
/// or if it is at least 36 bytes long (a UUID, for example). Such identifiers look generated,
/// so normalization replaces them with a placeholder.
bool isComplexIdentifier(const char * begin, const char * end);

/// Like `normalizedQueryHash`, but computed over the parsed query and insensitive to the order of
/// elements in lists where the order does not change what the query does: the `SELECT` expression
/// list, `GROUP BY` keys and the operands of `and` and `or`. `SELECT a, b FROM t` and
/// `SELECT b, a FROM t` get the same hash even though their results differ in column order, so this
/// is a way to group a workload by shape and must not be used to decide that two queries are
/// interchangeable.
UInt64 canonicalQueryHash(const IAST & ast);

}
