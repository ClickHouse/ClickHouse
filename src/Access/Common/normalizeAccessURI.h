#pragma once

#include <base/types.h>

namespace DB
{

/// Normalize a URI the way a source-filter grant (`GRANT READ ON S3('...')`) matches it. Every place
/// that authorizes a location has to normalize it identically, or the same grant matches a location
/// reached one way and not the same location reached another -- and a filter anchored at a prefix is
/// escaped by a `..` segment. A URI Poco cannot parse normalizes to an empty string, which no filter
/// matches, so it requires a grant on the whole source.
String normalizeAccessURI(const String & uri);

}
