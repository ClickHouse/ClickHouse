#pragma once

#include <base/types.h>

namespace DB
{

/// Normalize before matching source grants so `..` cannot escape a granted prefix.
/// Unparseable URIs require a grant on the whole source.
String normalizeAccessURI(const String & uri);

}
