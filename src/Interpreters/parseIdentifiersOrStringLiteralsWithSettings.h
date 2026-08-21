#pragma once

#include <Core/Types.h>

#include <unordered_set>
#include <vector>

namespace DB
{

struct Settings;

/// Parse a whole string as a list of identifiers or string literals, taking the parser limits from
/// the settings. These live outside `src/Parsers` so that the parser itself does not depend on the
/// settings schema; the parser-level overloads are in `Parsers/parseIdentifierOrStringLiteral.h`.

/// Parse a list of identifiers or string literals into a vector of strings.
std::vector<String> parseIdentifiersOrStringLiterals(const String & str, const Settings & settings);

/// Parse a list of identifiers or string literals into an unordered_set of strings.
std::unordered_set<String> parseIdentifiersOrStringLiteralsToSet(const String & str, const Settings & settings);

}
