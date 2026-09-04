#pragma once

#include <base/types.h>
#include <string_view>

/// SIMILAR TO's metacharacters consist of LIKE's and a subset of re2's:
/// - LIKE's: %_
/// - regex's: |*+?[](){}
/// - Exclude regex's: ^$.
/// Below we only focus on those not in LIKE's for case handling
#define SIMILAR_TO_EXCLUDING_LIKE_METACHARS(X) \
    X('|') \
    X('*') \
    X('+') \
    X('?') \
    X('{') \
    X('}') \
    X('(') \
    X(')') \
    X('[') \
    X(']')

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexp(std::string_view pattern);

/// Transforms the SIMILAR TO expression into regexp re2. For example, a.c%def -> ^a\.c.*def$
String similarToPatternToRegexp(std::string_view pattern);

/// Is the [I]LIKE / SIMILAR TO pattern equivalent to a substring search?
/// Returns true if the pattern is of the form '%substring%' (with no other wildcards),
/// and writes the extracted substring to 'res'. Templated on `is_similar_to` because the escape
/// semantics of the excluded metacharacters differ between the two grammars.
template <bool is_similar_to = false>
bool likePatternIsSubstring(std::string_view pattern, String & res);

/// Rewrites a LIKE pattern with custom escape character into a LIKE pattern with standard escape character (backslash).
/// Example: with escape_char='#': "50#%off" -> "50\%off"
String likePatternWithCustomEscapeToLikePattern(std::string_view pattern, char escape_char);

/// Rewrites a SIMILAR TO pattern with a custom escape character into a SIMILAR TO pattern with the
/// standard escape character (backslash), so it can then be fed to `similarToPatternToRegexp`.
/// The escape character makes the following character a literal (removing any special meaning).
/// Example: with escape_char='#': "a#_%|b" -> "a\_.*|b" is produced in two steps — here this
/// function yields "a\_%|b" (the `#_` becomes a literal `_`), which `similarToPatternToRegexp`
/// then translates to re2.
String similarToPatternWithCustomEscapeToSimilarToPattern(std::string_view pattern, char escape_char);

/// Checks if a LIKE pattern contains a backslash which does not form a valid escape sequence,
/// i.e. a backslash followed by a byte other than '%', '_' or '\', or a trailing backslash.
bool likePatternHasUnknownBackslashEscape(std::string_view pattern);

}
