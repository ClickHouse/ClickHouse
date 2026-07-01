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

/// Rewrites a LIKE pattern with custom escape character into a LIKE pattern with standard escape character (backslash).
/// Example: with escape_char='#': "50#%off" -> "50\%off"
String likePatternWithCustomEscapeToLikePattern(std::string_view pattern, char escape_char);

}
