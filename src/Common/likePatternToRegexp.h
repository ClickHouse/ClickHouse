#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

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


/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexp(std::string_view pattern);

/// Transforms the SIMILAR TO expression into regexp re2. For example, a.c%def -> ^a.c.*def$
String similarToPatternToRegexp(std::string_view pattern);

}
