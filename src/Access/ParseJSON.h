#pragma once

#include "config.h"

#if USE_JWT_CPP

#include <base/types.h>
#include <picojson/picojson.h>

/// Pinned at our only direct include of picojson so a contrib bump cannot drop them silently.
/// See `contrib/jwt-cpp-cmake/CMakeLists.txt` for why each one is needed.
#ifndef PICOJSON_USE_INT64
#error "PICOJSON_USE_INT64 must be defined for every picojson consumer, or picojson::value violates ODR"
#endif
static_assert(PICOJSON_USE_LOCALE == 0, "PICOJSON_USE_LOCALE must be 0, picojson would otherwise call the banned localeconv");

namespace DB
{

/// Parses `text` as one complete JSON value. Returns an error description, or an empty string.
/// Unlike `picojson::parse`, rejects trailing content: picojson stops after the first value and
/// ignores the rest, so `{"role":"admin"} AND {"tenant":"acme"}` would parse as just the object.
inline String parseWholeJSON(picojson::value & json, const String & text)
{
    const char * begin = text.data();
    const char * end = begin + text.size();

    String error;
    const char * rest = picojson::parse(json, begin, end, &error);
    if (!error.empty())
        return error;

    while (rest != end && (*rest == ' ' || *rest == '\t' || *rest == '\n' || *rest == '\r'))
        ++rest;
    if (rest != end)
        return "unexpected trailing characters after the JSON value";

    return {};
}

}

#endif
