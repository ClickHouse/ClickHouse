#pragma once

#include <Common/typeid_cast.h>

namespace DB
{

template <typename... Ts, typename T, typename F>
static bool castTypeToEither(const T * type, F && f)
{
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    return ((typeid_cast<const Ts *>(type) ? f(*typeid_cast<const Ts *>(type)) : false) || ...);
}

}
