#pragma once

#include <Common/typeid_cast.h>

namespace DB
{

class IDataType;

template <typename... Ts, typename F>
static bool castTypeToEither(const IDataType * type, F && f)
{
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    return ((typeid_cast<const Ts *>(type) ? f(*typeid_cast<const Ts *>(type)) : false) || ...);
}

}
