#pragma once

#include <Common/typeid_cast.h>
#include <base/TypeList.h>

namespace DB
{

template <typename... Ts, typename T, typename F>
static bool castTypeToEither(const T * type, F && f)
{
    return ((typeid_cast<const Ts *>(type) && f(*typeid_cast<const Ts *>(type))) || ...);
}

template <class ...Args>
static bool castTypeToEither(TypeList<Args...>, const auto * type, auto && f)
{
    return ((typeid_cast<const Args *>(type) != nullptr && f(*typeid_cast<const Args *>(type))) || ...);
}

}
