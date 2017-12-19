#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <string>

#include <Common/Exception.h>
#include <Common/demangle.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_CAST;
    }
}


/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast in the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */
template <typename To, typename From>
typename std::enable_if<std::is_reference<To>::value, To>::type typeid_cast(From & from)
{
    if (typeid(from) == typeid(To))
        return static_cast<To>(from);
    else
        throw DB::Exception("Bad cast from type " + demangle(typeid(from).name()) + " to " + demangle(typeid(To).name()),
            DB::ErrorCodes::BAD_CAST);
}

template <typename To, typename From>
To typeid_cast(From * from)
{
    if (typeid(*from) == typeid(typename std::remove_pointer<To>::type))
        return static_cast<To>(from);
    else
        return nullptr;
}
