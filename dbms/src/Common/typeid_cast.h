#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <string>

#include <Common/Exception.h>
#include <common/demangle.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_CAST;
    }
}


/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */
template <typename To, typename From>
std::enable_if_t<std::is_reference_v<To>, To> typeid_cast(From & from)
{
    try
    {
        if (typeid(from) == typeid(To))
            return static_cast<To>(from);
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::BAD_CAST);
    }

    throw DB::Exception("Bad cast from type " + demangle(typeid(from).name()) + " to " + demangle(typeid(To).name()),
                        DB::ErrorCodes::BAD_CAST);
}

template <typename To, typename From>
To typeid_cast(From * from)
{
    try
    {
        if (typeid(*from) == typeid(std::remove_pointer_t<To>))
            return static_cast<To>(from);
        else
            return nullptr;
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::BAD_CAST);
    }
}
