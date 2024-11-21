#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <string>

#include <Common/Exception.h>
#include <base/demangle.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }
}


/** Perform static_cast in release build.
  * Checks type by comparing typeid and throw an exception in debug build.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  */
template <typename To, typename From>
inline To assert_cast(From && from)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    try
    {
        if constexpr (std::is_pointer_v<To>)
        {
            if (typeid(*from) == typeid(std::remove_pointer_t<To>))
                return static_cast<To>(from);
        }
        else
        {
            if (typeid(from) == typeid(To))
                return static_cast<To>(from);
        }
    }
    catch (const std::exception & e)
    {
        throw DB::Exception::createDeprecated(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to {}",
                        demangle(typeid(from).name()), demangle(typeid(To).name()));
#else
    return static_cast<To>(from);
#endif
}
