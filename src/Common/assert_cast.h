#pragma once

#include <exception>
#include <typeinfo>

#ifdef DEBUG_OR_SANITIZER_BUILD
#include <type_traits>
#include <typeindex>
#include <string>
#endif


[[noreturn]] void throwBadAssertCast(const std::type_info & from, const std::type_info & to);
[[noreturn]] void throwBadAssertCastFromException(const std::exception & e);


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
        throwBadAssertCastFromException(e);
    }

    throwBadAssertCast(typeid(from), typeid(To));
#else
    return static_cast<To>(from);
#endif
}
