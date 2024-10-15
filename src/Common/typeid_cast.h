#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <memory>
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


/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */
template <typename To, typename From>
requires std::is_reference_v<To>
To typeid_cast(From & from) noexcept(false)
{
    if ((typeid(From) == typeid(To)) || (typeid(from) == typeid(To)))
        return static_cast<To>(from);

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to {}",
                        demangle(typeid(from).name()), demangle(typeid(To).name()));
}


template <typename To, typename From>
requires std::is_pointer_v<To>
To typeid_cast(From * from) noexcept
{
    if ((typeid(From) == typeid(std::remove_pointer_t<To>)) || (from && typeid(*from) == typeid(std::remove_pointer_t<To>)))
        return static_cast<To>(from);
    return nullptr;
}

namespace detail
{

template <typename T>
struct is_shared_ptr : std::false_type
{
};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type
{
};

template <typename T>
inline constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;

}

template <typename To, typename From>
requires detail::is_shared_ptr_v<To>
To typeid_cast(const std::shared_ptr<From> & from) noexcept
{
    if ((typeid(From) == typeid(typename To::element_type)) || (from && typeid(*from) == typeid(typename To::element_type)))
        return std::static_pointer_cast<typename To::element_type>(from);
    return nullptr;
}
