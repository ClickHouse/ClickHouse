#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>

#include <base/shared_ptr_helper.h>
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
To typeid_cast(From & from) requires(std::is_reference_v<To>)
{
    try
    {
        if ((typeid(From) == typeid(To)) || (typeid(from) == typeid(To)))
            return static_cast<To>(from);
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
        "Bad cast from {} to {}",
        demangle(typeid(from).name()), demangle(typeid(To).name()));
}

template <typename To, typename From>
To typeid_cast(From * from) requires(std::is_pointer_v<To>)
{
    using Unpointered = std::remove_pointer_t<To>;
    const std::type_info& to_info = typeid(Unpointered);

    try
    {
        if ((typeid(From) == to_info) || (from && typeid(*from) == to_info))
            return static_cast<To>(from);
        else
            return nullptr;
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}

template <typename To, typename From>
To typeid_cast(const std::shared_ptr<From> & from) requires(is_shared_ptr_v<To>)
{
    using Elem = typename To::element_type;

    try
    {
        if ((typeid(From) == typeid(Elem)) || (from && typeid(*from) == typeid(Elem)))
            return std::static_pointer_cast<Elem>(from);
        else
            return nullptr;
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}
