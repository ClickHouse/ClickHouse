#pragma once

#include <Common/typeid_cast.h>

namespace DB
{

/* This base class adds public methods:
 *  -       Derived * as<Derived>()
 *  - const Derived * as<Derived>() const
 *  -       Derived & as<Derived &>()
 *  - const Derived & as<Derived &>() const
 */

template <class Base>
class TypePromotion
{
private:
    /// Need a helper-struct to fight the lack of the function-template partial specialization.
    template <class T, bool is_const, bool is_ref = std::is_reference_v<T>>
    struct CastHelper;

    template <class T>
    struct CastHelper<T, false, true>
    {
        auto & value(Base * ptr) { return typeid_cast<T>(*ptr); }
    };

    template <class T>
    struct CastHelper<T, true, true>
    {
        auto & value(const Base * ptr) { return typeid_cast<std::add_lvalue_reference_t<std::add_const_t<std::remove_reference_t<T>>>>(*ptr); }
    };

    template <class T>
    struct CastHelper<T, false, false>
    {
        auto * value(Base * ptr) { return typeid_cast<T *>(ptr); }
    };

    template <class T>
    struct CastHelper<T, true, false>
    {
        auto * value(const Base * ptr) { return typeid_cast<std::add_const_t<T> *>(ptr); }
    };

public:
    template <class Derived>
    auto as() -> std::invoke_result_t<decltype(&CastHelper<Derived, false>::value), CastHelper<Derived, false>, Base *>
    {
        // TODO: if we do downcast to base type, then just return |this|.
        return CastHelper<Derived, false>().value(static_cast<Base *>(this));
    }

    template <class Derived>
    auto as() const -> std::invoke_result_t<decltype(&CastHelper<Derived, true>::value), CastHelper<Derived, true>, const Base *>
    {
        // TODO: if we do downcast to base type, then just return |this|.
        return CastHelper<Derived, true>().value(static_cast<const Base *>(this));
    }
};

} // namespace DB
