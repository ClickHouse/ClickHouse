/*
Copyright (C) 2002 Brad King (brad.king@kitware.com)
                   Douglas Gregor (gregod@cs.rpi.edu)

Copyright (C) 2002, 2008, 2013 Peter Dimov

Copyright (C) 2017 Glen Joseph Fernandes (glenjofe@gmail.com)

Distributed under the Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

#ifndef BOOST_CORE_ADDRESSOF_HPP
#define BOOST_CORE_ADDRESSOF_HPP

#include <boost/config.hpp>

#if defined(BOOST_MSVC_FULL_VER) && BOOST_MSVC_FULL_VER >= 190024215
#define BOOST_CORE_HAS_BUILTIN_ADDRESSOF
#elif defined(BOOST_GCC) && BOOST_GCC >= 70000
#define BOOST_CORE_HAS_BUILTIN_ADDRESSOF
#elif defined(__has_builtin)
#if __has_builtin(__builtin_addressof)
#define BOOST_CORE_HAS_BUILTIN_ADDRESSOF
#endif
#endif

#if defined(BOOST_CORE_HAS_BUILTIN_ADDRESSOF)
#if defined(BOOST_NO_CXX11_CONSTEXPR)
#define BOOST_CORE_NO_CONSTEXPR_ADDRESSOF
#endif

namespace boost {

template<class T>
BOOST_CONSTEXPR inline T*
addressof(T& o) BOOST_NOEXCEPT
{
    return __builtin_addressof(o);
}

} /* boost */
#else
#include <boost/config/workaround.hpp>
#include <cstddef>

namespace boost {
namespace detail {

template<class T>
class addressof_ref {
public:
    BOOST_FORCEINLINE addressof_ref(T& o) BOOST_NOEXCEPT
        : o_(o) { }
    BOOST_FORCEINLINE operator T&() const BOOST_NOEXCEPT {
        return o_;
    }
private:
    addressof_ref& operator=(const addressof_ref&);
    T& o_;
};

template<class T>
struct address_of {
    static BOOST_FORCEINLINE T* get(T& o, long) BOOST_NOEXCEPT {
        return reinterpret_cast<T*>(&
            const_cast<char&>(reinterpret_cast<const volatile char&>(o)));
    }
    static BOOST_FORCEINLINE T* get(T* p, int) BOOST_NOEXCEPT {
        return p;
    }
};

#if !defined(BOOST_NO_CXX11_NULLPTR)
#if !defined(BOOST_NO_CXX11_DECLTYPE) && \
    (defined(__INTEL_COMPILER) || \
        (defined(__clang__) && !defined(_LIBCPP_VERSION)))
typedef decltype(nullptr) addressof_null_t;
#else
typedef std::nullptr_t addressof_null_t;
#endif

template<>
struct address_of<addressof_null_t> {
    typedef addressof_null_t type;
    static BOOST_FORCEINLINE type* get(type& o, int) BOOST_NOEXCEPT {
        return &o;
    }
};

template<>
struct address_of<const addressof_null_t> {
    typedef const addressof_null_t type;
    static BOOST_FORCEINLINE type* get(type& o, int) BOOST_NOEXCEPT {
        return &o;
    }
};

template<>
struct address_of<volatile addressof_null_t> {
    typedef volatile addressof_null_t type;
    static BOOST_FORCEINLINE type* get(type& o, int) BOOST_NOEXCEPT {
        return &o;
    }
};

template<>
struct address_of<const volatile addressof_null_t> {
    typedef const volatile addressof_null_t type;
    static BOOST_FORCEINLINE type* get(type& o, int) BOOST_NOEXCEPT {
        return &o;
    }
};
#endif

} /* detail */

#if defined(BOOST_NO_CXX11_SFINAE_EXPR) || \
    defined(BOOST_NO_CXX11_CONSTEXPR) || \
    defined(BOOST_NO_CXX11_DECLTYPE)
#define BOOST_CORE_NO_CONSTEXPR_ADDRESSOF

template<class T>
BOOST_FORCEINLINE T*
addressof(T& o) BOOST_NOEXCEPT
{
#if BOOST_WORKAROUND(__BORLANDC__, BOOST_TESTED_AT(0x610)) || \
    BOOST_WORKAROUND(__SUNPRO_CC, <= 0x5120)
    return detail::address_of<T>::get(o, 0);
#else
    return detail::address_of<T>::get(detail::addressof_ref<T>(o), 0);
#endif
}

#if BOOST_WORKAROUND(__SUNPRO_CC, BOOST_TESTED_AT(0x590))
namespace detail {

template<class T>
struct addressof_result {
    typedef T* type;
};

} /* detail */

template<class T, std::size_t N>
BOOST_FORCEINLINE typename detail::addressof_result<T[N]>::type
addressof(T (&o)[N]) BOOST_NOEXCEPT
{
    return &o;
}
#endif

#if BOOST_WORKAROUND(__BORLANDC__, BOOST_TESTED_AT(0x564))
template<class T, std::size_t N>
BOOST_FORCEINLINE
T (*addressof(T (&o)[N]) BOOST_NOEXCEPT)[N]
{
   return reinterpret_cast<T(*)[N]>(&o);
}

template<class T, std::size_t N>
BOOST_FORCEINLINE
const T (*addressof(const T (&o)[N]) BOOST_NOEXCEPT)[N]
{
   return reinterpret_cast<const T(*)[N]>(&o);
}
#endif
#else
namespace detail {

template<class T>
T addressof_declval() BOOST_NOEXCEPT;

template<class>
struct addressof_void {
    typedef void type;
};

template<class T, class E = void>
struct addressof_member_operator {
    static constexpr bool value = false;
};

template<class T>
struct addressof_member_operator<T, typename
    addressof_void<decltype(addressof_declval<T&>().operator&())>::type> {
    static constexpr bool value = true;
};

#if BOOST_WORKAROUND(BOOST_INTEL, < 1600)
struct addressof_addressable { };

addressof_addressable*
operator&(addressof_addressable&) BOOST_NOEXCEPT;
#endif

template<class T, class E = void>
struct addressof_non_member_operator {
    static constexpr bool value = false;
};

template<class T>
struct addressof_non_member_operator<T, typename
    addressof_void<decltype(operator&(addressof_declval<T&>()))>::type> {
    static constexpr bool value = true;
};

template<class T, class E = void>
struct addressof_expression {
    static constexpr bool value = false;
};

template<class T>
struct addressof_expression<T,
    typename addressof_void<decltype(&addressof_declval<T&>())>::type> {
    static constexpr bool value = true;
};

template<class T>
struct addressof_is_constexpr {
    static constexpr bool value = addressof_expression<T>::value &&
        !addressof_member_operator<T>::value &&
        !addressof_non_member_operator<T>::value;
};

template<bool E, class T>
struct addressof_if { };

template<class T>
struct addressof_if<true, T> {
    typedef T* type;
};

template<class T>
BOOST_FORCEINLINE
typename addressof_if<!addressof_is_constexpr<T>::value, T>::type
addressof(T& o) BOOST_NOEXCEPT
{
    return address_of<T>::get(addressof_ref<T>(o), 0);
}

template<class T>
constexpr BOOST_FORCEINLINE
typename addressof_if<addressof_is_constexpr<T>::value, T>::type
addressof(T& o) BOOST_NOEXCEPT
{
    return &o;
}

} /* detail */

template<class T>
constexpr BOOST_FORCEINLINE T*
addressof(T& o) BOOST_NOEXCEPT
{
    return detail::addressof(o);
}
#endif

} /* boost */
#endif

#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES) && \
    !defined(BOOST_NO_CXX11_DELETED_FUNCTIONS)
namespace boost {

template<class T>
const T* addressof(const T&&) = delete;

} /* boost */
#endif

#endif
