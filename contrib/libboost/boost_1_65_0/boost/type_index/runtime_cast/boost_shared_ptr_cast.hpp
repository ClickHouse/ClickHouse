//
// Copyright (c) Chris Glover, 2016.
//
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef BOOST_TYPE_INDEX_RUNTIME_CAST_BOOST_SHARED_PTR_CAST_HPP
#define BOOST_TYPE_INDEX_RUNTIME_CAST_BOOST_SHARED_PTR_CAST_HPP

/// \file boost_shared_ptr_cast.hpp
/// \brief Contains the overload of boost::typeindex::runtime_pointer_cast for
/// boost::shared_ptr types.

#include <boost/type_index/runtime_cast/detail/runtime_cast_impl.hpp>
#include <boost/type_traits/is_base_and_derived.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#ifdef BOOST_HAS_PRAGMA_ONCE
# pragma once
#endif

namespace boost { namespace typeindex {

/// \brief Creates a new instance of std::shared_ptr whose stored pointer is obtained from u's
/// stored pointer using a runtime_cast.
///
/// The new shared_ptr will share ownership with u, except that it is empty if the runtime_cast
/// performed by runtime_pointer_cast returns a null pointer.
/// \tparam T The desired target type to return a pointer of.
/// \tparam U A complete class type of the source instance pointed to from u.
/// \return If there exists a valid conversion from U* to T*, returns a boost::shared_ptr<T>
/// that points to an address suitably offset from u.
/// If no such conversion exists, returns boost::shared_ptr<T>();
template<typename T, typename U>
boost::shared_ptr<T> runtime_pointer_cast(boost::shared_ptr<U> const& u) {
    T* value = detail::runtime_cast_impl<T>(u.get(), boost::is_base_and_derived<T, U>());
    if(value)
        return boost::shared_ptr<T>(u, value);
    return boost::shared_ptr<T>();
}

}} // namespace boost::typeindex

#endif // BOOST_TYPE_INDEX_RUNTIME_CAST_BOOST_SHARED_PTR_CAST_HPP
