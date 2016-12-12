//  (C) Copyright Gennadiy Rozental 2001.
//  Distributed under the Boost Software License, Version 1.0.
//  (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/test for the library home page.
//
//  File        : $RCSfile$
//
//  Version     : $Revision$
//
//  Description : defines the is_cstring type trait
// ***************************************************************************

#ifndef BOOST_TEST_UTILS_IS_CSTRING_HPP
#define BOOST_TEST_UTILS_IS_CSTRING_HPP

// Boost
#include <boost/mpl/bool.hpp>
#include <boost/type_traits/is_same.hpp>
#include <boost/type_traits/decay.hpp>
#include <boost/type_traits/remove_pointer.hpp>

//____________________________________________________________________________//

namespace boost {
namespace unit_test {

// ************************************************************************** //
// **************                  is_cstring                  ************** //
// ************************************************************************** //

namespace ut_detail {

template<typename T>
struct is_cstring_impl : public mpl::false_ {};

template<typename T>
struct is_cstring_impl<T const*> : public is_cstring_impl<T*> {};

template<typename T>
struct is_cstring_impl<T const* const> : public is_cstring_impl<T*> {};

template<>
struct is_cstring_impl<char*> : public mpl::true_ {};

template<>
struct is_cstring_impl<wchar_t*> : public mpl::true_ {};

} // namespace ut_detail

template<typename T>
struct is_cstring : public ut_detail::is_cstring_impl<typename decay<T>::type> {};

} // namespace unit_test
} // namespace boost

#endif // BOOST_TEST_UTILS_IS_CSTRING_HPP
