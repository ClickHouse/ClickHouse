//  (C) Copyright Daniel Frey and Robert Ramey 2009.
//  Use, modification and distribution are subject to the Boost Software License,
//  Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt).
//
//  See http://www.boost.org/libs/type_traits for most recent version including documentation.
 
#ifndef BOOST_TT_IS_VIRTUAL_BASE_OF_HPP_INCLUDED
#define BOOST_TT_IS_VIRTUAL_BASE_OF_HPP_INCLUDED

#include <boost/type_traits/is_base_of.hpp>
#include <boost/type_traits/is_same.hpp>

namespace boost {
namespace detail {


#ifdef BOOST_MSVC
#pragma warning( push )
#pragma warning( disable : 4584 4250 4594)
#elif defined(__GNUC__) && (__GNUC__ >= 4)
#pragma GCC system_header
#endif

template<typename Base, typename Derived, typename tag>
struct is_virtual_base_of_impl
{
    BOOST_STATIC_CONSTANT(bool, value = false);
};

template<typename Base, typename Derived>
struct is_virtual_base_of_impl<Base, Derived, true_type>
{
   union max_align
   {
      unsigned u;
      unsigned long ul;
      void* v;
      double d;
      long double ld;
#ifndef BOOST_NO_LONG_LONG
      long long ll;
#endif
   };
#ifdef __BORLANDC__
    struct boost_type_traits_internal_struct_X : public virtual Derived, public virtual Base 
    {
       boost_type_traits_internal_struct_X();
       boost_type_traits_internal_struct_X(const boost_type_traits_internal_struct_X&);
       boost_type_traits_internal_struct_X& operator=(const boost_type_traits_internal_struct_X&);
       ~boost_type_traits_internal_struct_X()throw();
       max_align data[4];
    };
    struct boost_type_traits_internal_struct_Y : public virtual Derived 
    {
       boost_type_traits_internal_struct_Y();
       boost_type_traits_internal_struct_Y(const boost_type_traits_internal_struct_Y&);
       boost_type_traits_internal_struct_Y& operator=(const boost_type_traits_internal_struct_Y&);
       ~boost_type_traits_internal_struct_Y()throw();
       max_align data[4];
    };
#else
    struct boost_type_traits_internal_struct_X : public Derived, virtual Base 
    {
       boost_type_traits_internal_struct_X();
       boost_type_traits_internal_struct_X(const boost_type_traits_internal_struct_X&);
       boost_type_traits_internal_struct_X& operator=(const boost_type_traits_internal_struct_X&);
       ~boost_type_traits_internal_struct_X()throw();
       max_align data[16];
    };
    struct boost_type_traits_internal_struct_Y : public Derived 
    {
       boost_type_traits_internal_struct_Y();
       boost_type_traits_internal_struct_Y(const boost_type_traits_internal_struct_Y&);
       boost_type_traits_internal_struct_Y& operator=(const boost_type_traits_internal_struct_Y&);
       ~boost_type_traits_internal_struct_Y()throw();
       max_align data[16];
    };
#endif
    BOOST_STATIC_CONSTANT(bool, value = (sizeof(boost_type_traits_internal_struct_X)==sizeof(boost_type_traits_internal_struct_Y)));
};

template<typename Base, typename Derived>
struct is_virtual_base_of_impl2
{
   typedef boost::integral_constant<bool, (boost::is_base_of<Base, Derived>::value && ! boost::is_same<Base, Derived>::value)> tag_type;
   typedef is_virtual_base_of_impl<Base, Derived, tag_type> imp;
   BOOST_STATIC_CONSTANT(bool, value = imp::value);
};

#ifdef BOOST_MSVC
#pragma warning( pop )
#endif

} // namespace detail

template <class Base, class Derived> struct is_virtual_base_of : public integral_constant<bool, (::boost::detail::is_virtual_base_of_impl2<Base, Derived>::value)>{};

template <class Base, class Derived> struct is_virtual_base_of<Base&, Derived> : public false_type{};
template <class Base, class Derived> struct is_virtual_base_of<Base, Derived&> : public false_type{};
template <class Base, class Derived> struct is_virtual_base_of<Base&, Derived&> : public false_type{};

} // namespace boost

#endif
