/* Copyright 2003-2015 Joaquin M Lopez Munoz.
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * See http://www.boost.org/libs/multi_index for library home page.
 */

#ifndef BOOST_MULTI_INDEX_MEM_FUN_HPP
#define BOOST_MULTI_INDEX_MEM_FUN_HPP

#if defined(_MSC_VER)
#pragma once
#endif

#include <boost/config.hpp> /* keep it first to prevent nasty warns in MSVC */
#include <boost/mpl/if.hpp>
#include <boost/type_traits/remove_reference.hpp>
#include <boost/utility/enable_if.hpp>

#if !defined(BOOST_NO_SFINAE)
#include <boost/type_traits/is_convertible.hpp>
#endif

namespace boost{

template<class T> class reference_wrapper; /* fwd decl. */

namespace multi_index{

/* mem_fun implements a read-only key extractor based on a given non-const
 * member function of a class.
 * const_mem_fun does the same for const member functions.
 * Additionally, mem_fun  and const_mem_fun are overloaded to support
 * referece_wrappers of T and "chained pointers" to T's. By chained pointer
 * to T we  mean a type P such that, given a p of Type P
 *   *...n...*x is convertible to T&, for some n>=1.
 * Examples of chained pointers are raw and smart pointers, iterators and
 * arbitrary combinations of these (vg. T** or unique_ptr<T*>.)
 */

template<class Class,typename Type,Type (Class::*PtrToMemberFunction)()const>
struct const_mem_fun
{
  typedef typename remove_reference<Type>::type result_type;

  template<typename ChainedPtr>

#if !defined(BOOST_NO_SFINAE)
  typename disable_if<
    is_convertible<const ChainedPtr&,const Class&>,Type>::type
#else
  Type
#endif

  operator()(const ChainedPtr& x)const
  {
    return operator()(*x);
  }

  Type operator()(const Class& x)const
  {
    return (x.*PtrToMemberFunction)();
  }

  Type operator()(const reference_wrapper<const Class>& x)const
  { 
    return operator()(x.get());
  }

  Type operator()(const reference_wrapper<Class>& x)const
  { 
    return operator()(x.get());
  }
};

template<class Class,typename Type,Type (Class::*PtrToMemberFunction)()>
struct mem_fun
{
  typedef typename remove_reference<Type>::type result_type;

  template<typename ChainedPtr>

#if !defined(BOOST_NO_SFINAE)
  typename disable_if<
    is_convertible<ChainedPtr&,Class&>,Type>::type
#else
  Type
#endif

  operator()(const ChainedPtr& x)const
  {
    return operator()(*x);
  }

  Type operator()(Class& x)const
  {
    return (x.*PtrToMemberFunction)();
  }

  Type operator()(const reference_wrapper<Class>& x)const
  { 
    return operator()(x.get());
  }
};

/* MSVC++ 6.0 has problems with const member functions as non-type template
 * parameters, somehow it takes them as non-const. const_mem_fun_explicit
 * workarounds this deficiency by accepting an extra type parameter that
 * specifies the signature of the member function. The workaround was found at:
 *   Daniel, C.:"Re: weird typedef problem in VC",
 *   news:microsoft.public.vc.language, 21st nov 2002, 
 *   http://groups.google.com/groups?
 *     hl=en&lr=&ie=UTF-8&selm=ukwvg3O0BHA.1512%40tkmsftngp05
 *
 * MSVC++ 6.0 support has been dropped and [const_]mem_fun_explicit is
 * deprecated.
 */

template<
  class Class,typename Type,
  typename PtrToMemberFunctionType,PtrToMemberFunctionType PtrToMemberFunction>
struct const_mem_fun_explicit
{
  typedef typename remove_reference<Type>::type result_type;

  template<typename ChainedPtr>

#if !defined(BOOST_NO_SFINAE)
  typename disable_if<
    is_convertible<const ChainedPtr&,const Class&>,Type>::type
#else
  Type
#endif

  operator()(const ChainedPtr& x)const
  {
    return operator()(*x);
  }

  Type operator()(const Class& x)const
  {
    return (x.*PtrToMemberFunction)();
  }

  Type operator()(const reference_wrapper<const Class>& x)const
  { 
    return operator()(x.get());
  }

  Type operator()(const reference_wrapper<Class>& x)const
  { 
    return operator()(x.get());
  }
};

template<
  class Class,typename Type,
  typename PtrToMemberFunctionType,PtrToMemberFunctionType PtrToMemberFunction>
struct mem_fun_explicit
{
  typedef typename remove_reference<Type>::type result_type;

  template<typename ChainedPtr>

#if !defined(BOOST_NO_SFINAE)
  typename disable_if<
    is_convertible<ChainedPtr&,Class&>,Type>::type
#else
  Type
#endif

  operator()(const ChainedPtr& x)const
  {
    return operator()(*x);
  }

  Type operator()(Class& x)const
  {
    return (x.*PtrToMemberFunction)();
  }

  Type operator()(const reference_wrapper<Class>& x)const
  { 
    return operator()(x.get());
  }
};

/* BOOST_MULTI_INDEX_CONST_MEM_FUN and BOOST_MULTI_INDEX_MEM_FUN used to
 * resolve to [const_]mem_fun_explicit for MSVC++ 6.0 and to
 * [const_]mem_fun otherwise. Support for this compiler having been dropped,
 * they are now just wrappers over [const_]mem_fun kept for backwards-
 * compatibility reasons.
 */

#define BOOST_MULTI_INDEX_CONST_MEM_FUN(Class,Type,MemberFunName) \
::boost::multi_index::const_mem_fun< Class,Type,&Class::MemberFunName >
#define BOOST_MULTI_INDEX_MEM_FUN(Class,Type,MemberFunName) \
::boost::multi_index::mem_fun< Class,Type,&Class::MemberFunName >

} /* namespace multi_index */

} /* namespace boost */

#endif
