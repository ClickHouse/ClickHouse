/*! \file
* \brief TODO.
*
* TODO. 
*
* Copyright (c) 2005-2007 Philipp Henkel
*
* Use, modification, and distribution are  subject to the
* Boost Software License, Version 1.0. (See accompanying  file
* LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
*
* http://threadpool.sourceforge.net
*
*/


#ifndef THREADPOOL_DETAIL_FUTURE_IMPL_HPP_INCLUDED
#define THREADPOOL_DETAIL_FUTURE_IMPL_HPP_INCLUDED


#include "locking_ptr.hpp"

#include <boost/smart_ptr.hpp>
#include <boost/optional.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/utility/result_of.hpp>
#include <boost/static_assert.hpp>
#include <boost/type_traits.hpp>

namespace boost { namespace threadpool { namespace detail 
{

template<class Result> 
class future_impl
{
public:
  typedef Result const & result_type; //!< Indicates the functor's result type.

  typedef Result future_result_type; //!< Indicates the future's result type.
  typedef future_impl<future_result_type> future_type;

private:
    volatile bool m_ready;
    volatile future_result_type m_result;

    mutable mutex m_monitor;
    mutable condition m_condition_ready;	

    volatile bool m_is_cancelled;
    volatile bool m_executing;

public:


public:

  future_impl()
  : m_ready(false)
  , m_is_cancelled(false)
  {
  }

  bool ready() const volatile
  {
    return m_ready; 
  }

  void wait() const volatile
  {
    const future_type* self = const_cast<const future_type*>(this);
    mutex::scoped_lock lock(self->m_monitor);

    while(!m_ready)
    {
      self->m_condition_ready.wait(lock);
    }
  }


  bool timed_wait(boost::xtime const & timestamp) const
  {
    const future_type* self = const_cast<const future_type*>(this);
    mutex::scoped_lock lock(self->m_monitor);

    while(!m_ready)
    {
      if(!self->m_condition_ready.timed_wait(lock, timestamp)) return false;
    }

    return true;
  }


  result_type operator()() const volatile
  {
    wait();
/*
    if( throw_exception_ != 0 )
    {
      throw_exception_( this );
    }
*/
 
    return *(const_cast<const future_result_type*>(&m_result));
  }


  void set_value(future_result_type const & r) volatile
  {
    locking_ptr<future_type, mutex> lockedThis(*this, m_monitor);
    if(!m_ready && !m_is_cancelled)
    {
      lockedThis->m_result = r;
      lockedThis->m_ready = true;
      lockedThis->m_condition_ready.notify_all();
    }
  }
/*
  template<class E> void set_exception() // throw()
  {
    m_impl->template set_exception<E>();
  }

  template<class E> void set_exception( char const * what ) // throw()
  {
    m_impl->template set_exception<E>( what );
  }
  */


   bool cancel() volatile
   {
     if(!m_ready || m_executing)
     {
        m_is_cancelled = true;
        return true;
     }
     else
     {
       return false;
     }
   }


   bool is_cancelled() const volatile
   {
     return m_is_cancelled;
   }


   void set_execution_status(bool executing) volatile
   {
     m_executing = executing;
   }
};


template<
  template <typename> class Future,
  typename Function
>
class future_impl_task_func
{

public:
  typedef void result_type;                         //!< Indicates the functor's result type.

  typedef Function function_type;                   //!< Indicates the function's type.
  typedef typename result_of<function_type()>::type future_result_type; //!< Indicates the future's result type.
  typedef Future<future_result_type> future_type;   //!< Indicates the future's type.

  // The task is required to be a nullary function.
  BOOST_STATIC_ASSERT(function_traits<function_type()>::arity == 0);

  // The task function's result type is required not to be void.
  BOOST_STATIC_ASSERT(!is_void<future_result_type>::value);

private:
  function_type             m_function;
  shared_ptr<future_type>   m_future;

public:
  future_impl_task_func(function_type const & function, shared_ptr<future_type> const & future)
  : m_function(function)
  , m_future(future)
  {
  }

  void operator()()
  {
    if(m_function)
    {
      m_future->set_execution_status(true);
      if(!m_future->is_cancelled())
      {
        // TODO future exeception handling 
        m_future->set_value(m_function());
      }
      m_future->set_execution_status(false); // TODO consider exceptions
    }
  }

};





} } } // namespace boost::threadpool::detail

#endif // THREADPOOL_DETAIL_FUTURE_IMPL_HPP_INCLUDED


