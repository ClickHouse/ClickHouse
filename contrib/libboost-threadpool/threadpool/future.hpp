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

#ifndef THREADPOOL_FUTURE_HPP_INCLUDED
#define THREADPOOL_FUTURE_HPP_INCLUDED


  
#include "detail/future.hpp"
#include <boost/utility/enable_if.hpp>

//#include "pool.hpp"
//#include <boost/utility.hpp>

//#include <boost/thread/mutex.hpp>


namespace boost { namespace threadpool
{

  /*! \brief Experimental. Do not use in production code. TODO. 
  *
  * TODO Future
  *
  * \see TODO
  *
  */ 


template<class Result> 
class future
{
private:
  shared_ptr<detail::future_impl<Result> > m_impl;

public:
    typedef Result const & result_type; //!< Indicates the functor's result type.
    typedef Result future_result_type; //!< Indicates the future's result type.


public:

  future()
  : m_impl(new detail::future_impl<future_result_type>()) // TODO remove this
  {
  }

  // only for internal usage
  future(shared_ptr<detail::future_impl<Result> > const & impl)
  : m_impl(impl)
  {
  }

  bool ready() const
  {
    return m_impl->ready();
  }

  void wait() const
  {
    m_impl->wait();
  }

  bool timed_wait(boost::xtime const & timestamp) const
  {
    return m_impl->timed_wait(timestamp);
  }

   result_type operator()() // throw( thread::cancelation_exception, ... )
   {
     return (*m_impl)();
   }

   result_type get() // throw( thread::cancelation_exception, ... )
   {
     return (*m_impl)();
   }

   bool cancel()
   {
     return m_impl->cancel();
   }

   bool is_cancelled() const
   {
     return m_impl->is_cancelled();
   }
};





template<class Pool, class Function>
typename disable_if < 
  is_void< typename result_of< Function() >::type >,
  future< typename result_of< Function() >::type >
>::type
schedule(Pool& pool, const Function& task)
{
  typedef typename result_of< Function() >::type future_result_type;

  // create future impl and future
  shared_ptr<detail::future_impl<future_result_type> > impl(new detail::future_impl<future_result_type>);
  future <future_result_type> res(impl);

  // schedule future impl
  pool.schedule(detail::future_impl_task_func<detail::future_impl, Function>(task, impl));

  // return future
  return res;

/*
 TODO
  if(pool->schedule(bind(&Future::run, future)))
  {
    return future;
  }
  else
  {
    // construct empty future
    return error_future;
  }
  */
}



} } // namespace boost::threadpool

#endif // THREADPOOL_FUTURE_HPP_INCLUDED

