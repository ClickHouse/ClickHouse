/*! \file
* \brief Pool adaptors.
*
* This file contains an easy-to-use adaptor similar to a smart 
* pointer for the pool class. 
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


#ifndef THREADPOOL_POOL_ADAPTORS_HPP_INCLUDED
#define THREADPOOL_POOL_ADAPTORS_HPP_INCLUDED

#include <boost/smart_ptr.hpp>


namespace boost { namespace threadpool
{


// TODO convenience scheduling function
    /*! Schedules a Runnable for asynchronous execution. A Runnable is an arbitrary class with a run()
    * member function. This a convenience shorthand for pool->schedule(bind(&Runnable::run, task_object)).
    * \param 
    * \param obj The Runnable object. The member function run() will be exectued and should not throw execeptions.
    * \return true, if the task could be scheduled and false otherwise. 
    */  
    template<typename Pool, typename Runnable>
    bool schedule(Pool& pool, shared_ptr<Runnable> const & obj)
    {	
      return pool->schedule(bind(&Runnable::run, obj));
    }	
    
    /*! Schedules a task for asynchronous execution. The task will be executed once only.
    * \param task The task function object.
    */  
    template<typename Pool>
    typename enable_if < 
      is_void< typename result_of< typename Pool::task_type() >::type >,
      bool
    >::type
    schedule(Pool& pool, typename Pool::task_type const & task)
    {	
      return pool.schedule(task);
    }	


    template<typename Pool>
    typename enable_if < 
      is_void< typename result_of< typename Pool::task_type() >::type >,
      bool
    >::type
    schedule(shared_ptr<Pool> const pool, typename Pool::task_type const & task)
    {	
      return pool->schedule(task);
    }	


} } // namespace boost::threadpool

#endif // THREADPOOL_POOL_ADAPTORS_HPP_INCLUDED


