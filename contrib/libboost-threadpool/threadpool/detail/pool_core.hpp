/*! \file
* \brief Thread pool core.
*
* This file contains the threadpool's core class: pool<Task, SchedulingPolicy>.
*
* Thread pools are a mechanism for asynchronous and parallel processing 
* within the same process. The pool class provides a convenient way 
* for dispatching asynchronous tasks as functions objects. The scheduling
* of these tasks can be easily controlled by using customized schedulers. 
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


#ifndef THREADPOOL_POOL_CORE_HPP_INCLUDED
#define THREADPOOL_POOL_CORE_HPP_INCLUDED




#include "locking_ptr.hpp"
#include "worker_thread.hpp"

#include "../task_adaptors.hpp"

#include <boost/thread.hpp>
#include <boost/thread/exceptions.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/static_assert.hpp>
#include <boost/type_traits.hpp>

#include <vector>


/// The namespace threadpool contains a thread pool and related utility classes.
namespace boost { namespace threadpool { namespace detail 
{

  /*! \brief Thread pool. 
  *
  * Thread pools are a mechanism for asynchronous and parallel processing 
  * within the same process. The pool class provides a convenient way 
  * for dispatching asynchronous tasks as functions objects. The scheduling
  * of these tasks can be easily controlled by using customized schedulers. 
  * A task must not throw an exception.
  *
  * A pool_impl is DefaultConstructible and NonCopyable.
  *
  * \param Task A function object which implements the operator 'void operator() (void) const'. The operator () is called by the pool to execute the task. Exceptions are ignored.
  * \param Scheduler A task container which determines how tasks are scheduled. It is guaranteed that this container is accessed only by one thread at a time. The scheduler shall not throw exceptions.
  *
  * \remarks The pool class is thread-safe.
  * 
  * \see Tasks: task_func, prio_task_func
  * \see Scheduling policies: fifo_scheduler, lifo_scheduler, prio_scheduler
  */ 
  template <
    typename Task, 

    template <typename> class SchedulingPolicy,
    template <typename> class SizePolicy,
    template <typename> class SizePolicyController,
    template <typename> class ShutdownPolicy
  > 
  class pool_core
  : public enable_shared_from_this< pool_core<Task, SchedulingPolicy, SizePolicy, SizePolicyController, ShutdownPolicy > > 
  , private noncopyable
  {

  public: // Type definitions
    typedef Task task_type;                                 //!< Indicates the task's type.
    typedef SchedulingPolicy<task_type> scheduler_type;     //!< Indicates the scheduler's type.
    typedef pool_core<Task, 
                      SchedulingPolicy, 
                      SizePolicy,
                      SizePolicyController,
                      ShutdownPolicy > pool_type;           //!< Indicates the thread pool's type.
    typedef SizePolicy<pool_type> size_policy_type;         //!< Indicates the sizer's type.
    //typedef typename size_policy_type::size_controller size_controller_type;

    typedef SizePolicyController<pool_type> size_controller_type;

//    typedef SizePolicy<pool_type>::size_controller size_controller_type;
    typedef ShutdownPolicy<pool_type> shutdown_policy_type;//!< Indicates the shutdown policy's type.  

    typedef worker_thread<pool_type> worker_type;

    // The task is required to be a nullary function.
    BOOST_STATIC_ASSERT(function_traits<task_type()>::arity == 0);

    // The task function's result type is required to be void.
    BOOST_STATIC_ASSERT(is_void<typename result_of<task_type()>::type >::value);


  private:  // Friends 
    friend class worker_thread<pool_type>;

#if defined(__SUNPRO_CC) && (__SUNPRO_CC <= 0x580)  // Tested with CC: Sun C++ 5.8 Patch 121018-08 2006/12/06
   friend class SizePolicy;
   friend class ShutdownPolicy;
#else
   friend class SizePolicy<pool_type>;
   friend class ShutdownPolicy<pool_type>;
#endif

  private: // The following members may be accessed by _multiple_ threads at the same time:
    volatile size_t m_worker_count;	
    volatile size_t m_target_worker_count;	
    volatile size_t m_active_worker_count;
      


  private: // The following members are accessed only by _one_ thread at the same time:
    scheduler_type  m_scheduler;
    scoped_ptr<size_policy_type> m_size_policy; // is never null
    
    bool  m_terminate_all_workers;								// Indicates if termination of all workers was triggered.
    std::vector<shared_ptr<worker_type> > m_terminated_workers; // List of workers which are terminated but not fully destructed.
    
  private: // The following members are implemented thread-safe:
    mutable recursive_mutex  m_monitor;
    mutable condition m_worker_idle_or_terminated_event;	// A worker is idle or was terminated.
    mutable condition m_task_or_terminate_workers_event;  // Task is available OR total worker count should be reduced.

  public:
    /// Constructor.
    pool_core()
      : m_worker_count(0) 
      , m_target_worker_count(0)
      , m_active_worker_count(0)
      , m_terminate_all_workers(false)
    {
      pool_type volatile & self_ref = *this;
      m_size_policy.reset(new size_policy_type(self_ref));

      m_scheduler.clear();
    }


    /// Destructor.
    ~pool_core()
    {
    }

    /*! Gets the size controller which manages the number of threads in the pool. 
    * \return The size controller.
    * \see SizePolicy
    */
    size_controller_type size_controller()
    {
      return size_controller_type(*m_size_policy, this->shared_from_this());
    }

    /*! Gets the number of threads in the pool.
    * \return The number of threads.
    */
    size_t size()	const volatile
    {
      return m_worker_count;
    }

// TODO is only called once
    void shutdown()
    {
      ShutdownPolicy<pool_type>::shutdown(*this);
    }

    /*! Schedules a task for asynchronous execution. The task will be executed once only.
    * \param task The task function object. It should not throw execeptions.
    * \return true, if the task could be scheduled and false otherwise. 
    */  
    bool schedule(task_type const & task) volatile
    {	
      locking_ptr<pool_type, recursive_mutex> lockedThis(*this, m_monitor); 
      
      if(lockedThis->m_scheduler.push(task))
      {
        lockedThis->m_task_or_terminate_workers_event.notify_one();
        return true;
      }
      else
      {
        return false;
      }
    }	


    /*! Returns the number of tasks which are currently executed.
    * \return The number of active tasks. 
    */  
    size_t active() const volatile
    {
      return m_active_worker_count;
    }


    /*! Returns the number of tasks which are ready for execution.    
    * \return The number of pending tasks. 
    */  
    size_t pending() const volatile
    {
      locking_ptr<const pool_type, recursive_mutex> lockedThis(*this, m_monitor);
      return lockedThis->m_scheduler.size();
    }


    /*! Removes all pending tasks from the pool's scheduler.
    */  
    void clear() volatile
    { 
      locking_ptr<pool_type, recursive_mutex> lockedThis(*this, m_monitor);
      lockedThis->m_scheduler.clear();
    }    


    /*! Indicates that there are no tasks pending. 
    * \return true if there are no tasks ready for execution.	
    * \remarks This function is more efficient that the check 'pending() == 0'.
    */   
    bool empty() const volatile
    {
      locking_ptr<const pool_type, recursive_mutex> lockedThis(*this, m_monitor);
      return lockedThis->m_scheduler.empty();
    }	


    /*! The current thread of execution is blocked until the sum of all active
    *  and pending tasks is equal or less than a given threshold. 
    * \param task_threshold The maximum number of tasks in pool and scheduler.
    */     
    void wait(size_t const task_threshold = 0) const volatile
    {
      const pool_type* self = const_cast<const pool_type*>(this);
      recursive_mutex::scoped_lock lock(self->m_monitor);

      if(0 == task_threshold)
      {
        while(0 != self->m_active_worker_count || !self->m_scheduler.empty())
        { 
          self->m_worker_idle_or_terminated_event.wait(lock);
        }
      }
      else
      {
        while(task_threshold < self->m_active_worker_count + self->m_scheduler.size())
        { 
          self->m_worker_idle_or_terminated_event.wait(lock);
        }
      }
    }	

    /*! The current thread of execution is blocked until the timestamp is met
    * or the sum of all active and pending tasks is equal or less 
    * than a given threshold.  
    * \param timestamp The time when function returns at the latest.
    * \param task_threshold The maximum number of tasks in pool and scheduler.
    * \return true if the task sum is equal or less than the threshold, false otherwise.
    */       
    bool wait(xtime const & timestamp, size_t const task_threshold = 0) const volatile
    {
      const pool_type* self = const_cast<const pool_type*>(this);
      recursive_mutex::scoped_lock lock(self->m_monitor);

      if(0 == task_threshold)
      {
        while(0 != self->m_active_worker_count || !self->m_scheduler.empty())
        { 
          if(!self->m_worker_idle_or_terminated_event.timed_wait(lock, timestamp)) return false;
        }
      }
      else
      {
        while(task_threshold < self->m_active_worker_count + self->m_scheduler.size())
        { 
          if(!self->m_worker_idle_or_terminated_event.timed_wait(lock, timestamp)) return false;
        }
      }

      return true;
    }


  private:	


    void terminate_all_workers(bool const wait) volatile
    {
      pool_type* self = const_cast<pool_type*>(this);
      recursive_mutex::scoped_lock lock(self->m_monitor);

      self->m_terminate_all_workers = true;

      m_target_worker_count = 0;
      self->m_task_or_terminate_workers_event.notify_all();

      if(wait)
      {
        while(m_worker_count > 0)
        {
          self->m_worker_idle_or_terminated_event.wait(lock);
        }

        for(typename std::vector<shared_ptr<worker_type> >::iterator it = self->m_terminated_workers.begin();
          it != self->m_terminated_workers.end();
          ++it)
        {
          (*it)->join();
        }
        self->m_terminated_workers.clear();
      }
    }


    /*! Changes the number of worker threads in the pool. The resizing 
    *  is handled by the SizePolicy.
    * \param threads The new number of worker threads.
    * \return true, if pool will be resized and false if not. 
    */
    bool resize(size_t const worker_count) volatile
    {
      locking_ptr<pool_type, recursive_mutex> lockedThis(*this, m_monitor); 

      if(!m_terminate_all_workers)
      {
        m_target_worker_count = worker_count;
      }
      else
      { 
        return false;
      }


      if(m_worker_count <= m_target_worker_count)
      { // increase worker count
        while(m_worker_count < m_target_worker_count)
        {
          try
          {
            worker_thread<pool_type>::create_and_attach(lockedThis->shared_from_this());
            m_worker_count++;
            m_active_worker_count++;	
          }
          catch(thread_resource_error)
          {
            return false;
          }
        }
      }
      else
      { // decrease worker count
        lockedThis->m_task_or_terminate_workers_event.notify_all();   // TODO: Optimize number of notified workers
      }

      return true;
    }


    // worker died with unhandled exception
    void worker_died_unexpectedly(shared_ptr<worker_type> worker) volatile
    {
      locking_ptr<pool_type, recursive_mutex> lockedThis(*this, m_monitor);

      m_worker_count--;
      m_active_worker_count--;
      lockedThis->m_worker_idle_or_terminated_event.notify_all();	

      if(m_terminate_all_workers)
      {
        lockedThis->m_terminated_workers.push_back(worker);
      }
      else
      {
        lockedThis->m_size_policy->worker_died_unexpectedly(m_worker_count);
      }
    }

    void worker_destructed(shared_ptr<worker_type> worker) volatile
    {
      locking_ptr<pool_type, recursive_mutex> lockedThis(*this, m_monitor);
      m_worker_count--;
      m_active_worker_count--;
      lockedThis->m_worker_idle_or_terminated_event.notify_all();	

      if(m_terminate_all_workers)
      {
        lockedThis->m_terminated_workers.push_back(worker);
      }
    }


    bool execute_task() volatile
    {
      function0<void> task;

      { // fetch task
        pool_type* lockedThis = const_cast<pool_type*>(this);
        recursive_mutex::scoped_lock lock(lockedThis->m_monitor);

        // decrease number of threads if necessary
        if(m_worker_count > m_target_worker_count)
        {	
          return false;	// terminate worker
        }


        // wait for tasks
        while(lockedThis->m_scheduler.empty())
        {	
          // decrease number of workers if necessary
          if(m_worker_count > m_target_worker_count)
          {	
            return false;	// terminate worker
          }
          else
          {
            m_active_worker_count--;
            lockedThis->m_worker_idle_or_terminated_event.notify_all();	
            lockedThis->m_task_or_terminate_workers_event.wait(lock);
            m_active_worker_count++;
          }
        }

        task = lockedThis->m_scheduler.top();
        lockedThis->m_scheduler.pop();
      }

      // call task function
      if(task)
      {
        task();
      }
 
      //guard->disable();
      return true;
    }
  };




} } } // namespace boost::threadpool::detail

#endif // THREADPOOL_POOL_CORE_HPP_INCLUDED
