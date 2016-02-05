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


#ifndef THREADPOOL_POOL_HPP_INCLUDED
#define THREADPOOL_POOL_HPP_INCLUDED

#include <boost/ref.hpp>

#include "detail/pool_core.hpp"

#include "task_adaptors.hpp"

#include "detail/locking_ptr.hpp"

#include "scheduling_policies.hpp"
#include "size_policies.hpp"
#include "shutdown_policies.hpp"



/// The namespace threadpool contains a thread pool and related utility classes.
namespace boost { namespace threadpool
{



  /*! \brief Thread pool. 
  *
  * Thread pools are a mechanism for asynchronous and parallel processing 
  * within the same process. The pool class provides a convenient way 
  * for dispatching asynchronous tasks as functions objects. The scheduling
  * of these tasks can be easily controlled by using customized schedulers. 
  * A task must not throw an exception.
  *
  * A pool is DefaultConstructible, CopyConstructible and Assignable.
  * It has reference semantics; all copies of the same pool are equivalent and interchangeable. 
  * All operations on a pool except assignment are strongly thread safe or sequentially consistent; 
  * that is, the behavior of concurrent calls is as if the calls have been issued sequentially in an unspecified order.
  *
  * \param Task A function object which implements the operator 'void operator() (void) const'. The operator () is called by the pool to execute the task. Exceptions are ignored.
  * \param SchedulingPolicy A task container which determines how tasks are scheduled. It is guaranteed that this container is accessed only by one thread at a time. The scheduler shall not throw exceptions.
  *
  * \remarks The pool class is thread-safe.
  * 
  * \see Tasks: task_func, prio_task_func
  * \see Scheduling policies: fifo_scheduler, lifo_scheduler, prio_scheduler
  */ 
  template <
    typename Task                                   = task_func,
    template <typename> class SchedulingPolicy      = fifo_scheduler,
    template <typename> class SizePolicy            = static_size,
    template <typename> class SizePolicyController  = resize_controller,
    template <typename> class ShutdownPolicy        = wait_for_all_tasks
  > 
  class thread_pool 
  {
    typedef detail::pool_core<Task, 
                              SchedulingPolicy,
                              SizePolicy,
                              SizePolicyController,
                              ShutdownPolicy> pool_core_type;
    shared_ptr<pool_core_type>          m_core; // pimpl idiom
    shared_ptr<void>                    m_shutdown_controller; // If the last pool holding a pointer to the core is deleted the controller shuts the pool down.

  public: // Type definitions
    typedef Task task_type;                                   //!< Indicates the task's type.
    typedef SchedulingPolicy<task_type> scheduler_type;       //!< Indicates the scheduler's type.
 /*   typedef thread_pool<Task, 
                        SchedulingPolicy,
                        SizePolicy,
                        ShutdownPolicy > pool_type;          //!< Indicates the thread pool's type.
 */
    typedef SizePolicy<pool_core_type> size_policy_type; 
    typedef SizePolicyController<pool_core_type> size_controller_type;


  public:
    /*! Constructor.
     * \param initial_threads The pool is immediately resized to set the specified number of threads. The pool's actual number threads depends on the SizePolicy.
     */
    thread_pool(size_t initial_threads = 0)
    : m_core(new pool_core_type)
    , m_shutdown_controller(static_cast<void*>(0), bind(&pool_core_type::shutdown, m_core))
    {
      size_policy_type::init(*m_core, initial_threads);
    }


    /*! Gets the size controller which manages the number of threads in the pool. 
    * \return The size controller.
    * \see SizePolicy
    */
    size_controller_type size_controller()
    {
      return m_core->size_controller();
    }


    /*! Gets the number of threads in the pool.
    * \return The number of threads.
    */
    size_t size()	const
    {
      return m_core->size();
    }


     /*! Schedules a task for asynchronous execution. The task will be executed once only.
     * \param task The task function object. It should not throw execeptions.
     * \return true, if the task could be scheduled and false otherwise. 
     */  
     bool schedule(task_type const & task)
     {	
       return m_core->schedule(task);
     }


    /*! Returns the number of tasks which are currently executed.
    * \return The number of active tasks. 
    */  
    size_t active() const
    {
      return m_core->active();
    }


    /*! Returns the number of tasks which are ready for execution.    
    * \return The number of pending tasks. 
    */  
    size_t pending() const
    {
      return m_core->pending();
    }


    /*! Removes all pending tasks from the pool's scheduler.
    */  
    void clear()
    { 
      m_core->clear();
    }    


    /*! Indicates that there are no tasks pending. 
    * \return true if there are no tasks ready for execution.	
    * \remarks This function is more efficient that the check 'pending() == 0'.
    */   
    bool empty() const
    {
      return m_core->empty();
    }	


    /*! The current thread of execution is blocked until the sum of all active
    *  and pending tasks is equal or less than a given threshold. 
    * \param task_threshold The maximum number of tasks in pool and scheduler.
    */     
    void wait(size_t task_threshold = 0) const
    {
      m_core->wait(task_threshold);
    }	


    /*! The current thread of execution is blocked until the timestamp is met
    * or the sum of all active and pending tasks is equal or less 
    * than a given threshold.  
    * \param timestamp The time when function returns at the latest.
    * \param task_threshold The maximum number of tasks in pool and scheduler.
    * \return true if the task sum is equal or less than the threshold, false otherwise.
    */       
    bool wait(xtime const & timestamp, size_t task_threshold = 0) const
    {
      return m_core->wait(timestamp, task_threshold);
    }
  };



  /*! \brief Fifo pool.
  *
  * The pool's tasks are fifo scheduled task_func functors.
  *
  */ 
  typedef thread_pool<task_func, fifo_scheduler, static_size, resize_controller, wait_for_all_tasks> fifo_pool;


  /*! \brief Lifo pool.
  *
  * The pool's tasks are lifo scheduled task_func functors.
  *
  */ 
  typedef thread_pool<task_func, lifo_scheduler, static_size, resize_controller, wait_for_all_tasks> lifo_pool;


  /*! \brief Pool for prioritized task.
  *
  * The pool's tasks are prioritized prio_task_func functors.
  *
  */ 
  typedef thread_pool<prio_task_func, prio_scheduler, static_size, resize_controller, wait_for_all_tasks> prio_pool;


  /*! \brief A standard pool.
  *
  * The pool's tasks are fifo scheduled task_func functors.
  *
  */ 
  typedef fifo_pool pool;



} } // namespace boost::threadpool

#endif // THREADPOOL_POOL_HPP_INCLUDED
