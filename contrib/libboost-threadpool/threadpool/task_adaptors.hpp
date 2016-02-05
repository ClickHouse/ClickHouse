/*! \file
* \brief Task adaptors.
*
* This file contains adaptors for task function objects.
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


#ifndef THREADPOOL_TASK_ADAPTERS_HPP_INCLUDED
#define THREADPOOL_TASK_ADAPTERS_HPP_INCLUDED

#include <boost/version.hpp>

#if BOOST_VERSION >= 105000
	#ifndef TIME_UTC
		#define TIME_UTC TIME_UTC_
	#endif
#endif


#include <boost/smart_ptr.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>


namespace boost { namespace threadpool
{

  /*! \brief Standard task function object.
  *
  * This function object wraps a nullary function which returns void.
  * The wrapped function is invoked by calling the operator ().
  *
  * \see boost function library
  *
  */ 
  typedef function0<void> task_func;




  /*! \brief Prioritized task function object. 
  *
  * This function object wraps a task_func object and binds a priority to it.
  * prio_task_funcs can be compared using the operator < which realises a partial ordering.
  * The wrapped task function is invoked by calling the operator ().
  *
  * \see prio_scheduler
  *
  */ 
  class prio_task_func
  {
  private:
    unsigned int m_priority;  //!< The priority of the task's function.
    task_func m_function;     //!< The task's function.

  public:
    typedef void result_type; //!< Indicates the functor's result type.

  public:
    /*! Constructor.
    * \param priority The priority of the task.
    * \param function The task's function object.
    */
    prio_task_func(unsigned int const priority, task_func const & function)
      : m_priority(priority)
      , m_function(function)
    {
    }

    /*! Executes the task function.
    */
    void operator() (void) const
    {
      if(m_function)
      {
        m_function();
      }
    }

    /*! Comparison operator which realises a partial ordering based on priorities.
    * \param rhs The object to compare with.
    * \return true if the priority of *this is less than right hand side's priority, false otherwise.
    */
    bool operator< (const prio_task_func& rhs) const
    {
      return m_priority < rhs.m_priority; 
    }

  };  // prio_task_func



 




  /*! \brief Looped task function object. 
  *
  * This function object wraps a boolean thread function object.
  * The wrapped task function is invoked by calling the operator () and it is executed in regular 
  * time intervals until false is returned. The interval length may be zero.
  * Please note that a pool's thread is engaged as long as the task is looped.
  *
  */ 
  class looped_task_func
  {
  private:
    function0<bool> m_function;   //!< The task's function.
    unsigned int m_break_s;              //!< Duration of breaks in seconds.
    unsigned int m_break_ns;             //!< Duration of breaks in nano seconds.

  public:
    typedef void result_type; //!< Indicates the functor's result type.

  public:
    /*! Constructor.
    * \param function The task's function object which is looped until false is returned.
    * \param interval The minimum break time in milli seconds before the first execution of the task function and between the following ones.
    */
    looped_task_func(function0<bool> const & function, unsigned int const interval = 0)
      : m_function(function)
    {
      m_break_s  = interval / 1000;
      m_break_ns = (interval - m_break_s * 1000) * 1000 * 1000;
    }

    /*! Executes the task function.
    */
    void operator() (void) const
    {
      if(m_function)
      {
        if(m_break_s > 0 || m_break_ns > 0)
        { // Sleep some time before first execution
          xtime xt;
          xtime_get(&xt, TIME_UTC);
          xt.nsec += m_break_ns;
          xt.sec += m_break_s;
          thread::sleep(xt); 
        }

        while(m_function())
        {
          if(m_break_s > 0 || m_break_ns > 0)
          {
            xtime xt;
            xtime_get(&xt, TIME_UTC);
            xt.nsec += m_break_ns;
            xt.sec += m_break_s;
            thread::sleep(xt); 
          }
          else
          {
            thread::yield(); // Be fair to other threads
          }
        }
      }
    }

  }; // looped_task_func


} } // namespace boost::threadpool

#endif // THREADPOOL_TASK_ADAPTERS_HPP_INCLUDED

