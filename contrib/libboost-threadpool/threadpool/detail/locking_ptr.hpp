/*! \file
* \brief The locking_ptr is smart pointer with a scoped locking mechanism.
*
* The class is a wrapper for a volatile pointer. It enables synchronized access to the
* internal pointer by locking the passed mutex.
* locking_ptr is based on Andrei Alexandrescu's LockingPtr. For more information
* see article "volatile - Multithreaded Programmer's Best Friend" by A. Alexandrescu.
*
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


#ifndef THREADPOOL_DETAIL_LOCKING_PTR_HPP_INCLUDED
#define THREADPOOL_DETAIL_LOCKING_PTR_HPP_INCLUDED

#include <boost/utility.hpp>

// Support for old boost::thread
//**********************************************
#include <boost/thread/mutex.hpp>
#ifndef BOOST_THREAD_MUTEX_HPP
#include <boost/thread/detail/lock.hpp>
#endif
//**********************************************

namespace boost { namespace threadpool { namespace detail 
{

/*! \brief  Smart pointer with a scoped locking mechanism.
 *
 * This class is a wrapper for a volatile pointer. It enables synchronized access to the
 * internal pointer by locking the passed mutex.
 */
  template <typename T, typename Mutex>
  class locking_ptr 
  : private noncopyable
  {
    T* m_obj;                     //!< The instance pointer. 
    Mutex & m_mutex;              //!< Mutex is used for scoped locking.

  public:
    /// Constructor.
    locking_ptr(volatile T& obj, const volatile Mutex& mtx)
      : m_obj(const_cast<T*>(&obj))
      , m_mutex(*const_cast<Mutex*>(&mtx))
    {   
      // Lock mutex

#ifndef BOOST_THREAD_MUTEX_HPP
      // Support for old boost::thread
      boost::detail::thread::lock_ops<Mutex>::lock(m_mutex);   
#else 
      m_mutex.lock();
#endif
    }


    /// Destructor.
    ~locking_ptr()
    { 
      // Unlock mutex
#ifndef BOOST_THREAD_MUTEX_HPP
      // Support for old boost::thread
      boost::detail::thread::lock_ops<Mutex>::unlock(m_mutex);   
#else 
      m_mutex.unlock();
#endif
    }


    /*! Returns a reference to the stored instance.
    * \return The instance's reference.
    */
    T& operator*() const
    {    
      return *m_obj;    
    }


    /*! Returns a pointer to the stored instance.
    * \return The instance's pointer.
    */
    T* operator->() const
    {   
      return m_obj;   
    }
  };


} } } // namespace boost::threadpool::detail


#endif // THREADPOOL_DETAIL_LOCKING_PTR_HPP_INCLUDED

