/*! \file
* \brief Size policies.
*
* This file contains size policies for thread_pool. A size 
* policy controls the number of worker threads in the pool.
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


#ifndef THREADPOOL_SIZE_POLICIES_HPP_INCLUDED
#define THREADPOOL_SIZE_POLICIES_HPP_INCLUDED



/// The namespace threadpool contains a thread pool and related utility classes.
namespace boost { namespace threadpool
{

  /*! \brief SizePolicyController which provides no functionality.
  *
  * \param Pool The pool's core type.
  */ 
  template<typename Pool>
  struct empty_controller
  {
    empty_controller(typename Pool::size_policy_type&, shared_ptr<Pool>) {}
  };


  /*! \brief SizePolicyController which allows resizing.
  *
  * \param Pool The pool's core type.
  */ 
  template< typename Pool >
  class resize_controller
  {
    typedef typename Pool::size_policy_type size_policy_type;
    reference_wrapper<size_policy_type> m_policy;
    shared_ptr<Pool> m_pool;                           //!< to make sure that the pool is alive (the policy pointer is valid) as long as the controller exists

  public:
    resize_controller(size_policy_type& policy, shared_ptr<Pool> pool)
      : m_policy(policy)
      , m_pool(pool)
    {
    }

    bool resize(size_t worker_count)
    {
      return m_policy.get().resize(worker_count);
    }
  };


  /*! \brief SizePolicy which preserves the thread count.
  *
  * \param Pool The pool's core type.
  */ 
  template<typename Pool>
  class static_size
  {
    reference_wrapper<Pool volatile> m_pool;

  public:
    static void init(Pool& pool, size_t const worker_count)
    {
      pool.resize(worker_count);
    }

    static_size(Pool volatile & pool)
      : m_pool(pool)
    {}

    bool resize(size_t const worker_count)
    {
      return m_pool.get().resize(worker_count);
    }

    void worker_died_unexpectedly(size_t const new_worker_count)
    {
      m_pool.get().resize(new_worker_count + 1);
    }

    // TODO this functions are not called yet
    void task_scheduled() {}
    void task_finished() {}
  };

} } // namespace boost::threadpool

#endif // THREADPOOL_SIZE_POLICIES_HPP_INCLUDED
