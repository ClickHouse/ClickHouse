// Copyright (C) 2001-2003
// William E. Kempf
// Copyright (C) 2007-8 Anthony Williams
// (C) Copyright 2011-2012 Vicente J. Botet Escriba
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <boost/thread/detail/config.hpp>

#include <boost/thread/thread_only.hpp>
#if defined BOOST_THREAD_USES_DATETIME
#include <boost/thread/xtime.hpp>
#endif
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/once.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread/future.hpp>

#ifdef __GLIBC__
#include <sys/sysinfo.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/sysctl.h>
#elif defined BOOST_HAS_UNISTD_H
#include <unistd.h>
#endif

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>

#include <fstream>
#include <string>
#include <set>
#include <vector>
#include <string.h> // memcmp.

namespace boost
{
    namespace detail
    {
        thread_data_base::~thread_data_base()
        {
            for (notify_list_t::iterator i = notify.begin(), e = notify.end();
                    i != e; ++i)
            {
                i->second->unlock();
                i->first->notify_all();
            }
            for (async_states_t::iterator i = async_states_.begin(), e = async_states_.end();
                    i != e; ++i)
            {
                (*i)->make_ready();
            }
        }

        struct thread_exit_callback_node
        {
            boost::detail::thread_exit_function_base* func;
            thread_exit_callback_node* next;

            thread_exit_callback_node(boost::detail::thread_exit_function_base* func_,
                                      thread_exit_callback_node* next_):
                func(func_),next(next_)
            {}
        };

        namespace
        {
#ifdef BOOST_THREAD_PROVIDES_ONCE_CXX11
          boost::once_flag current_thread_tls_init_flag;
#else
            boost::once_flag current_thread_tls_init_flag=BOOST_ONCE_INIT;
#endif
            pthread_key_t current_thread_tls_key;

            extern "C"
            {
                static void tls_destructor(void* data)
                {
                    //boost::detail::thread_data_base* thread_info=static_cast<boost::detail::thread_data_base*>(data);
                    boost::detail::thread_data_ptr thread_info = static_cast<boost::detail::thread_data_base*>(data)->shared_from_this();

                    if(thread_info)
                    {
                        while(!thread_info->tss_data.empty() || thread_info->thread_exit_callbacks)
                        {

                            while(thread_info->thread_exit_callbacks)
                            {
                                detail::thread_exit_callback_node* const current_node=thread_info->thread_exit_callbacks;
                                thread_info->thread_exit_callbacks=current_node->next;
                                if(current_node->func)
                                {
                                    (*current_node->func)();
                                    delete current_node->func;
                                }
                                delete current_node;
                            }
                            while (!thread_info->tss_data.empty())
                            {
                                std::map<void const*,detail::tss_data_node>::iterator current
                                    = thread_info->tss_data.begin();
                                if(current->second.func && (current->second.value!=0))
                                {
                                    (*current->second.func)(current->second.value);
                                }
                                thread_info->tss_data.erase(current);
                            }
                        }
                        thread_info->self.reset();
                    }
                }
            }

#if defined BOOST_THREAD_PATCH
            struct  delete_current_thread_tls_key_on_dlclose_t
            {
                delete_current_thread_tls_key_on_dlclose_t()
                {
                }
                ~delete_current_thread_tls_key_on_dlclose_t()
                {
                    const boost::once_flag uninitialized = BOOST_ONCE_INIT;
                    if (memcmp(&current_thread_tls_init_flag, &uninitialized, sizeof(boost::once_flag)))
                    {
                        void* data = pthread_getspecific(current_thread_tls_key);
                        if (data)
                            tls_destructor(data);
                        pthread_key_delete(current_thread_tls_key);
                    }
                }
            };
            delete_current_thread_tls_key_on_dlclose_t delete_current_thread_tls_key_on_dlclose;
#endif
            void create_current_thread_tls_key()
            {
                BOOST_VERIFY(!pthread_key_create(&current_thread_tls_key,&tls_destructor));
            }
        }

        boost::detail::thread_data_base* get_current_thread_data()
        {
            boost::call_once(current_thread_tls_init_flag,create_current_thread_tls_key);
            return (boost::detail::thread_data_base*)pthread_getspecific(current_thread_tls_key);
        }

        void set_current_thread_data(detail::thread_data_base* new_data)
        {
            boost::call_once(current_thread_tls_init_flag,create_current_thread_tls_key);
            BOOST_VERIFY(!pthread_setspecific(current_thread_tls_key,new_data));
        }
    }

    namespace
    {
        extern "C"
        {
            static void* thread_proxy(void* param)
            {
                //boost::detail::thread_data_ptr thread_info = static_cast<boost::detail::thread_data_base*>(param)->self;
                boost::detail::thread_data_ptr thread_info = static_cast<boost::detail::thread_data_base*>(param)->shared_from_this();
                thread_info->self.reset();
                detail::set_current_thread_data(thread_info.get());
#if defined BOOST_THREAD_PROVIDES_INTERRUPTIONS
                BOOST_TRY
                {
#endif
                    thread_info->run();
#if defined BOOST_THREAD_PROVIDES_INTERRUPTIONS

                }
                BOOST_CATCH (thread_interrupted const&)
                {
                }
// Removed as it stops the debugger identifying the cause of the exception
// Unhandled exceptions still cause the application to terminate
//                 BOOST_CATCH(...)
//                 {
//                   throw;
//
//                     std::terminate();
//                 }
                BOOST_CATCH_END
#endif
                detail::tls_destructor(thread_info.get());
                detail::set_current_thread_data(0);
                boost::lock_guard<boost::mutex> lock(thread_info->data_mutex);
                thread_info->done=true;
                thread_info->done_condition.notify_all();

                return 0;
            }
        }
    }
    namespace detail
    {
        struct externally_launched_thread:
            detail::thread_data_base
        {
            externally_launched_thread()
            {
#if defined BOOST_THREAD_PROVIDES_INTERRUPTIONS
                interrupt_enabled=false;
#endif
            }
            ~externally_launched_thread() {
              BOOST_ASSERT(notify.empty());
              notify.clear();
              BOOST_ASSERT(async_states_.empty());
              async_states_.clear();
            }
            void run()
            {}
            void notify_all_at_thread_exit(condition_variable*, mutex*)
            {}

        private:
            externally_launched_thread(externally_launched_thread&);
            void operator=(externally_launched_thread&);
        };

        thread_data_base* make_external_thread_data()
        {
            thread_data_base* const me(detail::heap_new<externally_launched_thread>());
            me->self.reset(me);
            set_current_thread_data(me);
            return me;
        }


        thread_data_base* get_or_make_current_thread_data()
        {
            thread_data_base* current_thread_data(get_current_thread_data());
            if(!current_thread_data)
            {
                current_thread_data=make_external_thread_data();
            }
            return current_thread_data;
        }

    }


    thread::thread() BOOST_NOEXCEPT
    {}

    bool thread::start_thread_noexcept()
    {
        thread_info->self=thread_info;
        int const res = pthread_create(&thread_info->thread_handle, 0, &thread_proxy, thread_info.get());
        if (res != 0)
        {
            thread_info->self.reset();
            return false;
        }
        return true;
    }

    bool thread::start_thread_noexcept(const attributes& attr)
    {
        thread_info->self=thread_info;
        const attributes::native_handle_type* h = attr.native_handle();
        int res = pthread_create(&thread_info->thread_handle, h, &thread_proxy, thread_info.get());
        if (res != 0)
        {
            thread_info->self.reset();
            return false;
        }
        int detached_state;
        res = pthread_attr_getdetachstate(h, &detached_state);
        if (res != 0)
        {
            thread_info->self.reset();
            return false;
        }
        if (PTHREAD_CREATE_DETACHED==detached_state)
        {
          detail::thread_data_ptr local_thread_info;
          thread_info.swap(local_thread_info);

          if(local_thread_info)
          {
              //lock_guard<mutex> lock(local_thread_info->data_mutex);
              if(!local_thread_info->join_started)
              {
                  //BOOST_VERIFY(!pthread_detach(local_thread_info->thread_handle));
                  local_thread_info->join_started=true;
                  local_thread_info->joined=true;
              }
          }
        }
        return true;
    }



    detail::thread_data_ptr thread::get_thread_info BOOST_PREVENT_MACRO_SUBSTITUTION () const
    {
        return thread_info;
    }

    bool thread::join_noexcept()
    {
        detail::thread_data_ptr const local_thread_info=(get_thread_info)();
        if(local_thread_info)
        {
            bool do_join=false;

            {
                unique_lock<mutex> lock(local_thread_info->data_mutex);
                while(!local_thread_info->done)
                {
                    local_thread_info->done_condition.wait(lock);
                }
                do_join=!local_thread_info->join_started;

                if(do_join)
                {
                    local_thread_info->join_started=true;
                }
                else
                {
                    while(!local_thread_info->joined)
                    {
                        local_thread_info->done_condition.wait(lock);
                    }
                }
            }
            if(do_join)
            {
                void* result=0;
                BOOST_VERIFY(!pthread_join(local_thread_info->thread_handle,&result));
                lock_guard<mutex> lock(local_thread_info->data_mutex);
                local_thread_info->joined=true;
                local_thread_info->done_condition.notify_all();
            }

            if(thread_info==local_thread_info)
            {
                thread_info.reset();
            }
            return true;
        }
        else
        {
          return false;
        }
    }

    bool thread::do_try_join_until_noexcept(struct timespec const &timeout, bool& res)
    {
        detail::thread_data_ptr const local_thread_info=(get_thread_info)();
        if(local_thread_info)
        {
            bool do_join=false;

            {
                unique_lock<mutex> lock(local_thread_info->data_mutex);
                while(!local_thread_info->done)
                {
                    if(!local_thread_info->done_condition.do_wait_until(lock,timeout))
                    {
                      res=false;
                      return true;
                    }
                }
                do_join=!local_thread_info->join_started;

                if(do_join)
                {
                    local_thread_info->join_started=true;
                }
                else
                {
                    while(!local_thread_info->joined)
                    {
                        local_thread_info->done_condition.wait(lock);
                    }
                }
            }
            if(do_join)
            {
                void* result=0;
                BOOST_VERIFY(!pthread_join(local_thread_info->thread_handle,&result));
                lock_guard<mutex> lock(local_thread_info->data_mutex);
                local_thread_info->joined=true;
                local_thread_info->done_condition.notify_all();
            }

            if(thread_info==local_thread_info)
            {
                thread_info.reset();
            }
            res=true;
            return true;
        }
        else
        {
          return false;
        }
    }

    bool thread::joinable() const BOOST_NOEXCEPT
    {
        return (get_thread_info)()?true:false;
    }


    void thread::detach()
    {
        detail::thread_data_ptr local_thread_info;
        thread_info.swap(local_thread_info);

        if(local_thread_info)
        {
            lock_guard<mutex> lock(local_thread_info->data_mutex);
            if(!local_thread_info->join_started)
            {
                BOOST_VERIFY(!pthread_detach(local_thread_info->thread_handle));
                local_thread_info->join_started=true;
                local_thread_info->joined=true;
            }
        }
    }

    namespace this_thread
    {
      namespace no_interruption_point
      {
        namespace hidden
        {
          void BOOST_THREAD_DECL sleep_for(const timespec& ts)
          {

                if (boost::detail::timespec_ge(ts, boost::detail::timespec_zero()))
                {

    #   if defined(BOOST_HAS_PTHREAD_DELAY_NP)
    #     if defined(__IBMCPP__) ||  defined(_AIX)
                  BOOST_VERIFY(!pthread_delay_np(const_cast<timespec*>(&ts)));
    #     else
                  BOOST_VERIFY(!pthread_delay_np(&ts));
    #     endif
    #   elif defined(BOOST_HAS_NANOSLEEP)
                  //  nanosleep takes a timespec that is an offset, not
                  //  an absolute time.
                  nanosleep(&ts, 0);
    #   else
                  mutex mx;
                  unique_lock<mutex> lock(mx);
                  condition_variable cond;
                  cond.do_wait_for(lock, ts);
    #   endif
                }
          }

          void BOOST_THREAD_DECL sleep_until(const timespec& ts)
          {
                timespec now = boost::detail::timespec_now();
                if (boost::detail::timespec_gt(ts, now))
                {
                  for (int foo=0; foo < 5; ++foo)
                  {

    #   if defined(BOOST_HAS_PTHREAD_DELAY_NP)
                    timespec d = boost::detail::timespec_minus(ts, now);
                    BOOST_VERIFY(!pthread_delay_np(&d));
    #   elif defined(BOOST_HAS_NANOSLEEP)
                    //  nanosleep takes a timespec that is an offset, not
                    //  an absolute time.
                    timespec d = boost::detail::timespec_minus(ts, now);
                    nanosleep(&d, 0);
    #   else
                    mutex mx;
                    unique_lock<mutex> lock(mx);
                    condition_variable cond;
                    cond.do_wait_until(lock, ts);
    #   endif
                    timespec now2 = boost::detail::timespec_now();
                    if (boost::detail::timespec_ge(now2, ts))
                    {
                      return;
                    }
                  }
                }
          }

        }
      }
      namespace hidden
      {
        void BOOST_THREAD_DECL sleep_for(const timespec& ts)
        {
            boost::detail::thread_data_base* const thread_info=boost::detail::get_current_thread_data();

            if(thread_info)
            {
              unique_lock<mutex> lk(thread_info->sleep_mutex);
              while( thread_info->sleep_condition.do_wait_for(lk,ts)) {}
            }
            else
            {
              boost::this_thread::no_interruption_point::hidden::sleep_for(ts);
            }
        }

        void BOOST_THREAD_DECL sleep_until(const timespec& ts)
        {
            boost::detail::thread_data_base* const thread_info=boost::detail::get_current_thread_data();

            if(thread_info)
            {
              unique_lock<mutex> lk(thread_info->sleep_mutex);
              while(thread_info->sleep_condition.do_wait_until(lk,ts)) {}
            }
            else
            {
              boost::this_thread::no_interruption_point::hidden::sleep_until(ts);
            }
        }
      } // hidden
    } // this_thread

    namespace this_thread
    {
        void yield() BOOST_NOEXCEPT
        {
#   if defined(BOOST_HAS_SCHED_YIELD)
            BOOST_VERIFY(!sched_yield());
#   elif defined(BOOST_HAS_PTHREAD_YIELD)
            BOOST_VERIFY(!pthread_yield());
//#   elif defined BOOST_THREAD_USES_DATETIME
//            xtime xt;
//            xtime_get(&xt, TIME_UTC_);
//            sleep(xt);
//            sleep_for(chrono::milliseconds(0));
#   else
#error
            timespec ts;
            ts.tv_sec= 0;
            ts.tv_nsec= 0;
            hidden::sleep_for(ts);
#   endif
        }
    }
    unsigned thread::hardware_concurrency() BOOST_NOEXCEPT
    {
#if defined(PTW32_VERSION) || defined(__hpux)
        return pthread_num_processors_np();
#elif defined(__APPLE__) || defined(__FreeBSD__)
        int count;
        size_t size=sizeof(count);
        return sysctlbyname("hw.ncpu",&count,&size,NULL,0)?0:count;
#elif defined(BOOST_HAS_UNISTD_H) && defined(_SC_NPROCESSORS_ONLN)
        int const count=sysconf(_SC_NPROCESSORS_ONLN);
        return (count>0)?count:0;
#elif defined(__GLIBC__)
        return get_nprocs();
#else
        return 0;
#endif
    }

    unsigned thread::physical_concurrency() BOOST_NOEXCEPT
    {
#ifdef __linux__
        try {
            using namespace std;

            ifstream proc_cpuinfo ("/proc/cpuinfo");

            const string physical_id("physical id"), core_id("core id");

            typedef std::pair<unsigned, unsigned> core_entry; // [physical ID, core id]

            std::set<core_entry> cores;

            core_entry current_core_entry;

            string line;
            while ( getline(proc_cpuinfo, line) ) {
                if (line.empty())
                    continue;

                vector<string> key_val(2);
                boost::split(key_val, line, boost::is_any_of(":"));

                if (key_val.size() != 2)
                  return hardware_concurrency();

                string key   = key_val[0];
                string value = key_val[1];
                boost::trim(key);
                boost::trim(value);

                if (key == physical_id) {
                    current_core_entry.first = boost::lexical_cast<unsigned>(value);
                    continue;
                }

                if (key == core_id) {
                    current_core_entry.second = boost::lexical_cast<unsigned>(value);
                    cores.insert(current_core_entry);
                    continue;
                }
            }
            // Fall back to hardware_concurrency() in case
            // /proc/cpuinfo is formatted differently than we expect.
            return cores.size() != 0 ? cores.size() : hardware_concurrency();
        } catch(...) {
          return hardware_concurrency();
        }
#elif defined(__APPLE__)
        int count;
        size_t size=sizeof(count);
        return sysctlbyname("hw.physicalcpu",&count,&size,NULL,0)?0:count;
#else
        return hardware_concurrency();
#endif
    }

#if defined BOOST_THREAD_PROVIDES_INTERRUPTIONS
    void thread::interrupt()
    {
        detail::thread_data_ptr const local_thread_info=(get_thread_info)();
        if(local_thread_info)
        {
            lock_guard<mutex> lk(local_thread_info->data_mutex);
            local_thread_info->interrupt_requested=true;
            if(local_thread_info->current_cond)
            {
                boost::pthread::pthread_mutex_scoped_lock internal_lock(local_thread_info->cond_mutex);
                BOOST_VERIFY(!pthread_cond_broadcast(local_thread_info->current_cond));
            }
        }
    }

    bool thread::interruption_requested() const BOOST_NOEXCEPT
    {
        detail::thread_data_ptr const local_thread_info=(get_thread_info)();
        if(local_thread_info)
        {
            lock_guard<mutex> lk(local_thread_info->data_mutex);
            return local_thread_info->interrupt_requested;
        }
        else
        {
            return false;
        }
    }
#endif

    thread::native_handle_type thread::native_handle()
    {
        detail::thread_data_ptr const local_thread_info=(get_thread_info)();
        if(local_thread_info)
        {
            lock_guard<mutex> lk(local_thread_info->data_mutex);
            return local_thread_info->thread_handle;
        }
        else
        {
            return pthread_t();
        }
    }



#if defined BOOST_THREAD_PROVIDES_INTERRUPTIONS
    namespace this_thread
    {
        void interruption_point()
        {
#ifndef BOOST_NO_EXCEPTIONS
            boost::detail::thread_data_base* const thread_info=detail::get_current_thread_data();
            if(thread_info && thread_info->interrupt_enabled)
            {
                lock_guard<mutex> lg(thread_info->data_mutex);
                if(thread_info->interrupt_requested)
                {
                    thread_info->interrupt_requested=false;
                    throw thread_interrupted();
                }
            }
#endif
        }

        bool interruption_enabled() BOOST_NOEXCEPT
        {
            boost::detail::thread_data_base* const thread_info=detail::get_current_thread_data();
            return thread_info && thread_info->interrupt_enabled;
        }

        bool interruption_requested() BOOST_NOEXCEPT
        {
            boost::detail::thread_data_base* const thread_info=detail::get_current_thread_data();
            if(!thread_info)
            {
                return false;
            }
            else
            {
                lock_guard<mutex> lg(thread_info->data_mutex);
                return thread_info->interrupt_requested;
            }
        }

        disable_interruption::disable_interruption() BOOST_NOEXCEPT:
            interruption_was_enabled(interruption_enabled())
        {
            if(interruption_was_enabled)
            {
                detail::get_current_thread_data()->interrupt_enabled=false;
            }
        }

        disable_interruption::~disable_interruption() BOOST_NOEXCEPT
        {
            if(detail::get_current_thread_data())
            {
                detail::get_current_thread_data()->interrupt_enabled=interruption_was_enabled;
            }
        }

        restore_interruption::restore_interruption(disable_interruption& d) BOOST_NOEXCEPT
        {
            if(d.interruption_was_enabled)
            {
                detail::get_current_thread_data()->interrupt_enabled=true;
            }
        }

        restore_interruption::~restore_interruption() BOOST_NOEXCEPT
        {
            if(detail::get_current_thread_data())
            {
                detail::get_current_thread_data()->interrupt_enabled=false;
            }
        }
    }
#endif

    namespace detail
    {
        void add_thread_exit_function(thread_exit_function_base* func)
        {
            detail::thread_data_base* const current_thread_data(get_or_make_current_thread_data());
            thread_exit_callback_node* const new_node=
                heap_new<thread_exit_callback_node>(func,current_thread_data->thread_exit_callbacks);
            current_thread_data->thread_exit_callbacks=new_node;
        }

        tss_data_node* find_tss_data(void const* key)
        {
            detail::thread_data_base* const current_thread_data(get_current_thread_data());
            if(current_thread_data)
            {
                std::map<void const*,tss_data_node>::iterator current_node=
                    current_thread_data->tss_data.find(key);
                if(current_node!=current_thread_data->tss_data.end())
                {
                    return &current_node->second;
                }
            }
            return 0;
        }

        void* get_tss_data(void const* key)
        {
            if(tss_data_node* const current_node=find_tss_data(key))
            {
                return current_node->value;
            }
            return 0;
        }

        void add_new_tss_node(void const* key,
                              boost::shared_ptr<tss_cleanup_function> func,
                              void* tss_data)
        {
            detail::thread_data_base* const current_thread_data(get_or_make_current_thread_data());
            current_thread_data->tss_data.insert(std::make_pair(key,tss_data_node(func,tss_data)));
        }

        void erase_tss_node(void const* key)
        {
            detail::thread_data_base* const current_thread_data(get_current_thread_data());
            if(current_thread_data)
            {
                current_thread_data->tss_data.erase(key);
            }
        }

        void set_tss_data(void const* key,
                          boost::shared_ptr<tss_cleanup_function> func,
                          void* tss_data,bool cleanup_existing)
        {
            if(tss_data_node* const current_node=find_tss_data(key))
            {
                if(cleanup_existing && current_node->func && (current_node->value!=0))
                {
                    (*current_node->func)(current_node->value);
                }
                if(func || (tss_data!=0))
                {
                    current_node->func=func;
                    current_node->value=tss_data;
                }
                else
                {
                    erase_tss_node(key);
                }
            }
            else if(func || (tss_data!=0))
            {
                add_new_tss_node(key,func,tss_data);
            }
        }
    }

    BOOST_THREAD_DECL void notify_all_at_thread_exit(condition_variable& cond, unique_lock<mutex> lk)
    {
      detail::thread_data_base* const current_thread_data(detail::get_current_thread_data());
      if(current_thread_data)
      {
        current_thread_data->notify_all_at_thread_exit(&cond, lk.release());
      }
    }
namespace detail {

    void BOOST_THREAD_DECL make_ready_at_thread_exit(shared_ptr<shared_state_base> as)
    {
      detail::thread_data_base* const current_thread_data(detail::get_current_thread_data());
      if(current_thread_data)
      {
        current_thread_data->make_ready_at_thread_exit(as);
      }
    }
}



}
