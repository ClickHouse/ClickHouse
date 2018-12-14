//===----------------------- cxa_thread_atexit.cpp ------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <pthread.h>

using Dtor = void(*)(void*);

namespace {
  // This implementation is used if the C library does not provide
  // __cxa_thread_atexit_impl() for us.  It has a number of limitations that are
  // difficult to impossible to address without ..._impl():
  //
  // - dso_symbol is ignored.  This means that a shared library may be unloaded
  //   (via dlclose()) before its thread_local destructors have run.
  //
  // - thread_local destructors for the main thread are run by the destructor of
  //   a static object.  This is later than expected; they should run before the
  //   destructors of any objects with static storage duration.
  //
  // - thread_local destructors on non-main threads run on the first iteration
  //   through the __libccpp_tls_key destructors.
  //   std::notify_all_at_thread_exit() and similar functions must be careful to
  //   wait until the second iteration to provide their intended ordering
  //   guarantees.
  //
  // Another limitation, though one shared with ..._impl(), is that any
  // thread_locals that are first initialized after non-thread_local global
  // destructors begin to run will not be destroyed.  [basic.start.term] states
  // that all thread_local destructors are sequenced before the destruction of
  // objects with static storage duration, resulting in a contradiction if a
  // thread_local is constructed after that point.  Thus we consider such
  // programs ill-formed, and don't bother to run those destructors.  (If the
  // program terminates abnormally after such a thread_local is constructed,
  // the destructor is not expected to run and thus there is no contradiction.
  // So construction still has to work.)

  struct DtorList {
    Dtor dtor;
    void* obj;
    DtorList* next;
  };

  // The linked list of thread-local destructors to run
  __thread DtorList* dtors = nullptr;
  // True if the destructors are currently scheduled to run on this thread
  __thread bool dtors_alive = false;
  // Used to trigger destructors on thread exit; value is ignored
  pthread_key_t dtors_key;

  void run_dtors(void*) {
    while (auto head = dtors) {
      dtors = head->next;
      head->dtor(head->obj);
      std::free(head);
    }

    dtors_alive = false;
  }

  struct DtorsManager {
    DtorsManager() {
      // There is intentionally no matching std::__libcpp_tls_delete call, as
      // __cxa_thread_atexit() may be called arbitrarily late (for example, from
      // global destructors or atexit() handlers).
      if (pthread_key_create(&dtors_key, run_dtors) != 0) {
        abort();
      }
    }

    ~DtorsManager() {
      // std::__libcpp_tls_key destructors do not run on threads that call exit()
      // (including when the main thread returns from main()), so we explicitly
      // call the destructor here.  This runs at exit time (potentially earlier
      // if libc++abi is dlclose()'d).  Any thread_locals initialized after this
      // point will not be destroyed.
      run_dtors(nullptr);
    }
  };
} // namespace


extern "C"
{
  int __cxa_thread_atexit_impl(Dtor dtor, void* obj, void* dso_symbol) throw()
  {
      // Initialize the dtors std::__libcpp_tls_key (uses __cxa_guard_*() for
      // one-time initialization and __cxa_atexit() for destruction)
      static DtorsManager manager;

      if (!dtors_alive) {
        if (pthread_setspecific(dtors_key, &dtors_key) != 0) {
          return -1;
        }
        dtors_alive = true;
      }

      auto head = static_cast<DtorList*>(std::malloc(sizeof(DtorList)));
      if (!head) {
        return -1;
      }

      head->dtor = dtor;
      head->obj = obj;
      head->next = dtors;
      dtors = head;

      return 0;
  }

  int __attribute__((__weak__)) __cxa_thread_atexit(Dtor dtor, void* obj, void* dso_symbol) throw()
  {
      return __cxa_thread_atexit_impl(dtor, obj, dso_symbol);
  }
} // extern "C"
