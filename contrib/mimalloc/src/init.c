/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#include "mimalloc.h"
#include "mimalloc-internal.h"

#include <string.h>  // memcpy

// Empty page used to initialize the small free pages array
const mi_page_t _mi_page_empty = {
  0, false, false, {0},
  0, 0,
  NULL, 0, 0,   // free, used, cookie
  NULL, 0, {0},
  0, NULL, NULL, NULL
  #if (MI_INTPTR_SIZE==4)
  , { NULL }
  #endif
};

#define MI_PAGE_EMPTY() ((mi_page_t*)&_mi_page_empty)
#define MI_SMALL_PAGES_EMPTY  \
  { MI_INIT128(MI_PAGE_EMPTY), MI_PAGE_EMPTY(), MI_PAGE_EMPTY() }


// Empty page queues for every bin
#define QNULL(sz)  { NULL, NULL, (sz)*sizeof(uintptr_t) }
#define MI_PAGE_QUEUES_EMPTY \
  { QNULL(1), \
    QNULL(1), QNULL(2), QNULL(3), QNULL(4), QNULL(5), QNULL(6), QNULL(7), QNULL(8), \
    QNULL(10), QNULL(12), QNULL(14), QNULL(16), QNULL(20), QNULL(24), QNULL(28), QNULL(32), \
    QNULL(40), QNULL(48), QNULL(56), QNULL(64), QNULL(80), QNULL(96), QNULL(112), QNULL(128), \
    QNULL(160), QNULL(192), QNULL(224), QNULL(256), QNULL(320), QNULL(384), QNULL(448), QNULL(512), \
    QNULL(640), QNULL(768), QNULL(896), QNULL(1024), QNULL(1280), QNULL(1536), QNULL(1792), QNULL(2048), \
    QNULL(2560), QNULL(3072), QNULL(3584), QNULL(4096), QNULL(5120), QNULL(6144), QNULL(7168), QNULL(8192), \
    QNULL(10240), QNULL(12288), QNULL(14336), QNULL(16384), QNULL(20480), QNULL(24576), QNULL(28672), QNULL(32768), \
    QNULL(40960), QNULL(49152), QNULL(57344), QNULL(65536), QNULL(81920), QNULL(98304), QNULL(114688), \
    QNULL(MI_LARGE_WSIZE_MAX + 1  /*131072, Huge queue */), \
    QNULL(MI_LARGE_WSIZE_MAX + 2) /* Full queue */ }

#define MI_STAT_COUNT_NULL()  {0,0,0,0}

// Empty statistics
#if MI_STAT>1
#define MI_STAT_COUNT_END_NULL()  , { MI_STAT_COUNT_NULL(), MI_INIT64(MI_STAT_COUNT_NULL) }
#else
#define MI_STAT_COUNT_END_NULL()
#endif

#define MI_STATS_NULL  \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  { 0, 0 } \
  MI_STAT_COUNT_END_NULL()

// --------------------------------------------------------
// Statically allocate an empty heap as the initial
// thread local value for the default heap,
// and statically allocate the backing heap for the main
// thread so it can function without doing any allocation
// itself (as accessing a thread local for the first time
// may lead to allocation itself on some platforms)
// --------------------------------------------------------

const mi_heap_t _mi_heap_empty = {
  NULL,
  MI_SMALL_PAGES_EMPTY,
  MI_PAGE_QUEUES_EMPTY,
  NULL,
  0,
  0,
  0,
  0,
  false
};

mi_decl_thread mi_heap_t* _mi_heap_default = (mi_heap_t*)&_mi_heap_empty;


#define tld_main_stats  ((mi_stats_t*)((uint8_t*)&tld_main + offsetof(mi_tld_t,stats)))

static mi_tld_t tld_main = {
  0,
  &_mi_heap_main,
  { { NULL, NULL }, 0, 0, 0, 0, {NULL,NULL}, tld_main_stats }, // segments
  { 0, NULL, NULL, 0, tld_main_stats },              // os
  { MI_STATS_NULL }                                  // stats
};

mi_heap_t _mi_heap_main = {
  &tld_main,
  MI_SMALL_PAGES_EMPTY,
  MI_PAGE_QUEUES_EMPTY,
  NULL,
  0,
  0,
  0,
  0,
  false   // can reclaim
};

bool _mi_process_is_initialized = false;  // set to `true` in `mi_process_init`.

mi_stats_t _mi_stats_main = { MI_STATS_NULL };

/* -----------------------------------------------------------
  Initialization of random numbers
----------------------------------------------------------- */

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

uintptr_t _mi_random_shuffle(uintptr_t x) {
  #if (MI_INTPTR_SIZE==8)
    // by Sebastiano Vigna, see: <http://xoshiro.di.unimi.it/splitmix64.c>
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9UL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebUL;
  x ^= x >> 31;
  #elif (MI_INTPTR_SIZE==4)
    // by Chris Wellons, see: <https://nullprogram.com/blog/2018/07/31/>
  x ^= x >> 16;
  x *= 0x7feb352dUL;
  x ^= x >> 15;
  x *= 0x846ca68bUL;
  x ^= x >> 16;
  #endif
  return x;
}

uintptr_t _mi_random_init(uintptr_t seed /* can be zero */) {
   // Hopefully, ASLR makes our function address random
  uintptr_t x = (uintptr_t)((void*)&_mi_random_init);
  x ^= seed;
  // xor with high res time
#ifdef _WIN32
  LARGE_INTEGER pcount;
  QueryPerformanceCounter(&pcount);
  x ^= (uintptr_t)(pcount.QuadPart);
#else
  struct timespec time;
  clock_gettime(CLOCK_MONOTONIC, &time);
  x ^= (uintptr_t)time.tv_sec;
  x ^= (uintptr_t)time.tv_nsec;
#endif
  // and do a few randomization steps
  uintptr_t max = ((x ^ (x >> 7)) & 0x0F) + 1;
  for (uintptr_t i = 0; i < max; i++) {
    x = _mi_random_shuffle(x);
  }
  return x;
}

uintptr_t _mi_ptr_cookie(const void* p) {
  return ((uintptr_t)p ^ _mi_heap_main.cookie);
}

/* -----------------------------------------------------------
  Initialization and freeing of the thread local heaps
----------------------------------------------------------- */

typedef struct mi_thread_data_s {
  mi_heap_t  heap;  // must come first due to cast in `_mi_heap_done`
  mi_tld_t   tld;
} mi_thread_data_t;

// Initialize the thread local default heap, called from `mi_thread_init`
static bool _mi_heap_init() {
  if (mi_heap_is_initialized(_mi_heap_default)) return true;
  if (_mi_is_main_thread()) {
    // the main heap is statically allocated
    _mi_heap_default = &_mi_heap_main;
    mi_assert_internal(_mi_heap_default->tld->heap_backing == _mi_heap_default);
  }
  else {
    // use `_mi_os_alloc` to allocate directly from the OS
    mi_thread_data_t* td = (mi_thread_data_t*)_mi_os_alloc(sizeof(mi_thread_data_t),&_mi_stats_main); // Todo: more efficient allocation?
    if (td == NULL) {
      _mi_error_message("failed to allocate thread local heap memory\n");
      return false;
    }
    mi_tld_t*  tld = &td->tld;
    mi_heap_t* heap = &td->heap;
    memcpy(heap, &_mi_heap_empty, sizeof(*heap));
    heap->thread_id = _mi_thread_id();
    heap->random = _mi_random_init(heap->thread_id);
    heap->cookie = ((uintptr_t)heap ^ _mi_heap_random(heap)) | 1;
    heap->tld = tld;
    memset(tld, 0, sizeof(*tld));
    tld->heap_backing = heap;
    tld->segments.stats = &tld->stats;
    tld->os.stats = &tld->stats;
    _mi_heap_default = heap;
  }
  return false;
}

// Free the thread local default heap (called from `mi_thread_done`)
static bool _mi_heap_done() {
  mi_heap_t* heap = _mi_heap_default;
  if (!mi_heap_is_initialized(heap)) return true;

  // reset default heap
  _mi_heap_default = (_mi_is_main_thread() ? &_mi_heap_main : (mi_heap_t*)&_mi_heap_empty);

  // todo: delete all non-backing heaps?

  // switch to backing heap and free it
  heap = heap->tld->heap_backing;
  if (!mi_heap_is_initialized(heap)) return false;

  _mi_stats_done(&heap->tld->stats);

  // free if not the main thread (or in debug mode)
  if (heap != &_mi_heap_main) {
    if (heap->page_count > 0) {
      _mi_heap_collect_abandon(heap);
    }
    _mi_os_free(heap, sizeof(mi_thread_data_t), &_mi_stats_main);
  }
  else if (MI_DEBUG > 0) {
    _mi_heap_destroy_pages(heap);
    mi_assert_internal(heap->tld->heap_backing == &_mi_heap_main);
  }
  return false;
}



// --------------------------------------------------------
// Try to run `mi_thread_done()` automatically so any memory
// owned by the thread but not yet released can be abandoned
// and re-owned by another thread.
//
// 1. windows dynamic library:
//     call from DllMain on DLL_THREAD_DETACH
// 2. windows static library:
//     use `FlsAlloc` to call a destructor when the thread is done
// 3. unix, pthreads:
//     use a pthread key to call a destructor when a pthread is done
//
// In the last two cases we also need to call `mi_process_init`
// to set up the thread local keys.
// --------------------------------------------------------

#ifndef _WIN32
#define MI_USE_PTHREADS
#endif

#if defined(_WIN32) && defined(MI_SHARED_LIB)
  // nothing to do as it is done in DllMain
#elif defined(_WIN32) && !defined(MI_SHARED_LIB)
  // use thread local storage keys to detect thread ending
  #include <windows.h>
  #include <fibersapi.h>
  static DWORD mi_fls_key;
  static void NTAPI mi_fls_done(PVOID value) {
    if (value!=NULL) mi_thread_done();
  }
#elif defined(MI_USE_PTHREADS)
  // use pthread locol storage keys to detect thread ending
  #include <pthread.h>
  static pthread_key_t mi_pthread_key;
  static void mi_pthread_done(void* value) {
    if (value!=NULL) mi_thread_done();
  }
#else
  #pragma message("define a way to call mi_thread_done when a thread is done")
#endif

// Set up handlers so `mi_thread_done` is called automatically
static void mi_process_setup_auto_thread_done() {
  static bool tls_initialized = false; // fine if it races
  if (tls_initialized) return;
  tls_initialized = true;
  #if defined(_WIN32) && defined(MI_SHARED_LIB)
    // nothing to do as it is done in DllMain
  #elif defined(_WIN32) && !defined(MI_SHARED_LIB)
    mi_fls_key = FlsAlloc(&mi_fls_done);
  #elif defined(MI_USE_PTHREADS)
    pthread_key_create(&mi_pthread_key, &mi_pthread_done);
  #endif
}


bool _mi_is_main_thread() {
  return (_mi_heap_main.thread_id==0 || _mi_heap_main.thread_id == _mi_thread_id());
}

// This is called from the `mi_malloc_generic`
void mi_thread_init() mi_attr_noexcept
{
  // ensure our process has started already
  mi_process_init();

  // initialize the thread local default heap
  if (_mi_heap_init()) return;  // returns true if already initialized

  // don't further initialize for the main thread
  if (_mi_is_main_thread()) return;

  mi_stat_increase(mi_get_default_heap()->tld->stats.threads, 1);

  // set hooks so our mi_thread_done() will be called
  #if defined(_WIN32) && defined(MI_SHARED_LIB)
    // nothing to do as it is done in DllMain
  #elif defined(_WIN32) && !defined(MI_SHARED_LIB)
    FlsSetValue(mi_fls_key, (void*)(_mi_thread_id()|1)); // set to a dummy value so that `mi_fls_done` is called
  #elif defined(MI_USE_PTHREADS)
    pthread_setspecific(mi_pthread_key, (void*)(_mi_thread_id()|1)); // set to a dummy value so that `mi_pthread_done` is called
  #endif

  _mi_verbose_message("thread init: 0x%zx\n", _mi_thread_id());
}

void mi_thread_done() mi_attr_noexcept {
  // stats
  mi_heap_t* heap = mi_get_default_heap();
  if (!_mi_is_main_thread() && mi_heap_is_initialized(heap))  {
    mi_stat_decrease(heap->tld->stats.threads, 1);
  }

  // abandon the thread local heap
  if (_mi_heap_done()) return; // returns true if already ran

  if (!_mi_is_main_thread()) {
    _mi_verbose_message("thread done: 0x%zx\n", _mi_thread_id());
  }
}


// --------------------------------------------------------
// Run functions on process init/done, and thread init/done
// --------------------------------------------------------
static void mi_process_done(void);

void mi_process_init() mi_attr_noexcept {
  // ensure we are called once
  if (_mi_process_is_initialized) return;
  _mi_process_is_initialized = true;

  _mi_heap_main.thread_id = _mi_thread_id();
  _mi_verbose_message("process init: 0x%zx\n", _mi_heap_main.thread_id);
  uintptr_t random = _mi_random_init(_mi_heap_main.thread_id);
  _mi_heap_main.cookie = (uintptr_t)&_mi_heap_main ^ random;
  _mi_heap_main.random = _mi_random_shuffle(random);
  #if (MI_DEBUG)
  _mi_verbose_message("debug level : %d\n", MI_DEBUG);
  #endif
  atexit(&mi_process_done);
  mi_process_setup_auto_thread_done();
  mi_stats_reset();
}

static void mi_process_done(void) {
  // only shutdown if we were initialized
  if (!_mi_process_is_initialized) return;
  // ensure we are called once
  static bool process_done = false;
  if (process_done) return;
  process_done = true;

  #ifndef NDEBUG
  mi_collect(true);
  #endif
  if (mi_option_is_enabled(mi_option_show_stats) ||
      mi_option_is_enabled(mi_option_verbose)) {
    mi_stats_print(NULL);
  }
  _mi_verbose_message("process done: 0x%zx\n", _mi_heap_main.thread_id);
}



#if defined(_WIN32) && defined(MI_SHARED_LIB)
  // Windows DLL: easy to hook into process_init and thread_done
  #include <windows.h>

  __declspec(dllexport) BOOL WINAPI DllMain(HINSTANCE inst, DWORD reason, LPVOID reserved) {
    UNUSED(reserved);
    UNUSED(inst);
    if (reason==DLL_PROCESS_ATTACH) {
      mi_process_init();
    }
    else if (reason==DLL_THREAD_DETACH) {
      mi_thread_done();
    }
    return TRUE;
  }

#elif defined(__cplusplus)
  // C++: use static initialization to detect process start
  static bool _mi_process_init() {
    mi_process_init();
    return (mi_main_thread_id != 0);
  }
  static bool mi_initialized = _mi_process_init();

#elif defined(__GNUC__) || defined(__clang__)
  // GCC,Clang: use the constructor attribute
  static void __attribute__((constructor)) _mi_process_init() {
    mi_process_init();
  }

#elif defined(_MSC_VER)
  // MSVC: use data section magic for static libraries
  // See <https://www.codeguru.com/cpp/misc/misc/applicationcontrol/article.php/c6945/Running-Code-Before-and-After-Main.htm>
  static int _mi_process_init(void) {
    mi_process_init();
    return 0;
  }
  typedef int(*_crt_cb)(void);
  #ifdef _M_X64
    __pragma(comment(linker, "/include:" "_mi_msvc_initu"))
    #pragma section(".CRT$XIU", long, read)
  #else
    __pragma(comment(linker, "/include:" "__mi_msvc_initu"))
  #endif
  #pragma data_seg(".CRT$XIU")
  _crt_cb _mi_msvc_initu[] = { &_mi_process_init };
  #pragma data_seg()

#else
#pragma message("define a way to call mi_process_init/done on your platform")
#endif
