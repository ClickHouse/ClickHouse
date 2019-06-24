/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#pragma once
#ifndef __MIMALLOC_H
#define __MIMALLOC_H

#define MI_MALLOC_VERSION 100   // major + 2 digits minor

// ------------------------------------------------------
// Compiler specific attributes
// ------------------------------------------------------

#ifdef __cplusplus
  #if (__GNUC__ <= 5) || (_MSC_VER <= 1900)
    #define mi_attr_noexcept   throw()
  #else
    #define mi_attr_noexcept   noexcept
  #endif
#else
  #define mi_attr_noexcept
#endif

#ifdef _MSC_VER
  #if !defined(MI_SHARED_LIB)
    #define mi_decl_export
  #elif defined(MI_SHARED_LIB_EXPORT)
    #define mi_decl_export            __declspec(dllexport)
  #else
    #define mi_decl_export            __declspec(dllimport)
  #endif
  #if (_MSC_VER >= 1900) && !defined(__EDG__)
    #define mi_decl_allocator         __declspec(allocator) __declspec(restrict)
  #else
    #define mi_decl_allocator         __declspec(restrict)
  #endif
  #define mi_decl_thread              __declspec(thread)
  #define mi_attr_malloc
  #define mi_attr_alloc_size(s)
  #define mi_attr_alloc_size2(s1,s2)
#elif defined(__GNUC__) || defined(__clang__)
  #define mi_decl_thread              __thread
  #define mi_decl_export              __attribute__((visibility("default")))
  #define mi_decl_allocator
  #define mi_attr_malloc              __attribute__((malloc))
  #if defined(__clang_major__) && (__clang_major__ < 4)
  #define mi_attr_alloc_size(s)
  #define mi_attr_alloc_size2(s1,s2)
  #else
  #define mi_attr_alloc_size(s)       __attribute__((alloc_size(s)))
  #define mi_attr_alloc_size2(s1,s2)  __attribute__((alloc_size(s1,s2)))
  #endif
#else
  #define mi_decl_thread              __thread
  #define mi_decl_export
  #define mi_decl_allocator
  #define mi_attr_malloc
  #define mi_attr_alloc_size(s)
  #define mi_attr_alloc_size2(s1,s2)
#endif

// ------------------------------------------------------
// Includes
// ------------------------------------------------------

#include <stdlib.h>     // size_t, malloc etc.
#include <stdbool.h>    // bool
#include <stdio.h>      // FILE

#ifdef __cplusplus
extern "C" {
#endif

// ------------------------------------------------------
// Standard malloc interface
// ------------------------------------------------------

mi_decl_export mi_decl_allocator void* mi_malloc(size_t size)                mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);
mi_decl_export mi_decl_allocator void* mi_calloc(size_t count, size_t size)  mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(1,2);
mi_decl_export mi_decl_allocator void* mi_realloc(void* p, size_t newsize)   mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_expand(void* p, size_t newsize)    mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);

mi_decl_export void  mi_free(void* p)                     mi_attr_noexcept;
mi_decl_export char* mi_strdup(const char* s)             mi_attr_noexcept;
mi_decl_export char* mi_strndup(const char* s, size_t n)  mi_attr_noexcept;
mi_decl_export char* mi_realpath(const char* fname, char* resolved_name) mi_attr_noexcept;

// ------------------------------------------------------
// Extended functionality
// ------------------------------------------------------
#define MI_SMALL_WSIZE_MAX  (128)
#define MI_SMALL_SIZE_MAX   (MI_SMALL_WSIZE_MAX*sizeof(void*))

mi_decl_export mi_decl_allocator void* mi_malloc_small(size_t size)   mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);
mi_decl_export mi_decl_allocator void* mi_zalloc_small(size_t size)   mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);
mi_decl_export mi_decl_allocator void* mi_zalloc(size_t size)         mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);

mi_decl_export mi_decl_allocator void* mi_mallocn(size_t count, size_t size)            mi_attr_noexcept;
mi_decl_export mi_decl_allocator void* mi_reallocn(void* p, size_t count, size_t size)  mi_attr_noexcept;
mi_decl_export mi_decl_allocator void* mi_reallocf(void* p, size_t newsize)             mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_rezalloc(void* p, size_t newsize)             mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_recalloc(void* p, size_t count, size_t size)  mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(2,3);

mi_decl_export size_t mi_usable_size(void* p)   mi_attr_noexcept;
mi_decl_export size_t mi_good_size(size_t size) mi_attr_noexcept;

mi_decl_export void mi_collect(bool force)    mi_attr_noexcept;
mi_decl_export void mi_stats_print(FILE* out) mi_attr_noexcept;
mi_decl_export void mi_stats_reset()          mi_attr_noexcept;

mi_decl_export void mi_process_init()         mi_attr_noexcept;
mi_decl_export void mi_thread_init()          mi_attr_noexcept;
mi_decl_export void mi_thread_done()          mi_attr_noexcept;
mi_decl_export void mi_thread_stats_print(FILE* out) mi_attr_noexcept;

typedef void (mi_deferred_free_fun)(bool force, unsigned long long heartbeat);
mi_decl_export void mi_register_deferred_free(mi_deferred_free_fun* deferred_free) mi_attr_noexcept;


// ------------------------------------------------------
// Aligned allocation
// ------------------------------------------------------

mi_decl_export mi_decl_allocator void* mi_malloc_aligned(size_t size, size_t alignment) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);
mi_decl_export mi_decl_allocator void* mi_malloc_aligned_at(size_t size, size_t alignment, size_t offset) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);

mi_decl_export mi_decl_allocator void* mi_zalloc_aligned(size_t size, size_t alignment) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);
mi_decl_export mi_decl_allocator void* mi_zalloc_aligned_at(size_t size, size_t alignment, size_t offset) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(1);

mi_decl_export mi_decl_allocator void* mi_calloc_aligned(size_t count, size_t size, size_t alignment) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(1,2);
mi_decl_export mi_decl_allocator void* mi_calloc_aligned_at(size_t count, size_t size, size_t alignment, size_t offset) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(1,2);

mi_decl_export mi_decl_allocator void* mi_realloc_aligned(void* p, size_t newsize, size_t alignment) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_realloc_aligned_at(void* p, size_t newsize, size_t alignment, size_t offset) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);

mi_decl_export mi_decl_allocator void* mi_rezalloc_aligned(void* p, size_t newsize, size_t alignment) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_rezalloc_aligned_at(void* p, size_t newsize, size_t alignment, size_t offset) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);

mi_decl_export mi_decl_allocator void* mi_recalloc_aligned(void* p, size_t count, size_t size, size_t alignment) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(2,3);
mi_decl_export mi_decl_allocator void* mi_recalloc_aligned_at(void* p, size_t count, size_t size, size_t alignment, size_t offset) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(2,3);


// ------------------------------------------------------
// Heaps
// ------------------------------------------------------
struct mi_heap_s;
typedef struct mi_heap_s mi_heap_t;

mi_decl_export mi_heap_t* mi_heap_new();
mi_decl_export void       mi_heap_delete(mi_heap_t* heap);
mi_decl_export void       mi_heap_destroy(mi_heap_t* heap);
mi_decl_export mi_heap_t* mi_heap_set_default(mi_heap_t* heap);
mi_decl_export mi_heap_t* mi_heap_get_default();
mi_decl_export mi_heap_t* mi_heap_get_backing();
mi_decl_export void       mi_heap_collect(mi_heap_t* heap, bool force) mi_attr_noexcept;

mi_decl_export mi_decl_allocator void* mi_heap_malloc(mi_heap_t* heap, size_t size) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_heap_zalloc(mi_heap_t* heap, size_t size) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size(2);
mi_decl_export mi_decl_allocator void* mi_heap_calloc(mi_heap_t* heap, size_t count, size_t size) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(2, 3);
mi_decl_export mi_decl_allocator void* mi_heap_mallocn(mi_heap_t* heap, size_t count, size_t size) mi_attr_noexcept mi_attr_malloc mi_attr_alloc_size2(2, 3);

mi_decl_export char* mi_heap_strdup(mi_heap_t* heap, const char* s) mi_attr_noexcept;
mi_decl_export char* mi_heap_strndup(mi_heap_t* heap, const char* s, size_t n) mi_attr_noexcept;
mi_decl_export char* mi_heap_realpath(mi_heap_t* heap, const char* fname, char* resolved_name) mi_attr_noexcept;


// ------------------------------------------------------
// Analysis
// ------------------------------------------------------

mi_decl_export bool mi_heap_contains_block(mi_heap_t* heap, const void* p);

mi_decl_export bool mi_heap_check_owned(mi_heap_t* heap, const void* p);
mi_decl_export bool mi_check_owned(const void* p);

// An area of heap space contains blocks of a single size.
typedef struct mi_heap_area_s {
  void*  blocks;      // start of the area containing heap blocks
  size_t reserved;    // bytes reserved for this area (virtual)
  size_t committed;   // current available bytes for this area
  size_t used;        // bytes in use by allocated blocks
  size_t block_size;  // size in bytes of each block
} mi_heap_area_t;

typedef bool (mi_block_visit_fun)(const mi_heap_t* heap, const mi_heap_area_t* area, void* block, size_t block_size, void* arg);

mi_decl_export bool mi_heap_visit_blocks(const mi_heap_t* heap, bool visit_all_blocks, mi_block_visit_fun* visitor, void* arg);

// ------------------------------------------------------
// Convenience
// ------------------------------------------------------

#define mi_malloc_tp(tp)        ((tp*)mi_malloc(sizeof(tp)))
#define mi_zalloc_tp(tp)        ((tp*)mi_zalloc(sizeof(tp)))
#define mi_calloc_tp(tp,n)      ((tp*)mi_calloc(n,sizeof(tp)))
#define mi_mallocn_tp(tp,n)     ((tp*)mi_mallocn(n,sizeof(tp)))

#define mi_reallocn_tp(p,tp,n)  ((tp*)mi_reallocn(p,n,sizeof(tp)))
#define mi_recalloc_tp(p,tp,n)  ((tp*)mi_recalloc(p,n,sizeof(tp)))

#define mi_heap_malloc_tp(hp,tp)        ((tp*)mi_heap_malloc(hp,sizeof(tp)))
#define mi_heap_zalloc_tp(hp,tp)        ((tp*)mi_heap_zalloc(hp,sizeof(tp)))
#define mi_heap_calloc_tp(hp,tp,n)      ((tp*)mi_heap_calloc(hp,n,sizeof(tp)))
#define mi_heap_mallocn_tp(hp,tp,n)     ((tp*)mi_heap_mallocn(hp,n,sizeof(tp)))


// ------------------------------------------------------
// Options, all `false` by default
// ------------------------------------------------------

typedef enum mi_option_e {
  mi_option_page_reset,
  mi_option_cache_reset,
  mi_option_pool_commit,
  mi_option_secure,
  mi_option_show_stats,
  mi_option_show_errors,
  mi_option_verbose,
  _mi_option_last
} mi_option_t;

mi_decl_export bool  mi_option_is_enabled(mi_option_t option);
mi_decl_export void  mi_option_enable(mi_option_t option, bool enable);
mi_decl_export void  mi_option_enable_default(mi_option_t option, bool enable);

mi_decl_export long  mi_option_get(mi_option_t option);
mi_decl_export void  mi_option_set(mi_option_t option, long value);
mi_decl_export void  mi_option_set_default(mi_option_t option, long value);

#ifdef __cplusplus
}
#endif

#endif
