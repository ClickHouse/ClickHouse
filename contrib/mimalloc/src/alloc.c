/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#include "mimalloc.h"
#include "mimalloc-internal.h"
#include "mimalloc-atomic.h"

#include <string.h>  // memset

#define MI_IN_ALLOC_C
#include "alloc-override.c"
#undef MI_IN_ALLOC_C

// ------------------------------------------------------
// Allocation
// ------------------------------------------------------

// Fast allocation in a page: just pop from the free list.
// Fall back to generic allocation only if the list is empty.
extern inline void* _mi_page_malloc(mi_heap_t* heap, mi_page_t* page, size_t size) mi_attr_noexcept {
  mi_assert_internal(page->block_size==0||page->block_size >= size);
  mi_block_t* block = page->free;
  if (mi_unlikely(block == NULL)) {
    return _mi_malloc_generic(heap, size); // slow path
  }
  mi_assert_internal(block != NULL && _mi_ptr_page(block) == page);
  // pop from the free list
  page->free = mi_block_next(page,block);
  page->used++;
  mi_assert_internal(page->free == NULL || _mi_ptr_page(page->free) == page);
#if (MI_DEBUG)
  memset(block, MI_DEBUG_UNINIT, size);
#elif (MI_SECURE)
  block->next = 0;
#endif
#if (MI_STAT>1)
  if(size <= MI_LARGE_SIZE_MAX) mi_heap_stat_increase(heap,normal[_mi_bin(size)], 1);
#endif
  return block;
}

// allocate a small block
extern inline void* mi_heap_malloc_small(mi_heap_t* heap, size_t size) mi_attr_noexcept {
  mi_assert(size <= MI_SMALL_SIZE_MAX);
  mi_page_t* page = _mi_heap_get_free_small_page(heap,size);
  return _mi_page_malloc(heap, page, size);
}

extern inline void* mi_malloc_small(size_t size) mi_attr_noexcept {
  return mi_heap_malloc_small(mi_get_default_heap(), size);
}

// zero initialized small block
void* mi_zalloc_small(size_t size) mi_attr_noexcept {
  void* p = mi_malloc_small(size);
  if (p != NULL) { memset(p, 0, size); }
  return p;
}

// The main allocation function
extern inline void* mi_heap_malloc(mi_heap_t* heap, size_t size) mi_attr_noexcept {
  mi_assert(heap!=NULL);
  mi_assert(heap->thread_id == 0 || heap->thread_id == _mi_thread_id()); // heaps are thread local
  void* p;
  if (mi_likely(size <= MI_SMALL_SIZE_MAX)) {
    p = mi_heap_malloc_small(heap, size);
  }
  else {
    p = _mi_malloc_generic(heap, size);
  }
  #if MI_STAT>1
  if (p != NULL) {
    if (!mi_heap_is_initialized(heap)) { heap = mi_get_default_heap(); }
    mi_heap_stat_increase( heap, malloc, mi_good_size(size) );  // overestimate for aligned sizes
  }
  #endif
  return p;
}

extern inline void* mi_malloc(size_t size) mi_attr_noexcept {
  return mi_heap_malloc(mi_get_default_heap(), size);
}

void* _mi_heap_malloc_zero(mi_heap_t* heap, size_t size, bool zero) {
  void* p = mi_heap_malloc(heap,size);
  if (zero && p != NULL) memset(p,0,size);
  return p;
}

extern inline void* mi_heap_zalloc(mi_heap_t* heap, size_t size) mi_attr_noexcept {
  return _mi_heap_malloc_zero(heap, size, true);
}

void* mi_zalloc(size_t size) mi_attr_noexcept {
  return mi_heap_zalloc(mi_get_default_heap(),size);
}


// ------------------------------------------------------
// Free
// ------------------------------------------------------

// multi-threaded free
static mi_decl_noinline void _mi_free_block_mt(mi_page_t* page, mi_block_t* block)
{
  mi_thread_free_t tfree;
  mi_thread_free_t tfreex;
  bool use_delayed;

  do {
    tfreex = tfree = page->thread_free;
    use_delayed = (tfree.delayed == MI_USE_DELAYED_FREE);
    if (mi_unlikely(use_delayed)) {
      // unlikely: this only happens on the first concurrent free in a page that is in the full list
      tfreex.delayed = MI_DELAYED_FREEING;
    }
    else {
      // usual: directly add to page thread_free list
      mi_block_set_next(page, block, (mi_block_t*)((uintptr_t)tfree.head << MI_TF_PTR_SHIFT));
      tfreex.head = (uintptr_t)block >> MI_TF_PTR_SHIFT;
    }
  } while (!mi_atomic_compare_exchange((volatile uintptr_t*)&page->thread_free, tfreex.value, tfree.value));

  if (mi_likely(!use_delayed)) {
    // increment the thread free count and return
    mi_atomic_increment(&page->thread_freed);
  }
  else {
    // racy read on `heap`, but ok because MI_DELAYED_FREEING is set (see `mi_heap_delete` and `mi_heap_collect_abandon`)
    mi_heap_t* heap = page->heap;
    mi_assert_internal(heap != NULL);
    if (heap != NULL) {
      // add to the delayed free list of this heap. (do this atomically as the lock only protects heap memory validity)
      mi_block_t* dfree;
      do {
        dfree = (mi_block_t*)heap->thread_delayed_free;
        mi_block_set_nextx(heap->cookie,block,dfree);
      } while (!mi_atomic_compare_exchange_ptr((volatile void**)&heap->thread_delayed_free, block, dfree));
    }

    // and reset the MI_DELAYED_FREEING flag
    do {
      tfreex = tfree = page->thread_free;
      tfreex.delayed = MI_NO_DELAYED_FREE;
    } while (!mi_atomic_compare_exchange((volatile uintptr_t*)&page->thread_free, tfreex.value, tfree.value));
  }
}


// regular free
static inline void _mi_free_block(mi_page_t* page, bool local, mi_block_t* block)
{
  #if (MI_DEBUG)
  memset(block, MI_DEBUG_FREED, page->block_size);
  #endif

  // and push it on the free list
  if (mi_likely(local)) {
    // owning thread can free a block directly
    mi_block_set_next(page, block, page->local_free);
    page->local_free = block;
    page->used--;
    if (mi_unlikely(mi_page_all_free(page))) {
      _mi_page_retire(page);
    }
    else if (mi_unlikely(page->flags.in_full)) {
      _mi_page_unfull(page);
    }
  }
  else {
    _mi_free_block_mt(page,block);
  }
}


// Adjust a block that was allocated aligned, to the actual start of the block in the page.
mi_block_t* _mi_page_ptr_unalign(const mi_segment_t* segment, const mi_page_t* page, void* p) {
  mi_assert_internal(page!=NULL && p!=NULL);
  size_t diff   = (uint8_t*)p - _mi_page_start(segment, page, NULL);
  size_t adjust = (diff % page->block_size);
  return (mi_block_t*)((uintptr_t)p - adjust);
}


static void mi_decl_noinline mi_free_generic(const mi_segment_t* segment, mi_page_t* page, bool local, void* p) {
  mi_block_t* block = (page->flags.has_aligned ? _mi_page_ptr_unalign(segment, page, p) : (mi_block_t*)p);
  _mi_free_block(page, local, block);
}

// Free a block
void mi_free(void* p) mi_attr_noexcept
{
  // optimize: merge null check with the segment masking (below)
  //if (p == NULL) return;

#if (MI_DEBUG>0)
  if (mi_unlikely(((uintptr_t)p & (MI_INTPTR_SIZE - 1)) != 0)) {
    _mi_error_message("trying to free an invalid (unaligned) pointer: %p\n", p);
    return;
  }
#endif

  const mi_segment_t* const segment = _mi_ptr_segment(p);
  if (segment == NULL) return;  // checks for (p==NULL)
  bool local = (_mi_thread_id() == segment->thread_id);  // preload, note: putting the thread_id in the page->flags does not improve performance

#if (MI_DEBUG>0)
  if (mi_unlikely(_mi_ptr_cookie(segment) != segment->cookie)) {
    _mi_error_message("trying to mi_free a pointer that does not point to a valid heap space: %p\n", p);
    return;
  }
#endif

  mi_page_t* page = _mi_segment_page_of(segment, p);

#if (MI_STAT>1)
  mi_heap_t* heap = mi_heap_get_default();
  mi_heap_stat_decrease( heap, malloc, mi_usable_size(p));
  if (page->block_size <= MI_LARGE_SIZE_MAX) {
    mi_heap_stat_decrease( heap, normal[_mi_bin(page->block_size)], 1);
  }
  // huge page stat is accounted for in `_mi_page_retire`
#endif

  // adjust if it might be an un-aligned block
  if (mi_likely(page->flags.value==0)) {  // note: merging both tests (local | value) does not matter for performance
    mi_block_t* block = (mi_block_t*)p;
    if (mi_likely(local)) {
      // owning thread can free a block directly
      mi_block_set_next(page, block, page->local_free);  // note: moving this write earlier does not matter for performance
      page->local_free = block;
      page->used--;
      if (mi_unlikely(mi_page_all_free(page))) { _mi_page_retire(page); }
    }
    else {
      // use atomic operations for a multi-threaded free
      _mi_free_block_mt(page, block);
    }
  }
  else {
    // aligned blocks, or a full page; use the more generic path
    mi_free_generic(segment, page, local, p);
  }
}

void _mi_free_delayed_block(mi_block_t* block) {
  mi_assert_internal(block != NULL);
  const mi_segment_t* segment = _mi_ptr_segment(block);
  mi_assert_internal(_mi_ptr_cookie(segment) == segment->cookie);
  mi_assert_internal(_mi_thread_id() == segment->thread_id);
  mi_page_t* page = _mi_segment_page_of(segment,block);
  _mi_free_block(page,true,block);
}

// Bytes available in a block
size_t mi_usable_size(void* p) mi_attr_noexcept {
  if (p==NULL) return 0;
  const mi_segment_t* segment = _mi_ptr_segment(p);
  const mi_page_t* page = _mi_segment_page_of(segment,p);
  size_t size = page->block_size;
  if (mi_unlikely(page->flags.has_aligned)) {
    ptrdiff_t adjust = (uint8_t*)p - (uint8_t*)_mi_page_ptr_unalign(segment,page,p);
    mi_assert_internal(adjust >= 0 && (size_t)adjust <= size);
    return (size - adjust);
  }
  else {
    return size;
  }
}





// ------------------------------------------------------
// ensure explicit external inline definitions are emitted!
// ------------------------------------------------------

#ifdef __cplusplus
void* _mi_externs[] = {
  (void*)&_mi_page_malloc,
  (void*)&mi_malloc_small,
  (void*)&mi_malloc,
};


#endif


// ------------------------------------------------------
// Allocation extensions
// ------------------------------------------------------

extern inline void* mi_heap_calloc(mi_heap_t* heap, size_t count, size_t size) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_heap_zalloc(heap,total);
}

void* mi_calloc(size_t count, size_t size) mi_attr_noexcept {
  return mi_heap_calloc(mi_get_default_heap(),count,size);
}

// Uninitialized `calloc`
extern void* mi_heap_mallocn(mi_heap_t* heap, size_t count, size_t size) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_heap_malloc(heap, total);
}

void* mi_mallocn(size_t count, size_t size) mi_attr_noexcept {
  return mi_heap_mallocn(mi_get_default_heap(),count,size);
}

// Expand in place or fail
void* mi_expand(void* p, size_t newsize) mi_attr_noexcept {
  if (p == NULL) return NULL;
  size_t size = mi_usable_size(p);
  if (newsize > size) return NULL;
  return p; // it fits
}

void* _mi_realloc_zero(void* p, size_t newsize, bool zero) {
  if (p == NULL) return _mi_heap_malloc_zero(mi_get_default_heap(),newsize,zero);
  size_t size = mi_usable_size(p);
  if (newsize <= size && newsize >= (size / 2)) {
    return p;  // reallocation still fits and not more than 50% waste
  }
  void* newp = mi_malloc(newsize); // maybe in another heap
  if (mi_likely(newp != NULL)) {
    if (zero && newsize > size) {
      // also set last word in the previous allocation to zero to ensure any padding is zero-initialized
      size_t start = (size >= sizeof(intptr_t) ? size - sizeof(intptr_t) : 0);
      memset((uint8_t*)newp + start, 0, newsize - start);
    }
    memcpy(newp, p, (newsize > size ? size : newsize));
    mi_free(p); // only free if successful
  }
  return newp;
}

void* mi_realloc(void* p, size_t newsize) mi_attr_noexcept {
  return _mi_realloc_zero(p,newsize,false);
}

// Zero initialized reallocation
void* mi_rezalloc(void* p, size_t newsize) mi_attr_noexcept {
  return _mi_realloc_zero(p,newsize,true);
}

void* mi_recalloc(void* p, size_t count, size_t size) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_rezalloc(p,total);
}

void* mi_reallocn(void* p, size_t count, size_t size) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_realloc(p,total);
}

// Reallocate but free `p` on errors
void* mi_reallocf(void* p, size_t newsize) mi_attr_noexcept {
  void* newp = mi_realloc(p,newsize);
  if (newp==NULL && p!=NULL) mi_free(p);
  return newp;
}

// `strdup` using mi_malloc
char* mi_heap_strdup(mi_heap_t* heap, const char* s) mi_attr_noexcept {
  if (s == NULL) return NULL;
  size_t n = strlen(s);
  char* t = (char*)mi_heap_malloc(heap,n+1);
  if (t != NULL) memcpy(t, s, n + 1);
  return t;
}

char* mi_strdup(const char* s) mi_attr_noexcept {
  return mi_heap_strdup(mi_get_default_heap(), s);
}

// `strndup` using mi_malloc
char* mi_heap_strndup(mi_heap_t* heap, const char* s, size_t n) mi_attr_noexcept {
  if (s == NULL) return NULL;
  size_t m = strlen(s);
  if (n > m) n = m;
  char* t = (char*)mi_heap_malloc(heap, n+1);
  if (t == NULL) return NULL;
  memcpy(t, s, n);
  t[n] = 0;
  return t;
}

char* mi_strndup(const char* s, size_t n) mi_attr_noexcept {
  return mi_heap_strndup(mi_get_default_heap(),s,n);
}

// `realpath` using mi_malloc
#ifdef _WIN32
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif
#include <windows.h>
#include <errno.h>
char* mi_heap_realpath(mi_heap_t* heap, const char* fname, char* resolved_name) mi_attr_noexcept {
  // todo: use GetFullPathNameW to allow longer file names
  char buf[PATH_MAX];
  DWORD res = GetFullPathNameA(fname, PATH_MAX, (resolved_name == NULL ? buf : resolved_name), NULL);
  if (res == 0) {
    errno = GetLastError(); return NULL;
  }
  else if (res > PATH_MAX) {
    errno = EINVAL; return NULL;
  }
  else if (resolved_name != NULL) {
    return resolved_name;
  }
  else {
    return mi_heap_strndup(heap, buf, PATH_MAX);
  }
}
#else
#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX 260
#endif

char* mi_heap_realpath(mi_heap_t* heap, const char* fname, char* resolved_name) mi_attr_noexcept {
  if (resolved_name != NULL) {
    return realpath(fname,resolved_name);
  }
  else {
    char buf[PATH_MAX+1];
    char* rname = realpath(fname,buf);
    return mi_heap_strndup(heap,rname,PATH_MAX); // ok if `rname==NULL`
  }
}
#endif

char* mi_realpath(const char* fname, char* resolved_name) mi_attr_noexcept {
  return mi_heap_realpath(mi_get_default_heap(),fname,resolved_name);
}
