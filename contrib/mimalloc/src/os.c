/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE   // ensure mmap flags are defined
#endif

#include "mimalloc.h"
#include "mimalloc-internal.h"

#include <string.h>  // memset
#include <stdio.h>   // debug fprintf
#include <errno.h>

/* -----------------------------------------------------------
  Raw allocation on Windows (VirtualAlloc) and Unix's (mmap).
  Defines a portable `mmap`, `munmap` and `mmap_trim`.
----------------------------------------------------------- */

#if defined(_WIN32)
  #include <windows.h>
#else
  #include <sys/mman.h>  // mmap
  #include <unistd.h>    // sysconf
#endif


uintptr_t _mi_align_up(uintptr_t sz, size_t alignment) {
  uintptr_t x = (sz / alignment) * alignment;
  if (x < sz) x += alignment;
  if (x < sz) return 0; // overflow
  return x;
}

static void* mi_align_up_ptr(void* p, size_t alignment) {
  return (void*)_mi_align_up((uintptr_t)p, alignment);
}

static uintptr_t _mi_align_down(uintptr_t sz, size_t alignment) {
  return (sz / alignment) * alignment;
}

static void* mi_align_down_ptr(void* p, size_t alignment) {
  return (void*)_mi_align_down((uintptr_t)p, alignment);
}

static void* os_pool_alloc(size_t size, size_t alignment, mi_os_tld_t* tld);

// cached OS page size
size_t _mi_os_page_size() {
  static size_t page_size = 0;
  if (page_size == 0) {
#if defined(_WIN32)
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    page_size = (si.dwPageSize > 0 ? si.dwPageSize : 4096);
#else
    long result = sysconf(_SC_PAGESIZE);
    page_size = (result > 0 ? (size_t)result : 4096);
#endif
  }
  return page_size;
}


static bool mi_munmap(void* addr, size_t size)
{
  if (addr == NULL || size == 0) return true;
  bool err = false;
#if defined(_WIN32)
  err = (VirtualFree(addr, 0, MEM_RELEASE) == 0);
#else
  err = (munmap(addr, size) == -1);
#endif
  if (err) {
    #pragma warning(suppress:4996)
    _mi_warning_message("munmap failed: %s, addr 0x%8li, size %lu\n", strerror(errno), (size_t)addr, size);
    return false;
  }
  else {
    return true;
  }
}

static void* mi_mmap(void* addr, size_t size, int extra_flags, mi_stats_t* stats) {
  UNUSED(stats);
  if (size == 0) return NULL;
  void* p;
#if defined(_WIN32)
  p = VirtualAlloc(addr, size, MEM_RESERVE | MEM_COMMIT | extra_flags, PAGE_READWRITE);
#else
  #if !defined(MAP_ANONYMOUS)
  #define MAP_ANONYMOUS  MAP_ANON
  #endif
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | extra_flags;
  if (addr != NULL) {
    #if defined(MAP_EXCL)
      flags |= MAP_FIXED | MAP_EXCL;  // BSD
    #elif defined(MAP_FIXED_NOREPLACE)
      flags |= MAP_FIXED_NOREPLACE;   // Linux
    #elif defined(MAP_FIXED)
      flags |= MAP_FIXED;
    #endif
  }
  p = mmap(addr, size, (PROT_READ | PROT_WRITE), flags, -1, 0);
  if (p == MAP_FAILED) p = NULL;
  if (addr != NULL && p != addr) {
    mi_munmap(p, size);
    p = NULL;
  }
#endif
  mi_assert(p == NULL || (addr == NULL && p != addr) || (addr != NULL && p == addr));
  if (p != NULL) mi_stat_increase(stats->mmap_calls, 1);
  return p;
}


static void* mi_os_page_align_region(void* addr, size_t size, size_t* newsize) {
  mi_assert(addr != NULL && size > 0);
  if (newsize != NULL) *newsize = 0;
  if (size == 0 || addr == NULL) return NULL;

  // page align conservatively within the range
  void* start = mi_align_up_ptr(addr, _mi_os_page_size());
  void* end = mi_align_down_ptr((uint8_t*)addr + size, _mi_os_page_size());
  ptrdiff_t diff = (uint8_t*)end - (uint8_t*)start;
  if (diff <= 0) return NULL;

  mi_assert_internal((size_t)diff <= size);
  if (newsize != NULL) *newsize = (size_t)diff;
  return start;
}

// Signal to the OS that the address range is no longer in use
// but may be used later again. This will release physical memory
// pages and reduce swapping while keeping the memory committed.
// We page align to a conservative area inside the range to reset.
bool _mi_os_reset(void* addr, size_t size) {
  // page align conservatively within the range
  size_t csize;
  void* start = mi_os_page_align_region(addr,size,&csize);
  if (csize==0) return true;

#if defined(_WIN32)
  void* p = VirtualAlloc(start, csize, MEM_RESET, PAGE_READWRITE);
  mi_assert(p == start);
  return (p == start);
#else
  #if defined(MADV_FREE)
    static int advice = MADV_FREE;
    int err = madvise(start, csize, advice);
    if (err!=0 && errno==EINVAL && advice==MADV_FREE) {
      // if MADV_FREE is not supported, fall back to MADV_DONTNEED from now on
      advice = MADV_DONTNEED;
      err = madvise(start, csize, advice);
    }
  #else
    int err = madvise(start, csize, MADV_DONTNEED);
  #endif
  if (err != 0) {
    _mi_warning_message("madvise reset error: start: 0x%8p, csize: 0x%8zux, errno: %i\n", start, csize, errno);
  }
  //mi_assert(err == 0);
  return (err == 0);
#endif
}

// Protect a region in memory to be not accessible.
static  bool mi_os_protectx(void* addr, size_t size, bool protect) {
  // page align conservatively within the range
  size_t csize = 0;
  void* start = mi_os_page_align_region(addr, size, &csize);
  if (csize==0) return false;

  int err = 0;
#ifdef _WIN32
  DWORD oldprotect = 0;
  BOOL ok = VirtualProtect(start,csize,protect ? PAGE_NOACCESS : PAGE_READWRITE,&oldprotect);
  err = (ok ? 0 : -1);
#else
  err = mprotect(start,csize,protect ? PROT_NONE : (PROT_READ|PROT_WRITE));
#endif
  if (err != 0) {
    _mi_warning_message("mprotect error: start: 0x%8p, csize: 0x%8zux, errno: %i\n", start, csize, errno);
  }
  return (err==0);
}

bool _mi_os_protect(void* addr, size_t size) {
  return mi_os_protectx(addr,size,true);
}

bool _mi_os_unprotect(void* addr, size_t size) {
  return mi_os_protectx(addr, size, false);
}

bool _mi_os_shrink(void* p, size_t oldsize, size_t newsize) {
  // page align conservatively within the range
  mi_assert_internal(oldsize > newsize && p != NULL);
  if (oldsize < newsize || p==NULL) return false;
  if (oldsize == newsize) return true;

  // oldsize and newsize should be page aligned or we cannot shrink precisely
  void* addr = (uint8_t*)p + newsize;
  size_t size = 0;
  void* start = mi_os_page_align_region(addr, oldsize - newsize, &size);
  if (size==0 || start != addr) return false;

  #ifdef _WIN32
  // we cannot shrink on windows
  return false;
  #else
  return mi_munmap( start, size );
  #endif
}

/* -----------------------------------------------------------
  OS allocation using mmap/munmap
----------------------------------------------------------- */

void* _mi_os_alloc(size_t size, mi_stats_t* stats) {
  if (size == 0) return NULL;
  void* p = mi_mmap(NULL, size, 0, stats);
  mi_assert(p!=NULL);
  if (p != NULL) mi_stat_increase(stats->reserved, size);
  return p;
}

void  _mi_os_free(void* p, size_t size, mi_stats_t* stats) {
  UNUSED(stats);
  mi_munmap(p, size);
  mi_stat_decrease(stats->reserved, size);
}

// Slow but guaranteed way to allocated aligned memory
// by over-allocating and then reallocating at a fixed aligned
// address that should be available then.
static void* mi_os_alloc_aligned_ensured(size_t size, size_t alignment, size_t trie, mi_stats_t* stats)
{
  if (trie >= 3) return NULL; // stop recursion (only on Windows)
  size_t alloc_size = size + alignment;
  mi_assert(alloc_size >= size); // overflow?
  if (alloc_size < size) return NULL;

  // allocate a chunk that includes the alignment
  void* p = mi_mmap(NULL, alloc_size, 0, stats);
  if (p == NULL) return NULL;
  // create an aligned pointer in the allocated area
  void* aligned_p = mi_align_up_ptr(p, alignment);
  mi_assert(aligned_p != NULL);
#if defined(_WIN32)
  // free it and try to allocate `size` at exactly `aligned_p`
  // note: this may fail in case another thread happens to VirtualAlloc
  // concurrently at that spot. We try up to 3 times to mitigate this.
  mi_munmap(p, alloc_size);
  p = mi_mmap(aligned_p, size, 0, stats);
  if (p != aligned_p) {
    if (p != NULL) mi_munmap(p, size);
    return mi_os_alloc_aligned_ensured(size, alignment, trie++, stats);
  }
#else
  // we selectively unmap parts around the over-allocated area.
  size_t pre_size = (uint8_t*)aligned_p - (uint8_t*)p;
  size_t mid_size = _mi_align_up(size, _mi_os_page_size());
  size_t post_size = alloc_size - pre_size - mid_size;
  if (pre_size > 0)  mi_munmap(p, pre_size);
  if (post_size > 0) mi_munmap((uint8_t*)aligned_p + mid_size, post_size);
#endif

  mi_assert(((uintptr_t)aligned_p) % alignment == 0);
  return aligned_p;
}

// Allocate an aligned block.
// Since `mi_mmap` is relatively slow we try to allocate directly at first and
// hope to get an aligned address; only when that fails we fall back
// to a guaranteed method by overallocating at first and adjusting.
// TODO: use VirtualAlloc2 with alignment on Windows 10 / Windows Server 2016.
void* _mi_os_alloc_aligned(size_t size, size_t alignment, mi_os_tld_t* tld)
{
  if (size == 0) return NULL;
  if (alignment < 1024) return _mi_os_alloc(size, tld->stats);

  void* p = os_pool_alloc(size,alignment,tld);
  if (p != NULL) return p;

  void* suggest = NULL;

#if defined(MAP_ALIGNED)
  // on BSD, use the aligned mmap api
  size_t n = _mi_bsr(alignment);
  if ((size_t)1 << n == alignment && n >= 12) {  // alignment is a power of 2 and >= 4096
    p = mi_mmap(suggest, size, MAP_ALIGNED(n), tld->stats);     // use the NetBSD/freeBSD aligned flags
  }
#endif
  if (p==NULL && (tld->mmap_next_probable % alignment) == 0) {
    // if the next probable address is aligned,
    // then try to just allocate `size` and hope it is aligned...
    p = mi_mmap(suggest, size, 0, tld->stats);
    if (p == NULL) return NULL;
    if (((uintptr_t)p % alignment) == 0) mi_stat_increase(tld->stats->mmap_right_align, 1);
  }
  //fprintf(stderr, "segment address guess: %s, p=%lxu, guess:%lxu\n", (p != NULL && (uintptr_t)p % alignment ==0 ? "correct" : "incorrect"), (uintptr_t)p, next_probable);

  if (p==NULL || ((uintptr_t)p % alignment) != 0) {
    // if `p` is not yet aligned after all, free the block and use a slower
    // but guaranteed way to allocate an aligned block
    if (p != NULL) mi_munmap(p, size);
    mi_stat_increase( tld->stats->mmap_ensure_aligned, 1);
    //fprintf(stderr, "mimalloc: slow mmap 0x%lx\n", _mi_thread_id());
    p = mi_os_alloc_aligned_ensured(size, alignment,0,tld->stats);
  }
  if (p != NULL) {
    mi_stat_increase( tld->stats->reserved, size);

    // next probable address is the page-aligned address just after the newly allocated area.
    const size_t alloc_align =
#if defined(_WIN32)
      64 * 1024; // Windows allocates 64kb aligned
#else
      _mi_os_page_size(); // page size on other OS's
#endif
    size_t probable_size = MI_SEGMENT_SIZE;
    if (tld->mmap_previous > p) {
      // Linux tends to allocate downward
      tld->mmap_next_probable = _mi_align_down((uintptr_t)p - probable_size, alloc_align); // ((uintptr_t)previous - (uintptr_t)p);
    }
    else {
      // Otherwise, guess the next address is page aligned `size` from current pointer
      tld->mmap_next_probable = _mi_align_up((uintptr_t)p + probable_size, alloc_align);
    }
    tld->mmap_previous = p;
  }
  return p;
}

// Pooled allocation: on 64-bit systems with plenty
// of virtual addresses, we allocate 10 segments at the
// time to minimize `mmap` calls and increase aligned
// allocations. This is only good on systems that
// do overcommit so we put it behind the `MIMALLOC_POOL_COMMIT` option.
// For now, we disable it on windows as VirtualFree must
// be called on the original allocation and cannot be called
// for individual fragments.
#if defined(_WIN32) || (MI_INTPTR_SIZE<8)

static void* os_pool_alloc(size_t size, size_t alignment, mi_os_tld_t* tld) {
  UNUSED(size);
  UNUSED(alignment);
  UNUSED(tld);
  return NULL;
}

#else

#define MI_POOL_ALIGNMENT   MI_SEGMENT_SIZE
#define MI_POOL_SIZE        (10*MI_POOL_ALIGNMENT)

static void* os_pool_alloc(size_t size, size_t alignment, mi_os_tld_t* tld)
{
  if (!mi_option_is_enabled(mi_option_pool_commit)) return NULL;
  if (alignment != MI_POOL_ALIGNMENT) return NULL;
  size = _mi_align_up(size,MI_POOL_ALIGNMENT);
  if (size > MI_POOL_SIZE) return NULL;

  if (tld->pool_available == 0) {
    tld->pool = (uint8_t*)mi_os_alloc_aligned_ensured(MI_POOL_SIZE,MI_POOL_ALIGNMENT,0,tld->stats);
    if (tld->pool == NULL) return NULL;
    tld->pool_available += MI_POOL_SIZE;
  }

  if (size > tld->pool_available) return NULL;
  void* p = tld->pool;
  tld->pool_available -= size;
  tld->pool += size;
  return p;
}

#endif
