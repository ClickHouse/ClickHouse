/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"license.txt" at the root of this distribution.
-----------------------------------------------------------------------------*/

#if !defined(MI_IN_ALLOC_C)
#error "this file should be included from 'alloc.c' (so aliases can work)"
#endif

#if defined(MI_MALLOC_OVERRIDE) && defined(_WIN32) && !(defined(MI_SHARED_LIB) && defined(_DLL))
#error "It is only possible to override malloc on Windows when building as a DLL (and linking the C runtime as a DLL)"
#endif

#if defined(MI_MALLOC_OVERRIDE) && !defined(_WIN32)

// ------------------------------------------------------
// Override system malloc
// ------------------------------------------------------

#if defined(_MSC_VER)
#pragma warning(disable:4273)  // inconsistent dll linking
#endif

#if (defined(__GNUC__) || defined(__clang__)) && !defined(__MACH__)
  // use aliasing to alias the exported function to one of our `mi_` functions
  #define MI_FORWARD(fun)      __attribute__((alias(#fun), used));
  #define MI_FORWARD1(fun,x)   MI_FORWARD(fun)
  #define MI_FORWARD2(fun,x,y) MI_FORWARD(fun)
  #define MI_FORWARD0(fun,x)   MI_FORWARD(fun)
#else
  // use forwarding by calling our `mi_` function
  #define MI_FORWARD1(fun,x)   { return fun(x); }
  #define MI_FORWARD2(fun,x,y) { return fun(x,y); }
  #define MI_FORWARD0(fun,x)   { fun(x); }
#endif

#if defined(__APPLE__) && defined(MI_SHARED_LIB_EXPORT) && defined(MI_INTERPOSE)
  // use interposing so `DYLD_INSERT_LIBRARIES` works without `DYLD_FORCE_FLAT_NAMESPACE=1`
  // See: <https://books.google.com/books?id=K8vUkpOXhN4C&pg=PA73>
  struct mi_interpose_s {
    const void* replacement;
    const void* target;
  };
  #define MI_INTERPOSEX(oldfun,newfun)  { (const void*)&newfun, (const void*)&oldfun }
  #define MI_INTERPOSE_MI(fun)         MI_INTERPOSEX(fun,mi_##fun)
  __attribute__((used)) static struct mi_interpose_s _mi_interposes[]  __attribute__((section("__DATA, __interpose"))) =
  {
    MI_INTERPOSE_MI(malloc),
    MI_INTERPOSE_MI(calloc),
    MI_INTERPOSE_MI(realloc),
    MI_INTERPOSE_MI(free)
  };
#else
  // On all other systems forward to our API
  void* malloc(size_t size)              mi_attr_noexcept  MI_FORWARD1(mi_malloc, size)
  void* calloc(size_t size, size_t n)    mi_attr_noexcept  MI_FORWARD2(mi_calloc, size, n)
  void* realloc(void* p, size_t newsize) mi_attr_noexcept  MI_FORWARD2(mi_realloc, p, newsize)
  void  free(void* p)                    mi_attr_noexcept  MI_FORWARD0(mi_free, p)
#endif

// ------------------------------------------------------
// Override new/delete
// This is not really necessary as they usually call
// malloc/free anyway, but it improves performance.
// ------------------------------------------------------
#ifdef __cplusplus
  // ------------------------------------------------------
  // With a C++ compiler we override the new/delete operators.
  // see <https://en.cppreference.com/w/cpp/memory/new/operator_new>
  // ------------------------------------------------------
  #include <new>
  void operator delete(void* p) noexcept              MI_FORWARD0(mi_free,p)
  void operator delete[](void* p) noexcept            MI_FORWARD0(mi_free,p)
  void* operator new(std::size_t n) noexcept(false)   MI_FORWARD1(mi_malloc,n)
  void* operator new[](std::size_t n) noexcept(false) MI_FORWARD1(mi_malloc,n)

  #if (__cplusplus >= 201703L)
  void* operator new( std::size_t n, std::align_val_t align) noexcept(false)   MI_FORWARD2(mi_malloc_aligned,n,align)
  void* operator new[]( std::size_t n, std::align_val_t align) noexcept(false) MI_FORWARD2(mi_malloc_aligned,n,align)
  #endif
#else
  // ------------------------------------------------------
  // With a C compiler we override the new/delete operators
  // by defining the mangled C++ names of the operators (as
  // used by GCC and CLang).
  // See <https://itanium-cxx-abi.github.io/cxx-abi/abi.html#mangling>
  // ------------------------------------------------------
  void _ZdlPv(void* p) MI_FORWARD0(mi_free,p) // delete
  void _ZdaPv(void* p) MI_FORWARD0(mi_free,p) // delete[]
  #if (MI_INTPTR_SIZE==8)
    void* _Znwm(uint64_t n)                  MI_FORWARD1(mi_malloc,n)               // new 64-bit
    void* _Znam(uint64_t n)                  MI_FORWARD1(mi_malloc,n)               // new[] 64-bit
    void* _Znwmm(uint64_t n, uint64_t align) { return mi_malloc_aligned(n,align); } // aligned new 64-bit
    void* _Znamm(uint64_t n, uint64_t align) { return mi_malloc_aligned(n,align); }  // aligned new[] 64-bit
  #elif (MI_INTPTR_SIZE==4)
    void* _Znwj(uint32_t n)                  MI_FORWARD1(mi_malloc,n)               // new 32-bit
    void* _Znaj(uint32_t n)                  MI_FORWARD1(mi_malloc,n)               // new[] 32-bit
    void* _Znwjj(uint32_t n, uint32_t align) { return mi_malloc_aligned(n,align); }  // aligned new 32-bit
    void* _Znajj(uint32_t n, uint32_t align) { return mi_malloc_aligned(n,align); }  // aligned new[] 32-bit
  #else
  #error "define overloads for new/delete for this platform (just for performance, can be skipped)"
  #endif
#endif // __cplusplus


#ifdef __cplusplus
extern "C" {
#endif

// ------------------------------------------------------
// Posix & Unix functions definitions
// ------------------------------------------------------

#include <errno.h>

#ifndef EINVAL
#define EINVAL 22
#endif
#ifndef ENOMEM
#define ENOMEM 12
#endif

void*  reallocf(void* p, size_t newsize) MI_FORWARD2(mi_reallocf,p,newsize)
size_t malloc_size(void* p)              MI_FORWARD1(mi_usable_size,p)
size_t malloc_usable_size(void *p)       MI_FORWARD1(mi_usable_size,p)
void   cfree(void* p)                    MI_FORWARD0(mi_free, p)

int posix_memalign(void** p, size_t alignment, size_t size) {
  if (alignment % sizeof(void*) != 0) { *p = NULL; return EINVAL; };
  *p = mi_malloc_aligned(size, alignment);
  return (*p == NULL ? ENOMEM : 0);
}

void* memalign(size_t alignment, size_t size) {
  return mi_malloc_aligned(size, alignment);
}

void* valloc(size_t size) {
  return mi_malloc_aligned(size, _mi_os_page_size());
}

void* pvalloc(size_t size) {
  size_t psize = _mi_os_page_size();
  if (size >= SIZE_MAX - psize) return NULL; // overflow
  size_t asize = ((size + psize - 1) / psize) * psize;
  return mi_malloc_aligned(asize, psize);
}

void* aligned_alloc(size_t alignment, size_t size) {
  return mi_malloc_aligned(size, alignment);
}

void* reallocarray( void* p, size_t count, size_t size ) {  // BSD
  void* newp = mi_reallocn(p,count,size);
  if (newp==NULL) errno = ENOMEM;
  return newp;
}

#if defined(__GLIBC__) && defined(__linux__)
  // forward __libc interface (needed for redhat linux)
  void* __libc_malloc(size_t size)                  MI_FORWARD1(mi_malloc,size)
  void* __libc_calloc(size_t count, size_t size)    MI_FORWARD2(mi_calloc,count,size)
  void* __libc_realloc(void* p, size_t size)        MI_FORWARD2(mi_realloc,p,size)
  void  __libc_free(void* p)                        MI_FORWARD0(mi_free,p)
  void  __libc_cfree(void* p)                       MI_FORWARD0(mi_free,p)

  void* __libc_memalign(size_t alignment, size_t size)  {
    return memalign(alignment,size);
  }
  void* __libc_valloc(size_t size) {
    return valloc(size);
  }
  void* __libc_pvalloc(size_t size) {
    return pvalloc(size);
  }
  int __posix_memalign(void** p, size_t alignment, size_t size)  {
    return posix_memalign(p,alignment,size);
  }
#endif

#ifdef __cplusplus
}
#endif

#endif // MI_MALLOC_OVERRIDE & !_WIN32
