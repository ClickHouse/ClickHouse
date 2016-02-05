// Copyright (c) 2010, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---

#ifndef UTIL_GTL_LIBC_ALLOCATOR_WITH_REALLOC_H_
#define UTIL_GTL_LIBC_ALLOCATOR_WITH_REALLOC_H_

#include <sparsehash/internal/sparseconfig.h>
#include <stdlib.h>           // for malloc/realloc/free
#include <stddef.h>           // for ptrdiff_t
#include <new>                // for placement new

_START_GOOGLE_NAMESPACE_

template<class T>
class libc_allocator_with_realloc {
 public:
  typedef T value_type;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;

  typedef T* pointer;
  typedef const T* const_pointer;
  typedef T& reference;
  typedef const T& const_reference;

  libc_allocator_with_realloc() {}
  libc_allocator_with_realloc(const libc_allocator_with_realloc&) {}
  ~libc_allocator_with_realloc() {}

  pointer address(reference r) const  { return &r; }
  const_pointer address(const_reference r) const  { return &r; }

  pointer allocate(size_type n, const_pointer = 0) {
    return static_cast<pointer>(malloc(n * sizeof(value_type)));
  }
  void deallocate(pointer p, size_type) {
    free(p);
  }
  pointer reallocate(pointer p, size_type n) {
    return static_cast<pointer>(realloc(p, n * sizeof(value_type)));
  }

  size_type max_size() const  {
    return static_cast<size_type>(-1) / sizeof(value_type);
  }

  void construct(pointer p, const value_type& val) {
    new(p) value_type(val);
  }
  void destroy(pointer p) { p->~value_type(); }

  template <class U>
  libc_allocator_with_realloc(const libc_allocator_with_realloc<U>&) {}

  template<class U>
  struct rebind {
    typedef libc_allocator_with_realloc<U> other;
  };
};

// libc_allocator_with_realloc<void> specialization.
template<>
class libc_allocator_with_realloc<void> {
 public:
  typedef void value_type;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef void* pointer;
  typedef const void* const_pointer;

  template<class U>
  struct rebind {
    typedef libc_allocator_with_realloc<U> other;
  };
};

template<class T>
inline bool operator==(const libc_allocator_with_realloc<T>&,
                       const libc_allocator_with_realloc<T>&) {
  return true;
}

template<class T>
inline bool operator!=(const libc_allocator_with_realloc<T>&,
                       const libc_allocator_with_realloc<T>&) {
  return false;
}

_END_GOOGLE_NAMESPACE_

#endif  // UTIL_GTL_LIBC_ALLOCATOR_WITH_REALLOC_H_
