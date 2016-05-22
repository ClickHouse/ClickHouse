// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2005, Google Inc.
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
// Author: Sanjay Ghemawat
//
// This has the implementation details of malloc_hook that are needed
// to use malloc-hook inside the tcmalloc system.  It does not hold
// any of the client-facing calls that are used to add new hooks.

#ifndef _MALLOC_HOOK_INL_H_
#define _MALLOC_HOOK_INL_H_

#include <stddef.h>
#include <sys/types.h>
#include "base/atomicops.h"
#include "base/basictypes.h"
#include <gperftools/malloc_hook.h>

#include "common.h" // for UNLIKELY

namespace base { namespace internal {

// Capacity of 8 means that HookList is 9 words.
static const int kHookListCapacity = 8;
// last entry is reserved for deprecated "singular" hooks. So we have
// 7 "normal" hooks per list
static const int kHookListMaxValues = 7;
static const int kHookListSingularIdx = 7;

// HookList: a class that provides synchronized insertions and removals and
// lockless traversal.  Most of the implementation is in malloc_hook.cc.
template <typename T>
struct PERFTOOLS_DLL_DECL HookList {
  COMPILE_ASSERT(sizeof(T) <= sizeof(AtomicWord), T_should_fit_in_AtomicWord);

  // Adds value to the list.  Note that duplicates are allowed.  Thread-safe and
  // blocking (acquires hooklist_spinlock).  Returns true on success; false
  // otherwise (failures include invalid value and no space left).
  bool Add(T value);

  void FixupPrivEndLocked();

  // Removes the first entry matching value from the list.  Thread-safe and
  // blocking (acquires hooklist_spinlock).  Returns true on success; false
  // otherwise (failures include invalid value and no value found).
  bool Remove(T value);

  // Store up to n values of the list in output_array, and return the number of
  // elements stored.  Thread-safe and non-blocking.  This is fast (one memory
  // access) if the list is empty.
  int Traverse(T* output_array, int n) const;

  // Fast inline implementation for fast path of Invoke*Hook.
  bool empty() const {
    return base::subtle::NoBarrier_Load(&priv_end) == 0;
  }

  // Used purely to handle deprecated singular hooks
  T GetSingular() const {
    const AtomicWord *place = &priv_data[kHookListSingularIdx];
    return bit_cast<T>(base::subtle::NoBarrier_Load(place));
  }

  T ExchangeSingular(T new_val);

  // This internal data is not private so that the class is an aggregate and can
  // be initialized by the linker.  Don't access this directly.  Use the
  // INIT_HOOK_LIST macro in malloc_hook.cc.

  // One more than the index of the last valid element in priv_data.  During
  // 'Remove' this may be past the last valid element in priv_data, but
  // subsequent values will be 0.
  //
  // Index kHookListCapacity-1 is reserved as 'deprecated' single hook pointer
  AtomicWord priv_end;
  AtomicWord priv_data[kHookListCapacity];
};

ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::NewHook> new_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::DeleteHook> delete_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::PreMmapHook> premmap_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::MmapHook> mmap_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::MmapReplacement> mmap_replacement_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::MunmapHook> munmap_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::MunmapReplacement> munmap_replacement_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::MremapHook> mremap_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::PreSbrkHook> presbrk_hooks_;
ATTRIBUTE_VISIBILITY_HIDDEN extern HookList<MallocHook::SbrkHook> sbrk_hooks_;

} }  // namespace base::internal

// The following method is DEPRECATED
inline MallocHook::NewHook MallocHook::GetNewHook() {
  return base::internal::new_hooks_.GetSingular();
}

inline void MallocHook::InvokeNewHook(const void* p, size_t s) {
  if (UNLIKELY(!base::internal::new_hooks_.empty())) {
    InvokeNewHookSlow(p, s);
  }
}

// The following method is DEPRECATED
inline MallocHook::DeleteHook MallocHook::GetDeleteHook() {
  return base::internal::delete_hooks_.GetSingular();
}

inline void MallocHook::InvokeDeleteHook(const void* p) {
  if (UNLIKELY(!base::internal::delete_hooks_.empty())) {
    InvokeDeleteHookSlow(p);
  }
}

// The following method is DEPRECATED
inline MallocHook::PreMmapHook MallocHook::GetPreMmapHook() {
  return base::internal::premmap_hooks_.GetSingular();
}

inline void MallocHook::InvokePreMmapHook(const void* start,
                                          size_t size,
                                          int protection,
                                          int flags,
                                          int fd,
                                          off_t offset) {
  if (!base::internal::premmap_hooks_.empty()) {
    InvokePreMmapHookSlow(start, size, protection, flags, fd, offset);
  }
}

// The following method is DEPRECATED
inline MallocHook::MmapHook MallocHook::GetMmapHook() {
  return base::internal::mmap_hooks_.GetSingular();
}

inline void MallocHook::InvokeMmapHook(const void* result,
                                       const void* start,
                                       size_t size,
                                       int protection,
                                       int flags,
                                       int fd,
                                       off_t offset) {
  if (!base::internal::mmap_hooks_.empty()) {
    InvokeMmapHookSlow(result, start, size, protection, flags, fd, offset);
  }
}

inline bool MallocHook::InvokeMmapReplacement(const void* start,
                                              size_t size,
                                              int protection,
                                              int flags,
                                              int fd,
                                              off_t offset,
                                              void** result) {
  if (!base::internal::mmap_replacement_.empty()) {
    return InvokeMmapReplacementSlow(start, size,
                                     protection, flags,
                                     fd, offset,
                                     result);
  }
  return false;
}

// The following method is DEPRECATED
inline MallocHook::MunmapHook MallocHook::GetMunmapHook() {
  return base::internal::munmap_hooks_.GetSingular();
}

inline void MallocHook::InvokeMunmapHook(const void* p, size_t size) {
  if (!base::internal::munmap_hooks_.empty()) {
    InvokeMunmapHookSlow(p, size);
  }
}

inline bool MallocHook::InvokeMunmapReplacement(
    const void* p, size_t size, int* result) {
  if (!base::internal::mmap_replacement_.empty()) {
    return InvokeMunmapReplacementSlow(p, size, result);
  }
  return false;
}

// The following method is DEPRECATED
inline MallocHook::MremapHook MallocHook::GetMremapHook() {
  return base::internal::mremap_hooks_.GetSingular();
}

inline void MallocHook::InvokeMremapHook(const void* result,
                                         const void* old_addr,
                                         size_t old_size,
                                         size_t new_size,
                                         int flags,
                                         const void* new_addr) {
  if (!base::internal::mremap_hooks_.empty()) {
    InvokeMremapHookSlow(result, old_addr, old_size, new_size, flags, new_addr);
  }
}

// The following method is DEPRECATED
inline MallocHook::PreSbrkHook MallocHook::GetPreSbrkHook() {
  return base::internal::presbrk_hooks_.GetSingular();
}

inline void MallocHook::InvokePreSbrkHook(ptrdiff_t increment) {
  if (!base::internal::presbrk_hooks_.empty() && increment != 0) {
    InvokePreSbrkHookSlow(increment);
  }
}

// The following method is DEPRECATED
inline MallocHook::SbrkHook MallocHook::GetSbrkHook() {
  return base::internal::sbrk_hooks_.GetSingular();
}

inline void MallocHook::InvokeSbrkHook(const void* result,
                                       ptrdiff_t increment) {
  if (!base::internal::sbrk_hooks_.empty() && increment != 0) {
    InvokeSbrkHookSlow(result, increment);
  }
}

#endif /* _MALLOC_HOOK_INL_H_ */
