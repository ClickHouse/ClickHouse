// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2014, Linaro
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
//
// Author: Riku Voipio, riku.voipio@linaro.org
//
// atomic primitives implemented with gcc atomic intrinsics:
// http://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html
//

#ifndef BASE_ATOMICOPS_INTERNALS_GCC_GENERIC_H_
#define BASE_ATOMICOPS_INTERNALS_GCC_GENERIC_H_

#include <stdio.h>
#include <stdlib.h>
#include "base/basictypes.h"

typedef int32_t Atomic32;

namespace base {
namespace subtle {

typedef int64_t Atomic64;

inline void MemoryBarrier() {
    __sync_synchronize();
}

inline Atomic32 NoBarrier_CompareAndSwap(volatile Atomic32* ptr,
                                         Atomic32 old_value,
                                         Atomic32 new_value) {
  Atomic32 prev_value = old_value;
  __atomic_compare_exchange_n(ptr, &prev_value, new_value, 
          0, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  return prev_value;
}

inline Atomic32 NoBarrier_AtomicExchange(volatile Atomic32* ptr,
                                         Atomic32 new_value) {
  return __atomic_exchange_n(const_cast<Atomic32*>(ptr), new_value, __ATOMIC_RELAXED);
}

inline Atomic32 Acquire_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value) {
  return __atomic_exchange_n(const_cast<Atomic32*>(ptr), new_value,  __ATOMIC_ACQUIRE);
}

inline Atomic32 Release_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value) {
  return __atomic_exchange_n(const_cast<Atomic32*>(ptr), new_value, __ATOMIC_RELEASE);
}

inline Atomic32 Acquire_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value) {
  Atomic32 prev_value = old_value;
  __atomic_compare_exchange_n(ptr, &prev_value, new_value, 
          0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
  return prev_value;
}

inline Atomic32 Release_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value) {
  Atomic32 prev_value = old_value;
  __atomic_compare_exchange_n(ptr, &prev_value, new_value, 
          0, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
  return prev_value;
}

inline void NoBarrier_Store(volatile Atomic32* ptr, Atomic32 value) {
  *ptr = value;
}

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
  *ptr = value;
  MemoryBarrier();
}

inline void Release_Store(volatile Atomic32* ptr, Atomic32 value) {
  MemoryBarrier();
  *ptr = value;
}

inline Atomic32 NoBarrier_Load(volatile const Atomic32* ptr) {
  return *ptr;
}

inline Atomic32 Acquire_Load(volatile const Atomic32* ptr) {
  Atomic32 value = *ptr;
  MemoryBarrier();
  return value;
}

inline Atomic32 Release_Load(volatile const Atomic32* ptr) {
  MemoryBarrier();
  return *ptr;
}

// 64-bit versions

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  Atomic64 prev_value = old_value;
  __atomic_compare_exchange_n(ptr, &prev_value, new_value, 
          0, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  return prev_value;
}

inline Atomic64 NoBarrier_AtomicExchange(volatile Atomic64* ptr,
                                         Atomic64 new_value) {
  return __atomic_exchange_n(const_cast<Atomic64*>(ptr), new_value, __ATOMIC_RELAXED);
}

inline Atomic64 Acquire_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  return __atomic_exchange_n(const_cast<Atomic64*>(ptr), new_value,  __ATOMIC_ACQUIRE);
}

inline Atomic64 Release_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  return __atomic_exchange_n(const_cast<Atomic64*>(ptr), new_value, __ATOMIC_RELEASE);
}

inline Atomic64 Acquire_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  Atomic64 prev_value = old_value;
  __atomic_compare_exchange_n(ptr, &prev_value, new_value, 
          0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
  return prev_value;
}

inline Atomic64 Release_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  Atomic64 prev_value = old_value;
  __atomic_compare_exchange_n(ptr, &prev_value, new_value, 
          0, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
  return prev_value;
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  *ptr = value;
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  *ptr = value;
  MemoryBarrier();
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  MemoryBarrier();
  *ptr = value;
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  return *ptr;
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  Atomic64 value = *ptr;
  MemoryBarrier();
  return value;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  MemoryBarrier();
  return *ptr;
}

}  // namespace base::subtle
}  // namespace base

#endif  // BASE_ATOMICOPS_INTERNALS_GCC_GENERIC_H_
