// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
/* Copyright (c) 2013, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

// Author: Jovan Zelincevic <jovan.zelincevic@imgtec.com>
// based on atomicops-internals by Sanjay Ghemawat

// This file is an internal atomic implementation, use base/atomicops.h instead.
//
// This code implements MIPS atomics.

#ifndef BASE_ATOMICOPS_INTERNALS_MIPS_H_
#define BASE_ATOMICOPS_INTERNALS_MIPS_H_

#if (_MIPS_ISA == _MIPS_ISA_MIPS64)
#define BASE_HAS_ATOMIC64 1
#endif

typedef int32_t Atomic32;

namespace base {
namespace subtle {

// Atomically execute:
// result = *ptr;
// if (*ptr == old_value)
// *ptr = new_value;
// return result;
//
// I.e., replace "*ptr" with "new_value" if "*ptr" used to be "old_value".
// Always return the old value of "*ptr"
//
// This routine implies no memory barriers.
inline Atomic32 NoBarrier_CompareAndSwap(volatile Atomic32* ptr,
                                         Atomic32 old_value,
                                         Atomic32 new_value)
{
    Atomic32 prev, tmp;
    __asm__ volatile(
        ".set   push                \n"
        ".set   noreorder           \n"

    "1:                             \n"
        "ll     %0,     %5          \n" // prev = *ptr
        "bne    %0,     %3,     2f  \n" // if (prev != old_value) goto 2
        " move  %2,     %4          \n" // tmp = new_value
        "sc     %2,     %1          \n" // *ptr = tmp (with atomic check)
        "beqz   %2,     1b          \n" // start again on atomic error
        " nop                       \n" // delay slot nop
    "2:                             \n"

        ".set   pop                 \n"
        : "=&r" (prev), "=m" (*ptr),
          "=&r" (tmp)
        : "Ir" (old_value), "r" (new_value),
          "m" (*ptr)
        : "memory"
    );
    return prev;
}

// Atomically store new_value into *ptr, returning the previous value held in
// *ptr. This routine implies no memory barriers.
inline Atomic32 NoBarrier_AtomicExchange(volatile Atomic32* ptr,
                                         Atomic32 new_value)
{
    Atomic32 temp, old;
    __asm__ volatile(
        ".set   push                \n"
        ".set   noreorder           \n"

    "1:                             \n"
        "ll     %1,     %2          \n" // old = *ptr
        "move   %0,     %3          \n" // temp = new_value
        "sc     %0,     %2          \n" // *ptr = temp (with atomic check)
        "beqz   %0,     1b          \n" // start again on atomic error
        " nop                       \n" // delay slot nop

        ".set   pop                 \n"
        : "=&r" (temp), "=&r" (old),
          "=m" (*ptr)
        : "r" (new_value), "m" (*ptr)
        : "memory"
    );
    return old;
}

inline void MemoryBarrier()
{
    __asm__ volatile("sync" : : : "memory");
}

// "Acquire" operations
// ensure that no later memory access can be reordered ahead of the operation.
// "Release" operations ensure that no previous memory access can be reordered
// after the operation. "Barrier" operations have both "Acquire" and "Release"
// semantics. A MemoryBarrier() has "Barrier" semantics, but does no memory
// access.
inline Atomic32 Acquire_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value)
{
    Atomic32 res = NoBarrier_CompareAndSwap(ptr, old_value, new_value);
    MemoryBarrier();
    return res;
}

inline Atomic32 Release_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value)
{
    MemoryBarrier();
    Atomic32 res = NoBarrier_CompareAndSwap(ptr, old_value, new_value);
    return res;
}

inline void NoBarrier_Store(volatile Atomic32* ptr, Atomic32 value)
{
    *ptr = value;
}

inline Atomic32 Acquire_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value)
{
    Atomic32 old_value = NoBarrier_AtomicExchange(ptr, new_value);
    MemoryBarrier();
    return old_value;
}

inline Atomic32 Release_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value)
{
    MemoryBarrier();
    return NoBarrier_AtomicExchange(ptr, new_value);
}

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value)
{
    *ptr = value;
    MemoryBarrier();
}

inline void Release_Store(volatile Atomic32* ptr, Atomic32 value)
{
    MemoryBarrier();
    *ptr = value;
}

inline Atomic32 NoBarrier_Load(volatile const Atomic32* ptr)
{
    return *ptr;
}

inline Atomic32 Acquire_Load(volatile const Atomic32* ptr)
{
    Atomic32 value = *ptr;
    MemoryBarrier();
    return value;
}

inline Atomic32 Release_Load(volatile const Atomic32* ptr)
{
    MemoryBarrier();
    return *ptr;
}

#if (_MIPS_ISA == _MIPS_ISA_MIPS64) || (_MIPS_SIM == _MIPS_SIM_ABI64)

typedef int64_t Atomic64;

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value)
{
    Atomic64 prev, tmp;
    __asm__ volatile(
        ".set   push                \n"
        ".set   noreorder           \n"

    "1:                             \n"
        "lld    %0,     %5          \n" // prev = *ptr
        "bne    %0,     %3,     2f  \n" // if (prev != old_value) goto 2
        " move  %2,     %4          \n" // tmp = new_value
        "scd    %2,     %1          \n" // *ptr = tmp (with atomic check)
        "beqz   %2,     1b          \n" // start again on atomic error
        " nop                       \n" // delay slot nop
    "2:                             \n"

        ".set   pop                 \n"
        : "=&r" (prev), "=m" (*ptr),
          "=&r" (tmp)
        : "Ir" (old_value), "r" (new_value),
          "m" (*ptr)
        : "memory"
    );
    return prev;
}

inline Atomic64 NoBarrier_AtomicExchange(volatile Atomic64* ptr,
                                         Atomic64 new_value)
{
    Atomic64 temp, old;
    __asm__ volatile(
        ".set   push                \n"
        ".set   noreorder           \n"

    "1:                             \n"
        "lld    %1,     %2          \n" // old = *ptr
        "move   %0,     %3          \n" // temp = new_value
        "scd    %0,     %2          \n" // *ptr = temp (with atomic check)
        "beqz   %0,     1b          \n" // start again on atomic error
        " nop                       \n" // delay slot nop

        ".set   pop                 \n"
        : "=&r" (temp), "=&r" (old),
          "=m" (*ptr)
        : "r" (new_value), "m" (*ptr)
        : "memory"
    );
    return old;
}

inline Atomic64 Acquire_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value)
{
    Atomic64 old_value = NoBarrier_AtomicExchange(ptr, new_value);
    MemoryBarrier();
    return old_value;
}

inline Atomic64 Acquire_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value)
{
    Atomic64 res = NoBarrier_CompareAndSwap(ptr, old_value, new_value);
    MemoryBarrier();
    return res;
}

inline Atomic64 Release_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value)
{
    MemoryBarrier();
    Atomic64 res = NoBarrier_CompareAndSwap(ptr, old_value, new_value);
    return res;
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value)
{
    *ptr = value;
}

inline Atomic64 Release_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value)
{
    MemoryBarrier();
    return NoBarrier_AtomicExchange(ptr, new_value);
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value)
{
    *ptr = value;
    MemoryBarrier();
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value)
{
    MemoryBarrier();
    *ptr = value;
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr)
{
    return *ptr;
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr)
{
    Atomic64 value = *ptr;
    MemoryBarrier();
    return value;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr)
{
    MemoryBarrier();
    return *ptr;
}

#endif

}   // namespace base::subtle
}   // namespace base

#endif  // BASE_ATOMICOPS_INTERNALS_MIPS_H_
