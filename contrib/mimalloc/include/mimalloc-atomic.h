/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#pragma once
#ifndef __MIMALLOC_ATOMIC_H
#define __MIMALLOC_ATOMIC_H

// ------------------------------------------------------
// Atomics
// ------------------------------------------------------

// Atomically increment a value; returns the incremented result.
static inline uintptr_t mi_atomic_increment(volatile uintptr_t* p);

// Atomically increment a value; returns the incremented result.
static inline uint32_t mi_atomic_increment32(volatile uint32_t* p);

// Atomically decrement a value; returns the decremented result.
static inline uintptr_t mi_atomic_decrement(volatile uintptr_t* p);

// Atomically add a 64-bit value; returns the added result.
static inline int64_t mi_atomic_add(volatile int64_t* p, int64_t add);

// Atomically subtract a value; returns the subtracted result.
static inline uintptr_t mi_atomic_subtract(volatile uintptr_t* p, uintptr_t sub);

// Atomically subtract a value; returns the subtracted result.
static inline uint32_t mi_atomic_subtract32(volatile uint32_t* p, uint32_t sub);

// Atomically compare and exchange a value; returns `true` if successful.
static inline bool mi_atomic_compare_exchange32(volatile uint32_t* p, uint32_t exchange, uint32_t compare);

// Atomically compare and exchange a value; returns `true` if successful.
static inline bool mi_atomic_compare_exchange(volatile uintptr_t* p, uintptr_t exchange, uintptr_t compare);

// Atomically exchange a value.
static inline uintptr_t mi_atomic_exchange(volatile uintptr_t* p, uintptr_t exchange);

static inline void mi_atomic_yield();

// Atomically compare and exchange a pointer; returns `true` if successful.
static inline bool mi_atomic_compare_exchange_ptr(volatile void** p, void* newp, void* compare) {
  return mi_atomic_compare_exchange((volatile uintptr_t*)p, (uintptr_t)newp, (uintptr_t)compare);
}

// Atomically exchange a pointer value.
static inline void* mi_atomic_exchange_ptr(volatile void** p, void* exchange) {
  return (void*)mi_atomic_exchange((volatile uintptr_t*)p, (uintptr_t)exchange);
}



#define mi_atomic_locked(mutex) for(bool _mheld = mi_mutex_lock(mutex); _mheld; _mheld = mi_mutex_unlock(mutex))

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <intrin.h>
#if (MI_INTPTR_SIZE==8)
#define RC64(f)  f##64
#else
#define RC64(f)  f
#endif
static inline uintptr_t mi_atomic_increment(volatile uintptr_t* p) {
  return (uintptr_t)RC64(_InterlockedIncrement)((volatile intptr_t*)p);
}
static inline uint32_t mi_atomic_increment32(volatile uint32_t* p) {
  return (uint32_t)_InterlockedIncrement((volatile int32_t*)p);
}
static inline uintptr_t mi_atomic_decrement(volatile uintptr_t* p) {
  return (uintptr_t)RC64(_InterlockedDecrement)((volatile intptr_t*)p);
}
static inline uintptr_t mi_atomic_subtract(volatile uintptr_t* p, uintptr_t sub) {
  return (uintptr_t)RC64(_InterlockedExchangeAdd)((volatile intptr_t*)p, -((intptr_t)sub)) - sub;
}
static inline uint32_t mi_atomic_subtract32(volatile uint32_t* p, uint32_t sub) {
  return (uint32_t)_InterlockedExchangeAdd((volatile int32_t*)p, -((int32_t)sub)) - sub;
}
static inline bool mi_atomic_compare_exchange32(volatile uint32_t* p, uint32_t exchange, uint32_t compare) {
  return ((int32_t)compare == _InterlockedCompareExchange((volatile int32_t*)p, (int32_t)exchange, (int32_t)compare));
}
static inline bool mi_atomic_compare_exchange(volatile uintptr_t* p, uintptr_t exchange, uintptr_t compare) {
  return (compare == RC64(_InterlockedCompareExchange)((volatile intptr_t*)p, (intptr_t)exchange, (intptr_t)compare));
}
static inline uintptr_t mi_atomic_exchange(volatile uintptr_t* p, uintptr_t exchange) {
  return (uintptr_t)RC64(_InterlockedExchange)((volatile intptr_t*)p, (intptr_t)exchange);
}
static inline void mi_atomic_yield() {
  YieldProcessor();
}
static inline int64_t mi_atomic_add(volatile int64_t* p, int64_t add) {
  #if (MI_INTPTR_SIZE==8)
  return _InterlockedExchangeAdd64(p, add) + add;
  #else
  int64_t current;
  int64_t sum;
  do {
    current = *p;
    sum = current + add;
  } while (_InterlockedCompareExchange64(p, sum, current) != current);
  return sum;
  #endif
}

#else
#ifdef __cplusplus
#include <atomic>
#define  MI_USING_STD   using namespace std;
#define  _Atomic(tp)    atomic<tp>
#else
#include <stdatomic.h>
#define  MI_USING_STD
#endif
static inline uintptr_t mi_atomic_increment(volatile uintptr_t* p) {
  MI_USING_STD
  return atomic_fetch_add_explicit((volatile atomic_uintptr_t*)p, (uintptr_t)1, memory_order_relaxed) + 1;
}
static inline uint32_t mi_atomic_increment32(volatile uint32_t* p) {
  MI_USING_STD
  return atomic_fetch_add_explicit((volatile _Atomic(uint32_t)*)p, (uint32_t)1, memory_order_relaxed) + 1;
}
static inline uintptr_t mi_atomic_decrement(volatile uintptr_t* p) {
  MI_USING_STD
  return atomic_fetch_sub_explicit((volatile atomic_uintptr_t*)p, (uintptr_t)1, memory_order_relaxed) - 1;
}
static inline int64_t mi_atomic_add(volatile int64_t* p, int64_t add) {
  MI_USING_STD
  return atomic_fetch_add_explicit((volatile _Atomic(int64_t)*)p, add, memory_order_relaxed) + add;
}
static inline uintptr_t mi_atomic_subtract(volatile uintptr_t* p, uintptr_t sub) {
  MI_USING_STD
  return atomic_fetch_sub_explicit((volatile atomic_uintptr_t*)p, sub, memory_order_relaxed) - sub;
}
static inline uint32_t mi_atomic_subtract32(volatile uint32_t* p, uint32_t sub) {
  MI_USING_STD
  return atomic_fetch_sub_explicit((volatile _Atomic(uint32_t)*)p, sub, memory_order_relaxed) - sub;
}
static inline bool mi_atomic_compare_exchange32(volatile uint32_t* p, uint32_t exchange, uint32_t compare) {
  MI_USING_STD
  return atomic_compare_exchange_weak_explicit((volatile _Atomic(uint32_t)*)p, &compare, exchange, memory_order_relaxed, memory_order_seq_cst);
}
static inline bool mi_atomic_compare_exchange(volatile uintptr_t* p, uintptr_t exchange, uintptr_t compare) {
  MI_USING_STD
  return atomic_compare_exchange_weak_explicit((volatile atomic_uintptr_t*)p, &compare, exchange, memory_order_relaxed, memory_order_seq_cst);
}
static inline uintptr_t mi_atomic_exchange(volatile uintptr_t* p, uintptr_t exchange) {
  MI_USING_STD
  return atomic_exchange_explicit((volatile atomic_uintptr_t*)p, exchange, memory_order_relaxed);
}

#if defined(__cplusplus)
  #include <thread>
  static inline void mi_atomic_yield() {
    std::this_thread::yield();
  }
#elif (defined(__GNUC__) || defined(__clang__)) && \
      (defined(__x86_64__) || defined(__i386__) || defined(__arm__) || defined(__aarch64__))
#if defined(__x86_64__) || defined(__i386__)
  static inline void mi_atomic_yield() {
    asm volatile ("pause" ::: "memory");
  }
#elif defined(__arm__) || defined(__aarch64__)
  static inline void mi_atomic_yield() {
    asm volatile("yield");
  }
#endif
#else
  #include <unistd.h>
  static inline void mi_atomic_yield() {
    sleep(0);
  }
#endif

#endif

// Light weight mutex for low contention situations
typedef struct mi_mutex_s {
  volatile uint32_t value;
} mi_mutex_t;

static inline bool mi_mutex_lock(mi_mutex_t* mutex) {
  while(!mi_atomic_compare_exchange32(&mutex->value, 1, 0)) {
    mi_atomic_yield();
  }
  return true;
}

static inline bool mi_mutex_unlock(mi_mutex_t* mutex) {
  mutex->value = 0;
  return false;
}


#endif // __MIMALLOC_ATOMIC_H
