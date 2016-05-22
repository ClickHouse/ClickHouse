// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
/* Copyright (c) 2006, Google Inc.
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
 *
 * ---
 * Author: Sanjay Ghemawat
 */

#include "../config.h"
#include "base/spinlock.h"
#include "base/spinlock_internal.h"
#include "base/sysinfo.h"   /* for GetSystemCPUsCount() */

// NOTE on the Lock-state values:
//
// kSpinLockFree represents the unlocked state
// kSpinLockHeld represents the locked state with no waiters
// kSpinLockSleeper represents the locked state with waiters

static int adaptive_spin_count = 0;

const base::LinkerInitialized SpinLock::LINKER_INITIALIZED =
    base::LINKER_INITIALIZED;

namespace {
struct SpinLock_InitHelper {
  SpinLock_InitHelper() {
    // On multi-cpu machines, spin for longer before yielding
    // the processor or sleeping.  Reduces idle time significantly.
    if (GetSystemCPUsCount() > 1) {
      adaptive_spin_count = 1000;
    }
  }
};

// Hook into global constructor execution:
// We do not do adaptive spinning before that,
// but nothing lock-intensive should be going on at that time.
static SpinLock_InitHelper init_helper;

inline void SpinlockPause(void) {
#if defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))
  __asm__ __volatile__("rep; nop" : : );
#endif
}

}  // unnamed namespace

// Monitor the lock to see if its value changes within some time
// period (adaptive_spin_count loop iterations). The last value read
// from the lock is returned from the method.
Atomic32 SpinLock::SpinLoop() {
  int c = adaptive_spin_count;
  while (base::subtle::NoBarrier_Load(&lockword_) != kSpinLockFree && --c > 0) {
    SpinlockPause();
  }
  return base::subtle::Acquire_CompareAndSwap(&lockword_, kSpinLockFree,
                                              kSpinLockSleeper);
}

void SpinLock::SlowLock() {
  Atomic32 lock_value = SpinLoop();

  int lock_wait_call_count = 0;
  while (lock_value != kSpinLockFree) {
    // If the lock is currently held, but not marked as having a sleeper, mark
    // it as having a sleeper.
    if (lock_value == kSpinLockHeld) {
      // Here, just "mark" that the thread is going to sleep.  Don't store the
      // lock wait time in the lock as that will cause the current lock
      // owner to think it experienced contention.
      lock_value = base::subtle::Acquire_CompareAndSwap(&lockword_,
                                                        kSpinLockHeld,
                                                        kSpinLockSleeper);
      if (lock_value == kSpinLockHeld) {
        // Successfully transitioned to kSpinLockSleeper.  Pass
        // kSpinLockSleeper to the SpinLockDelay routine to properly indicate
        // the last lock_value observed.
        lock_value = kSpinLockSleeper;
      } else if (lock_value == kSpinLockFree) {
        // Lock is free again, so try and acquire it before sleeping.  The
        // new lock state will be the number of cycles this thread waited if
        // this thread obtains the lock.
        lock_value = base::subtle::Acquire_CompareAndSwap(&lockword_,
                                                          kSpinLockFree,
                                                          kSpinLockSleeper);
        continue;  // skip the delay at the end of the loop
      }
    }

    // Wait for an OS specific delay.
    base::internal::SpinLockDelay(&lockword_, lock_value,
                                  ++lock_wait_call_count);
    // Spin again after returning from the wait routine to give this thread
    // some chance of obtaining the lock.
    lock_value = SpinLoop();
  }
}

void SpinLock::SlowUnlock() {
  // wake waiter if necessary
  base::internal::SpinLockWake(&lockword_, false);
}
