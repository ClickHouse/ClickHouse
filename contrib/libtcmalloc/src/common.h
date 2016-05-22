// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2008, Google Inc.
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
// Author: Sanjay Ghemawat <opensource@google.com>
//
// Common definitions for tcmalloc code.

#ifndef TCMALLOC_COMMON_H_
#define TCMALLOC_COMMON_H_

#include "config.h"
#include <stddef.h>                     // for size_t
#ifdef HAVE_STDINT_H
#include <stdint.h>                     // for uintptr_t, uint64_t
#endif
#include "internal_logging.h"  // for ASSERT, etc
#include "base/basictypes.h"   // for LIKELY, etc

#ifdef HAVE_BUILTIN_EXPECT
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

// Type that can hold a page number
typedef uintptr_t PageID;

// Type that can hold the length of a run of pages
typedef uintptr_t Length;

//-------------------------------------------------------------------
// Configuration
//-------------------------------------------------------------------

#if defined(TCMALLOC_ALIGN_8BYTES)
// Unless we force to use 8 bytes alignment we use an alignment of
// at least 16 bytes to statisfy requirements for some SSE types.
// Keep in mind when using the 16 bytes alignment you can have a space
// waste due alignment of 25%. (eg malloc of 24 bytes will get 32 bytes)
static const size_t kMinAlign   = 8;
// Number of classes created until reach page size 128.
static const size_t kBaseClasses = 16;
#else
static const size_t kMinAlign   = 16;
static const size_t kBaseClasses = 9;
#endif

// Using large pages speeds up the execution at a cost of larger memory use.
// Deallocation may speed up by a factor as the page map gets 8x smaller, so
// lookups in the page map result in fewer L2 cache misses, which translates to
// speedup for application/platform combinations with high L2 cache pressure.
// As the number of size classes increases with large pages, we increase
// the thread cache allowance to avoid passing more free ranges to and from
// central lists.  Also, larger pages are less likely to get freed.
// These two factors cause a bounded increase in memory use.
#if defined(TCMALLOC_32K_PAGES)
static const size_t kPageShift  = 15;
static const size_t kNumClasses = kBaseClasses + 69;
#elif defined(TCMALLOC_64K_PAGES)
static const size_t kPageShift  = 16;
static const size_t kNumClasses = kBaseClasses + 73;
#else
static const size_t kPageShift  = 13;
static const size_t kNumClasses = kBaseClasses + 79;
#endif

static const size_t kMaxThreadCacheSize = 4 << 20;

static const size_t kPageSize   = 1 << kPageShift;
static const size_t kMaxSize    = 256 * 1024;
static const size_t kAlignment  = 8;
static const size_t kLargeSizeClass = 0;
// For all span-lengths < kMaxPages we keep an exact-size list.
static const size_t kMaxPages = 1 << (20 - kPageShift);

// Default bound on the total amount of thread caches.
#ifdef TCMALLOC_SMALL_BUT_SLOW
// Make the overall thread cache no bigger than that of a single thread
// for the small memory footprint case.
static const size_t kDefaultOverallThreadCacheSize = kMaxThreadCacheSize;
#else
static const size_t kDefaultOverallThreadCacheSize = 8u * kMaxThreadCacheSize;
#endif

// Lower bound on the per-thread cache sizes
static const size_t kMinThreadCacheSize = kMaxSize * 2;

// The number of bytes one ThreadCache will steal from another when
// the first ThreadCache is forced to Scavenge(), delaying the
// next call to Scavenge for this thread.
static const size_t kStealAmount = 1 << 16;

// The number of times that a deallocation can cause a freelist to
// go over its max_length() before shrinking max_length().
static const int kMaxOverages = 3;

// Maximum length we allow a per-thread free-list to have before we
// move objects from it into the corresponding central free-list.  We
// want this big to avoid locking the central free-list too often.  It
// should not hurt to make this list somewhat big because the
// scavenging code will shrink it down when its contents are not in use.
static const int kMaxDynamicFreeListLength = 8192;

static const Length kMaxValidPages = (~static_cast<Length>(0)) >> kPageShift;

#if defined __x86_64__
// All current and planned x86_64 processors only look at the lower 48 bits
// in virtual to physical address translation.  The top 16 are thus unused.
// TODO(rus): Under what operating systems can we increase it safely to 17?
// This lets us use smaller page maps.  On first allocation, a 36-bit page map
// uses only 96 KB instead of the 4.5 MB used by a 52-bit page map.
static const int kAddressBits = (sizeof(void*) < 8 ? (8 * sizeof(void*)) : 48);
#else
static const int kAddressBits = 8 * sizeof(void*);
#endif

namespace tcmalloc {

// Convert byte size into pages.  This won't overflow, but may return
// an unreasonably large value if bytes is huge enough.
inline Length pages(size_t bytes) {
  return (bytes >> kPageShift) +
      ((bytes & (kPageSize - 1)) > 0 ? 1 : 0);
}

// For larger allocation sizes, we use larger memory alignments to
// reduce the number of size classes.
int AlignmentForSize(size_t size);

// Size-class information + mapping
class SizeMap {
 private:
  // Number of objects to move between a per-thread list and a central
  // list in one shot.  We want this to be not too small so we can
  // amortize the lock overhead for accessing the central list.  Making
  // it too big may temporarily cause unnecessary memory wastage in the
  // per-thread free list until the scavenger cleans up the list.
  int num_objects_to_move_[kNumClasses];

  //-------------------------------------------------------------------
  // Mapping from size to size_class and vice versa
  //-------------------------------------------------------------------

  // Sizes <= 1024 have an alignment >= 8.  So for such sizes we have an
  // array indexed by ceil(size/8).  Sizes > 1024 have an alignment >= 128.
  // So for these larger sizes we have an array indexed by ceil(size/128).
  //
  // We flatten both logical arrays into one physical array and use
  // arithmetic to compute an appropriate index.  The constants used by
  // ClassIndex() were selected to make the flattening work.
  //
  // Examples:
  //   Size       Expression                      Index
  //   -------------------------------------------------------
  //   0          (0 + 7) / 8                     0
  //   1          (1 + 7) / 8                     1
  //   ...
  //   1024       (1024 + 7) / 8                  128
  //   1025       (1025 + 127 + (120<<7)) / 128   129
  //   ...
  //   32768      (32768 + 127 + (120<<7)) / 128  376
  static const int kMaxSmallSize = 1024;
  static const size_t kClassArraySize =
      ((kMaxSize + 127 + (120 << 7)) >> 7) + 1;
  unsigned char class_array_[kClassArraySize];

  static inline size_t SmallSizeClass(size_t s) {
    return (static_cast<uint32_t>(s) + 7) >> 3;
  }

  static inline size_t LargeSizeClass(size_t s) {
    return (static_cast<uint32_t>(s) + 127 + (120 << 7)) >> 7;
  }

  // Compute index of the class_array[] entry for a given size
  static inline size_t ClassIndex(size_t s) {
    // Use unsigned arithmetic to avoid unnecessary sign extensions.
    ASSERT(0 <= s);
    ASSERT(s <= kMaxSize);
    if (LIKELY(s <= kMaxSmallSize)) {
      return SmallSizeClass(s);
    } else {
      return LargeSizeClass(s);
    }
  }

  int NumMoveSize(size_t size);

  // Mapping from size class to max size storable in that class
  size_t class_to_size_[kNumClasses];

  // Mapping from size class to number of pages to allocate at a time
  size_t class_to_pages_[kNumClasses];

 public:
  // Constructor should do nothing since we rely on explicit Init()
  // call, which may or may not be called before the constructor runs.
  SizeMap() { }

  // Initialize the mapping arrays
  void Init();

  inline int SizeClass(size_t size) {
    return class_array_[ClassIndex(size)];
  }

  inline bool MaybeSizeClass(size_t size, size_t *size_class) {
    size_t class_idx;
    if (LIKELY(size <= kMaxSmallSize)) {
      class_idx = SmallSizeClass(size);
    } else if (size <= kMaxSize) {
      class_idx = LargeSizeClass(size);
    } else {
      return false;
    }
    *size_class = class_array_[class_idx];
    return true;
  }

  // Get the byte-size for a specified class
  inline size_t ByteSizeForClass(size_t cl) {
    return class_to_size_[cl];
  }

  // Mapping from size class to max size storable in that class
  inline size_t class_to_size(size_t cl) {
    return class_to_size_[cl];
  }

  // Mapping from size class to number of pages to allocate at a time
  inline size_t class_to_pages(size_t cl) {
    return class_to_pages_[cl];
  }

  // Number of objects to move between a per-thread list and a central
  // list in one shot.  We want this to be not too small so we can
  // amortize the lock overhead for accessing the central list.  Making
  // it too big may temporarily cause unnecessary memory wastage in the
  // per-thread free list until the scavenger cleans up the list.
  inline int num_objects_to_move(size_t cl) {
    return num_objects_to_move_[cl];
  }
};

// Allocates "bytes" worth of memory and returns it.  Increments
// metadata_system_bytes appropriately.  May return NULL if allocation
// fails.  Requires pageheap_lock is held.
void* MetaDataAlloc(size_t bytes);

// Returns the total number of bytes allocated from the system.
// Requires pageheap_lock is held.
uint64_t metadata_system_bytes();

// size/depth are made the same size as a pointer so that some generic
// code below can conveniently cast them back and forth to void*.
static const int kMaxStackDepth = 31;
struct StackTrace {
  uintptr_t size;          // Size of object
  uintptr_t depth;         // Number of PC values stored in array below
  void*     stack[kMaxStackDepth];
};

}  // namespace tcmalloc

#endif  // TCMALLOC_COMMON_H_
