// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2013, Google Inc.
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

// This file defines structs to accumulate memory allocation and deallocation
// counts.  These structs are commonly used for malloc (in HeapProfileTable)
// and mmap (in MemoryRegionMap).

// A bucket is data structure for heap profiling to store a pair of a stack
// trace and counts of (de)allocation.  Buckets are stored in a hash table
// which is declared as "HeapProfileBucket**".
//
// A hash value is computed from a stack trace.  Collision in the hash table
// is resolved by separate chaining with linked lists.  The links in the list
// are implemented with the member "HeapProfileBucket* next".
//
// A structure of a hash table HeapProfileBucket** bucket_table would be like:
// bucket_table[0] => NULL
// bucket_table[1] => HeapProfileBucket() => HeapProfileBucket() => NULL
// ...
// bucket_table[i] => HeapProfileBucket() => NULL
// ...
// bucket_table[n] => HeapProfileBucket() => NULL

#ifndef HEAP_PROFILE_STATS_H_
#define HEAP_PROFILE_STATS_H_

struct HeapProfileStats {
  // Returns true if the two HeapProfileStats are semantically equal.
  bool Equivalent(const HeapProfileStats& other) const {
    return allocs - frees == other.allocs - other.frees &&
        alloc_size - free_size == other.alloc_size - other.free_size;
  }

  int32 allocs;      // Number of allocation calls.
  int32 frees;       // Number of free calls.
  int64 alloc_size;  // Total size of all allocated objects so far.
  int64 free_size;   // Total size of all freed objects so far.
};

// Allocation and deallocation statistics per each stack trace.
struct HeapProfileBucket : public HeapProfileStats {
  // Longest stack trace we record.
  static const int kMaxStackDepth = 32;

  uintptr_t hash;           // Hash value of the stack trace.
  int depth;                // Depth of stack trace.
  const void** stack;       // Stack trace.
  HeapProfileBucket* next;  // Next entry in hash-table.
};

#endif  // HEAP_PROFILE_STATS_H_
