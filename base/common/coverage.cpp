#include <stdint.h>
#include <stdio.h>
#include "coverage.h"

void dumpCoverageReportIfPossible()  {}

// Use roaring bitmaps
// Use some way to pass test id without the foss

// This callback is inserted by the compiler as a module constructor
// into every DSO. 'start' and 'stop' correspond to the
// beginning and end of the section with the guards for the entire
// binary (executable or DSO). The callback will be called at least
// once per DSO and may be called multiple times with the same parameters.
extern "C" void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop) {
  static uint64_t n;  // Counter for the guards.

  if (start == stop || *start) return;  // Initialize only once.

  printf("INIT: %p %p\n",
          static_cast<void *>(start),
          static_cast<void *>(stop));

  // initialize the roaring data

  for (uint32_t *x = start; x < stop; x++)
    *x = ++n;  // Guards should start from 1.
}

// This callback is inserted by the compiler on every edge in the
// control flow (some optimizations apply).
// Typically, the compiler will emit the code like this:
//    if(*guard)
//      __sanitizer_cov_trace_pc_guard(guard);
// But for large functions it will emit a simple call:
//    __sanitizer_cov_trace_pc_guard(guard);
extern "C" void __sanitizer_cov_trace_pc_guard(uint32_t *edge_index) {
  if (!*edge_index) return;  // Duplicate the guard check.
  // If you set *guard to 0 this code will not be called again for this edge.
  // Now you can get the PC and do whatever you want:
  //   store it somewhere or symbolize it and print right away.
  // The values of `*guard` are as you set them in
  // __sanitizer_cov_trace_pc_guard_init and so you can make them consecutive
  // and use them to dereference an array or a bit vector.
  printf("guard: %p %x PC\n",
          static_cast<void*>(edge_index),
          *edge_index);
}
