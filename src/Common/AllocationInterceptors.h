#pragma once

#include "config.h"

#include <cstddef>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"

// NOLINTBEGIN

/// These macros provide direct access to the underlying allocator,
/// bypassing the tracking wrappers in malloc.cpp.
/// Use them in code that does its own memory tracking (e.g. Allocator, AllocatorWithMemoryTracking)
/// to avoid double-counting.

#if USE_JEMALLOC

#include <jemalloc/jemalloc.h>

#define __real_malloc(size) je_malloc(size)
#define __real_calloc(nmemb, size) je_calloc(nmemb, size)
#define __real_realloc(ptr, size) je_realloc(ptr, size)
#define __real_posix_memalign(memptr, alignment, size) je_posix_memalign(memptr, alignment, size)
#define __real_aligned_alloc(alignment, size) je_aligned_alloc(alignment, size)
#define __real_free je_free

#else

#define __real_malloc(size) ::malloc(size)
#define __real_calloc(nmemb, size) ::calloc(nmemb, size)
#define __real_realloc(ptr, size) ::realloc(ptr, size)
#define __real_posix_memalign(memptr, alignment, size) ::posix_memalign(memptr, alignment, size)
#define __real_aligned_alloc(alignment, size) ::aligned_alloc(alignment, size)
#define __real_free ::free

#endif

// NOLINTEND

#pragma clang diagnostic pop
