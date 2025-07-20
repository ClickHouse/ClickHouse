#pragma once

#include <cstddef>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"

// NOLINTBEGIN

extern "C" void * __real_malloc(size_t size);
extern "C" void * __real_calloc(size_t nmemb, size_t size);
extern "C" void * __real_realloc(void * ptr, size_t size);
extern "C" int    __real_posix_memalign(void ** memptr, size_t alignment, size_t size);
extern "C" void * __real_aligned_alloc(size_t alignment, size_t size);
extern "C" void * __real_valloc(size_t size);
extern "C" void * __real_memalign(size_t alignment, size_t size);

extern "C" void   __real_free(void * ptr);
extern "C" void   __real_sdallocx(void * ptr, size_t size, int flags);

// NOLINTEND

#pragma clang diagnostic pop
