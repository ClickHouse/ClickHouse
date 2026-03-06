#pragma once

#include <base/defines.h>
#include <cstddef>
#include <netdb.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"

// NOLINTBEGIN

#if defined(SANITIZER) || defined(SANITIZE_COVERAGE) || defined(OS_DARWIN) || defined(OS_FREEBSD) || defined(OS_SUNOS)

#define __real_malloc(size) ::malloc(size)
#define __real_calloc(nmemb, size) ::calloc(nmemb, size)
#define __real_realloc(ptr, size) ::realloc(ptr, size)
#define __real_posix_memalign(memptr, alignment, size) ::posix_memalign(memptr, alignment, size)
#define __real_aligned_alloc(alignment, size) ::aligned_alloc(alignment, size)
#define __real_valloc(size) ::valloc(size)
#define __real_free ::free

#if !defined(OS_DARWIN)
#define __real_memalign(alignment, size) ::memalign(alignment, size)
#endif

#if !defined(USE_MUSL) && defined(OS_LINUX)
#define __real_pvalloc(size) ::pvalloc(size)
#endif

#define __real_getaddrinfo(node, service, hints, result) ::getaddrinfo(node, service, hints, result)
#define __real_freeaddrinfo(result) ::freeaddrinfo(result)

#else

extern "C" void * __real_malloc(size_t size);
extern "C" void * __real_calloc(size_t nmemb, size_t size);
extern "C" void * __real_realloc(void * ptr, size_t size);
extern "C" int    __real_posix_memalign(void ** memptr, size_t alignment, size_t size);
extern "C" void * __real_aligned_alloc(size_t alignment, size_t size);
extern "C" void * __real_valloc(size_t size);
extern "C" void * __real_memalign(size_t alignment, size_t size);
extern "C" void   __real_free(void * ptr);
#if !defined(USE_MUSL) && defined(OS_LINUX)
extern "C" void * __real_pvalloc(size_t size);
#endif

extern "C" int __real_getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** result);
extern "C" void __real_freeaddrinfo(struct addrinfo * result);
extern "C" char * __real_strdup(const char * str);
extern "C" char * __real_strndup(const char * str, size_t size);

#endif

// NOLINTEND

#pragma clang diagnostic pop
