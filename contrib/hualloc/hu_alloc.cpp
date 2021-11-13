#include "hu_alloc.h"

#include <string.h> // memset, memcpy

#ifndef NDEBUG
#define DBG_FILL_MEMORY
#endif

#ifdef DBG_FILL_MEMORY
static void *hu_alloc_dbg(size_t _nSize)
{
    void *res = hu_alloc(_nSize);
    memset(res, 0xcf, _nSize);
    return res;
}
#define ALLOC_FUNC hu_alloc_dbg
#else
#define ALLOC_FUNC hu_alloc
#endif

#ifdef _MSC_VER
void DisableWarningHuGetSize()
{
    hu_getsize(0);
}
#else
static void* SafeMalloc(size_t size)
{
    return ALLOC_FUNC(size);
}

extern "C" void* malloc(size_t size) {
    return SafeMalloc(size);
}

extern "C" void free(void* ptr) {
    hu_free(ptr);
}

extern "C" void* calloc(size_t n, size_t elem_size) {
    // Overflow check
    const size_t size = n * elem_size;
    if (elem_size != 0 && size / elem_size != n) return NULL;

    void* result = SafeMalloc(size);
    if (result != NULL) {
        memset(result, 0, size);
    }
    return result;
}

extern "C" void cfree(void* ptr) {
    hu_free(ptr);
}

extern "C" void* realloc(void* old_ptr, size_t new_size) {
    if (old_ptr == NULL) {
        void* result = SafeMalloc(new_size);
        return result;
    }
    if (new_size == 0) {
        hu_free(old_ptr);
        return NULL;
    }

    void* new_ptr = SafeMalloc(new_size);
    if (new_ptr == NULL) {
        return NULL;
    }
    size_t old_size = hu_getsize(old_ptr);
    memcpy(new_ptr, old_ptr, ((old_size < new_size) ? old_size : new_size));
    hu_free(old_ptr);
    return new_ptr;
}

extern "C" int posix_memalign(void** ptr, size_t align, size_t size) {
    Y_VERIFY(align <= 4096);

    *ptr = malloc(align > size ? align : size);

    return 0;
}

extern "C" size_t malloc_usable_size(void * ptr) {
    return hu_getsize(ptr);
}

#endif
