#include "hu_alloc.h"

#ifndef NDEBUG
#define DBG_FILL_MEMORY
#endif

#ifdef DBG_FILL_MEMORY
static void* hu_alloc_dbg(size_t _nSize)
{
    void *res = hu_alloc(_nSize);
    memset(res, 0xcf, _nSize);
    return res;
}
#define ALLOC_FUNC hu_alloc_dbg
#else
#define ALLOC_FUNC hu_alloc
#endif

inline void* hu_alloc_aligned(size_t size, size_t align) {
    if (align > PAGE_SIZE) {
        abort();
    }
    return ALLOC_FUNC(align > size ? align : size);
}

inline void hu_free_aligned(void *p, size_t align) {
    (void)align;
    hu_free(p);
}

//////////////////////////////////////////////////////////////////////////
// hooks
#if defined(USE_INTELCC) || defined(_darwin_) || defined(_freebsd_) || defined(_STLPORT_VERSION)
#define OP_THROWNOTHING throw ()
#define OP_THROWBADALLOC throw (std::bad_alloc)
#else
#define OP_THROWNOTHING noexcept
#define OP_THROWBADALLOC
#endif

void* operator new(size_t size) OP_THROWBADALLOC {
    return ALLOC_FUNC(size);
}

void* operator new(size_t size, const std::nothrow_t&) OP_THROWNOTHING {
    return ALLOC_FUNC(size);
}

void operator delete(void* p) OP_THROWNOTHING {
    hu_free(p);
}

void operator delete(void* p, const std::nothrow_t&) OP_THROWNOTHING {
    hu_free(p);
}

void* operator new[](size_t size) OP_THROWBADALLOC {
    return ALLOC_FUNC(size);
}

void* operator new[](size_t size, const std::nothrow_t&) OP_THROWNOTHING {
    return ALLOC_FUNC(size);
}

void operator delete[](void* p) OP_THROWNOTHING {
    hu_free(p);
}

void operator delete[](void* p, const std::nothrow_t&) OP_THROWNOTHING {
    hu_free(p);
}

#if (__cplusplus >= 201402L || _MSC_VER >= 1916)
void operator delete  (void* p, size_t n) OP_THROWNOTHING {
    (void)n;
    hu_free(p);
}
void operator delete[](void* p, size_t n) OP_THROWNOTHING {
    (void)n;
    hu_free(p);
}
#endif

#if (__cplusplus > 201402L && defined(__cpp_aligned_new)) && (!defined(__GNUC__) || (__GNUC__ > 5))
void operator delete  (void* p, std::align_val_t al) OP_THROWNOTHING { hu_free_aligned(p, static_cast<size_t>(al)); }
void operator delete[](void* p, std::align_val_t al) OP_THROWNOTHING { hu_free_aligned(p, static_cast<size_t>(al)); }
void operator delete  (void* p, size_t n, std::align_val_t al) OP_THROWNOTHING { (void)n; hu_free_aligned(p, static_cast<size_t>(al)); };
void operator delete[](void* p, size_t n, std::align_val_t al) OP_THROWNOTHING { (void)n; hu_free_aligned(p, static_cast<size_t>(al)); };

void* operator new(size_t n, std::align_val_t al) OP_THROWBADALLOC { return hu_alloc_aligned(n, static_cast<size_t>(al)); }
void* operator new[](size_t n, std::align_val_t al) OP_THROWBADALLOC { return hu_alloc_aligned(n, static_cast<size_t>(al)); }
void* operator new(size_t n, std::align_val_t al, const std::nothrow_t&) OP_THROWNOTHING { return hu_alloc_aligned(n, static_cast<size_t>(al)); }
void* operator new[](size_t n, std::align_val_t al, const std::nothrow_t&) OP_THROWNOTHING { return hu_alloc_aligned(n, static_cast<size_t>(al)); }
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

void* reallocf(void* p, size_t size) { return realloc(p, size); }
void* reallocarray(void* p, size_t count, size_t size) { return realloc(p, count * size); }


extern "C" int posix_memalign(void** ptr, size_t align, size_t size) {
    *ptr = hu_alloc_aligned(size, align);
    return 0;
}

void* memalign(size_t align, size_t size) { return hu_alloc_aligned(size, align); }
// msvc specific?
//void* _aligned_malloc(size_t align, size_t size) { return hu_alloc_aligned(align, size); }
//void _aligned_free(void *p) { hu_free_aligned(p, 0); }

// `aligned_alloc` is only available when __USE_ISOC11 is defined.
#if __USE_ISOC11 
void* aligned_alloc(size_t align, size_t size)   { return hu_alloc_aligned(size, align); }
#endif


extern "C" size_t malloc_size(const void* p) { return hu_getsize(p); }
extern "C" size_t malloc_usable_size(void *p) { return hu_getsize(p); }


void* valloc(size_t size) { return hu_alloc_aligned(size, PAGE_SIZE); }
void* pvalloc(size_t size) { return hu_alloc_aligned(size, PAGE_SIZE); }

#endif
