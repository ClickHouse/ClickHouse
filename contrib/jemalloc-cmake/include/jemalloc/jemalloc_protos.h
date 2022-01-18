// OSX does not have this for system alloc functions, so you will get
// "exception specification in declaration" error.
#if defined(__APPLE__) || defined(__FreeBSD__)
# undef JEMALLOC_NOTHROW
# define JEMALLOC_NOTHROW

# undef JEMALLOC_CXX_THROW
# define JEMALLOC_CXX_THROW
#endif

/*
 * The je_ prefix on the following public symbol declarations is an artifact
 * of namespace management, and should be omitted in application code unless
 * JEMALLOC_NO_DEMANGLE is defined (see jemalloc_mangle.h).
 */
extern JEMALLOC_EXPORT const char	*je_malloc_conf;
extern JEMALLOC_EXPORT void		(*je_malloc_message)(void *cbopaque,
    const char *s);

JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_malloc(size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE(1);
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_calloc(size_t num, size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE2(1, 2);
JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_posix_memalign(void **memptr,
    size_t alignment, size_t size) JEMALLOC_CXX_THROW JEMALLOC_ATTR(nonnull(1));
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_aligned_alloc(size_t alignment,
    size_t size) JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc)
    JEMALLOC_ALLOC_SIZE(2);
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_realloc(void *ptr, size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ALLOC_SIZE(2);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_free(void *ptr)
    JEMALLOC_CXX_THROW;

JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_mallocx(size_t size, int flags)
    JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE(1);
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_rallocx(void *ptr, size_t size,
    int flags) JEMALLOC_ALLOC_SIZE(2);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_xallocx(void *ptr, size_t size,
    size_t extra, int flags);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_sallocx(const void *ptr,
    int flags) JEMALLOC_ATTR(pure);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_dallocx(void *ptr, int flags);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_sdallocx(void *ptr, size_t size,
    int flags);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_nallocx(size_t size, int flags)
    JEMALLOC_ATTR(pure);

JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_mallctl(const char *name,
    void *oldp, size_t *oldlenp, void *newp, size_t newlen);
JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_mallctlnametomib(const char *name,
    size_t *mibp, size_t *miblenp);
JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_mallctlbymib(const size_t *mib,
    size_t miblen, void *oldp, size_t *oldlenp, void *newp, size_t newlen);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_malloc_stats_print(
    void (*write_cb)(void *, const char *), void *je_cbopaque,
    const char *opts);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_malloc_usable_size(
    JEMALLOC_USABLE_SIZE_CONST void *ptr) JEMALLOC_CXX_THROW;

#ifdef JEMALLOC_OVERRIDE_MEMALIGN
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_memalign(size_t alignment, size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc);
#endif

#ifdef JEMALLOC_OVERRIDE_VALLOC
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_valloc(size_t size) JEMALLOC_CXX_THROW
    JEMALLOC_ATTR(malloc);
#endif
