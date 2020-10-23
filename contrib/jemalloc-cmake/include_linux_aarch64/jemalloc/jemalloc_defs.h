/* include/jemalloc/jemalloc_defs.h.  Generated from jemalloc_defs.h.in by configure.  */
/* Defined if __attribute__((...)) syntax is supported. */
#define JEMALLOC_HAVE_ATTR

/* Defined if alloc_size attribute is supported. */
#define JEMALLOC_HAVE_ATTR_ALLOC_SIZE

/* Defined if format(printf, ...) attribute is supported. */
#define JEMALLOC_HAVE_ATTR_FORMAT_PRINTF

/*
 * Define overrides for non-standard allocator-related functions if they are
 * present on the system.
 */
#define JEMALLOC_OVERRIDE_MEMALIGN
#define JEMALLOC_OVERRIDE_VALLOC

/*
 * At least Linux omits the "const" in:
 *
 *   size_t malloc_usable_size(const void *ptr);
 *
 * Match the operating system's prototype.
 */
#define JEMALLOC_USABLE_SIZE_CONST

/*
 * If defined, specify throw() for the public function prototypes when compiling
 * with C++.  The only justification for this is to match the prototypes that
 * glibc defines.
 */
#define JEMALLOC_USE_CXX_THROW

#ifdef _MSC_VER
#  ifdef _WIN64
#    define LG_SIZEOF_PTR_WIN 3
#  else
#    define LG_SIZEOF_PTR_WIN 2
#  endif
#endif

/* sizeof(void *) == 2^LG_SIZEOF_PTR. */
#define LG_SIZEOF_PTR 3
