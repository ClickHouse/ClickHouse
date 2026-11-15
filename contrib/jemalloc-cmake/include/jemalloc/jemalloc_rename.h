/*
 * Name mangling for public symbols is controlled by --with-mangling and
 * --with-jemalloc-prefix.  With default settings the je_ prefix is stripped by
 * these macro definitions.
 *
 * When JEMALLOC_PREFIX is defined (i.e. jemalloc is built with je_ prefix),
 * no renaming is needed — je_malloc IS the canonical symbol name.
 */
#if !defined(JEMALLOC_NO_RENAME) && !defined(JEMALLOC_PREFIX)
#  define je_aligned_alloc aligned_alloc
#  define je_calloc calloc
#  define je_dallocx dallocx
#  define je_free free
#  define je_mallctl mallctl
#  define je_mallctlbymib mallctlbymib
#  define je_mallctlnametomib mallctlnametomib
#  define je_malloc malloc
#  define je_malloc_conf malloc_conf
#  define je_malloc_conf_2_conf_harder malloc_conf_2_conf_harder
#  define je_malloc_message malloc_message
#  define je_malloc_stats_print malloc_stats_print
#  define je_malloc_usable_size malloc_usable_size
#  define je_mallocx mallocx
#  define je_smallocx_ca709c3139f77f4c00a903cdee46d71e9028f6c6 smallocx_ca709c3139f77f4c00a903cdee46d71e9028f6c6
#  define je_nallocx nallocx
#  define je_posix_memalign posix_memalign
#  define je_rallocx rallocx
#  define je_realloc realloc
#  define je_sallocx sallocx
#  define je_sdallocx sdallocx
#  define je_xallocx xallocx
#  define je_memalign memalign
#  define je_valloc valloc
#endif
