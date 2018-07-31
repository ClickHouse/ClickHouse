# Currently this file only enables building on Windows and not Cygwin or MSYS
cmake_minimum_required (VERSION 3.4 FATAL_ERROR)

# Set policy based on the CMake version
cmake_policy(VERSION 3.4.3)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

set (JEMALLOC_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../jemalloc/")
set (JEMALLOC_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/../jemalloc/")

include(${CMAKE_CURRENT_SOURCE_DIR}/Utilities.cmake)

# The following config switches that mimic the original autoconf behavior are supported
# use -D to define cmd arguments, options may have values of ON/OFF
option(disable-munmap "Disables unmapping for later reuse (default - enabled)" OFF )
#  with-mangling=k:v,k:v... comma separated list of key:value pairs overrides specific function mangling
#  with-jemalloc-prefix=<prefix> override default je_ prefix
option(without-export "Ddisable export of public APIs" OFF)
#  with-private-namespace=<additional_prefix>
#  with-install-suffix=<suffix> added to public headers and the library
#  with-malloc-conf=lg_chunk:18 Embed <malloc_conf> as a run-time options string that is processed prior to
#       the malloc_conf global variable
option(disable-cc-silence "Disable compiler silencing code" OFF)
option(enable-debug "Enable debugging code" OFF)
option(enable-ivsalloc "Validate pub API pointers" OFF)
option(disable-stats "Disable stats calculation (on by default)" OFF)
option(disable-tcache "Disable thread-specific caching (on by default)" OFF)
option(disable-fill  "Disabling filling memory with junk on by default" OFF)
option(enable-xmalloc "Support xmalloc option" OFF)
option(disable-cache-oblivious "Disable uniform distribution of large allocations" OFF)
#  with-lg-tiny-min=<lg2 value> override default value of 3 of lg2 minimum tiny clas size
#  with-lg-quantum=<lg2 of the min allocation alignment>
#  with-lg-page=<lg2 of the page size> override system page size
#  with-lg-page-sizes=<comma separated list of lg2 pages sizes> Base 2 logs of system page sizes to support
#  with-lg_size-class-group=<Base 2 log of size classes per doubling> default 2
option(enable-lazy-lock "Enable lazy locking (only lock when multi-threaded" OFF)
option(force_lazy_lock "Forcing lazy-lock to avoid allocator/threading bootstrap issues" OFF)
# install_prefix - installation directory prefix
# with-xslroot=<path>  XSL stylesheet root path

set (PACKAGE_NAME "jemalloc")
project (${PACKAGE_NAME} C)

include (CheckTypeSize)
include (CheckIncludeFiles)
include (TestBigEndian)
include (CheckCSourceCompiles)

CHECK_INCLUDE_FILES (alloca.h JEMALLOC_HAS_ALLOCA_H)

set(abi "elf")

# Whether malloc_usable_size definition can use const argument
CHECK_INCLUDE_FILES (malloc.h HAVE_MALLOC_H)
set(JEMALLOC_USABLE_SIZE_CONST)
if(HAVE_MALLOC_H)
    set(JEMALLOC_USABLE_SIZE_CONST const)
endif()

CHECK_INCLUDE_FILES (inttypes.h HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES (stdatomic.h JEMALLOC_C11ATOMICS)
CHECK_INCLUDE_FILES (sys/time.h HAVE_SYSTIME_H)

TEST_BIG_ENDIAN(JEMALLOC_BIG_ENDIAN)

# CPU-specific settings.

if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "i686" OR CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64")
  set( CPU_SPINWAIT "__asm__ volatile(\"pause\")")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "powerpc")
  set(HAVE_ALTIVEC 1)
endif()


# If defined, use munmap() to unmap freed chunks, rather than storing them for
# later reuse.  This is disabled by default on Linux because common sequences
# of mmap()/munmap() calls will cause virtual memory map holes.
# But it is enabled by default on Windows
set(JEMALLOC_MUNMAP 0)

# If defined, adjacent virtual memory mappings with identical attributes
# automatically coalesce, and they fragment when changes are made to subranges.
# This is the normal order of things for mmap()/munmap(), but on Windows
# VirtualAlloc()/VirtualFree() operations must be precisely matched, i.e.
# mappings do *not* coalesce/fragment.
set(JEMALLOC_MAPS_COALESCE 1)

###################################################
# Undefined
# #undef JEMALLOC_HAVE_ATTR
set(JEMALLOC_HAVE_ATTR 1)
# Below is everything under HAVE_ATTR
# #undef JEMALLOC_HAVE_ATTR_ALLOC_SIZE
# #undef JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF
# #undef JEMALLOC_FORMAT_PRINTF
# #undef JEMALLOC_USE_CXX_THROW
#

set (JEMALLOC_OVERRIDE_MEMALIGN 1)
set (JEMALLOC_OVERRIDE_VALLOC 1)

if(with-mangling)
# We are expecting entries separated by a comma
# with individual entries split by a ':' as in n:m
# Convert that into a CMake list of ';' separated pairs
  string(REPLACE "," ";" MANGLING_MAP  ${with-mangling})
endif()

# Set the default API prefix for public
set(JEMALLOC_PREFIX je_)
# Protos are always je_ but are renamed by #defines according to prefix
set(je_ "je_")

if(with-jemalloc-prefix)
  set(JEMALLOC_PREFIX ${with-jemalloc-prefix})
endif()


# Uppercase copy of the JEMALLOC_PREFIX
# Need  quotes so the preprocessor concats two strings
if(JEMALLOC_PREFIX)
    string(TOUPPER \"${JEMALLOC_PREFIX}\" JEMALLOC_CPREFIX)
endif()

# Disable exporting jemalloc public APIs
# We need to define the var to whitespace string
# as empty strings will not be defined in CMake
# Non-empty definition is necessary so
if(without-export)
    set(JEMALLOC_EXPORT " ")
endif()

# Prefix to prepend to all library-private APIs
# default is on
set(JEMALLOC_PRIVATE_NAMESPACE je_)
if(with-private-namespace)
    set(JEMALLOC_PRIVATE_NAMESPACE "${with_private_namespace}je_")
endif()

set(private_namespace ${JEMALLOC_PRIVATE_NAMESPACE})

# Default empty
# Specify default malloc_conf
set(JEMALLOC_CONFIG_MALLOC_CONF "\"\"")
if(with-malloc-conf)
    set(JEMALLOC_CONFIG_MALLOC_CONF "\"${with-malloc-conf}\"")
endif()

if(with-install-suffix)
    set(INSTALL_SUFFIX ${with-install-suffix})
    set(install_suffix ${with-install-suffix})
endif()

# Do not silence irrelevant compiler warnings
set(JEMALLOC_CC_SILENCE 1)
if(disable-cc-silence)
    set(JEMALLOC_CC_SILENCE 0)
endif()

# Build debugging code (implies --enable-ivsalloc)
if(enable-debug)
    set(JEMALLOC_DEBUG 1)
    set(JEMALLOC_IVSALLOC 1)
endif()

# Validate pointers passed through the public API
if(enable-ivsalloc)
    set(JEMALLOC_IVSALLOC 1)
endif()

# Enable stats by default
set(JEMALLOC_STATS 1)
# Disable statistics calculation/reporting
if(disable-stats)
  set(JEMALLOC_STATS 0)
endif()

# Enable thread-specific caching by default.
set(JEMALLOC_TCACHE 1)
if(disable-tcache)
  set(JEMALLOC_TCACHE 0)
endif()

set(JEMALLOC_PREFIX_JET jet_)

# Disabling dss allocation because sbrk is deprecated
set(JEMALLOC_DSS 0)

# Support the junk/zero filling option by default.
set (JEMALLOC_FILL 1)
# Disable support for junk/zero filling, quarantine, and redzones
if(disable-fill)
  set (JEMALLOC_FILL 0)
endif()

# Windows does not have it
set(JEMALLOC_UTRACE 0)
set(JEMALLOC_VALGRIND 0)

# Support xmalloc option
set(JEMALLOC_XMALLOC 0)
if(enable-xmalloc)
  set(JEMALLOC_XMALLOC 1)
endif()

# Support cache-oblivious allocation alignment by default.
# If defined, explicitly attempt to more uniformly distribute large allocation
# pointer alignments across all cache indices.
set(JEMALLOC_CACHE_OBLIVIOUS 1)
if(disable-cache-oblivious)
  set(JEMALLOC_CACHE_OBLIVIOUS 0)
endif()

set(JEMALLOC_INTERNAL_UNREACHABLE abort)

# ffsl and ffs are defined in msvc_compat/strings.h
set(JEMALLOC_INTERNAL_FFSL ffsl)
set(JEMALLOC_INTERNAL_FFS ffs)
set(JEMALLOC_INTERNAL_FFSLL ffsll)

# Base 2 log of minimum tiny size class to support
set(LG_TINY_MIN 3)
if(with-lg-tiny-min)
  set(LG_TINY_MIN ${with-lg-tiny-min})
endif()

# Base 2 log of minimum allocation alignment
set(LG_QUANTA 3 4)
if(with-lg-quantum)
  # Convert to a CMake list
  string(REPLACE "," ";" LG_QUANTA  ${with-lg-quantum})
  set(LG_QUANTA ${with-lg-quantum})
  set(LG_QUANTUM ${LG_QUANTA})
endif()

# Base 2 log of system page size

if(with-lg-page)
  set(LG_PAGE ${with-lg-page})
endif()

set(LG_PAGE 12)

# Base 2 logs of system page sizes to support
set (LG_PAGE_SIZES ${LG_PAGE})
if(with-lg-page-sizes)
  string(REPLACE "," ";" LG_PAGE_SIZES  ${with-lg-page-sizes})
endif()

# Base 2 log of size classes per doubling
set (LG_SIZE_CLASS_GROUP 2)
if(with-lg-size-class-group)
  set (LG_SIZE_CLASS_GROUP ${with-lg-size-class-group})
endif()

if(NOT WIN32)
# Check if syscall(2) is usable.  Treat warnings as errors, so that e.g. OS X
# 10.12's deprecation warning prevents use.
  set(CMAKE_REQUIRED_FLAGS  "${CMAKE_C_FLAGS} -Werror")
  CHECK_C_SOURCE_COMPILES("
    #include <sys/syscall.h>
    #include <unistd.h>
    syscall(SYS_write, 2, \"hello\", 5);
    " HAVE_SYSCALL)

  if(HAVE_SYSCALL)
    set(JEMALLOC_HAVE_SYSCALL 1)
  endif()

  CHECK_FUNCTION_EXISTS(secure_getenv HAVE_SECURE_GETENV)
  if(HAVE_SECURE_GETENV)
    set(JEMALLOC_HAVE_SECURE_GETENV 1)
  endif()

endif()

set(JEMALLOC_HAVE_ISSETUGID 0)
set(JEMALLOC_MALLOC_THREAD_CLEANUP 0)

set(JEMALLOC_MUTEX_INIT_CB 0)

############################
# enable-lazy-lock
set(JEMALLOC_LAZY_LOCK 0)
if(NOT enable-lazy-lock)
   if(force_lazy_lock)
      message(STATUS "Forcing lazy-lock to avoid allocator/threading bootstrap issues")
      set(enable-lazy-lock ON)
  endif()
endif()

if(enable-lazy-lock)
  if(${abi} STREQUAL "pecoff")
      message(STATUS "Forcing no lazy-lock because thread creation monitoring is unimplemented")
      set(enable-lazy-lock OFF)
  endif()
endif()

if(enable-lazy-lock)
  CHECK_INCLUDE_FILES (dlfcn.h HAVE_DLFCN_H)
  if(NOT HAVE_DLFCN_H)
    message(FATAL_ERROR "dlfcn.h is missing")
  endif()

  set(CMAKE_REQUIRED_LIBRARIES "dl")
  CHECK_FUNCTION_EXISTS(dlsym HAVE_DLSYM)
  if(NOT HAVE_DLSYM)
    message(FATAL_ERROR "libdl is missing]")
  endif()
endif()

#########################

# Separate clause for _WIN32 does the right thing
# So TLS is enabled for Windows
set(JEMALLOC_TLS 0)

# Relevant for FreeBSD only
set(JEMALLOC_ATOMIC9 0)

# Only for iOS
set(JEMALLOC_OSATOMIC 0)
set(JEMALLOC_OSSPIN 0)
set(JEMALLOC_ZONE 0)

# Only for GNU
set(JE_FORCE_SYNC_COMPARE_AND_SWAP_4 0)
set(JE_FORCE_SYNC_COMPARE_AND_SWAP_8 0)
set(JEMALLOC_HAVE_BUILTIN_CLZ 0)
set(JEMALLOC_HAVE_MADVISE 0)
set(JEMALLOC_THREADED_INIT 0)

set(JEMALLOC_TLS_MODEL 0)
set(JEMALLOC_CODE_COVERAGE 0)
set(JEMALLOC_PROF 0)
set(JEMALLOC_PROF_LIBUNWIND 0)
set(JEMALLOC_PROF_LIBGCC 0)
set(JEMALLOC_PROF_GCC 0)

###########################################################################
# Generate configured public headers for concatenation
# Public Headers in for configuring
set(PUBLIC_SYM
  malloc_conf
  malloc_message
  malloc
  calloc
  posix_memalign
  aligned_alloc
  realloc
  free
  mallocx
  rallocx
  xallocx
  sallocx
  dallocx
  sdallocx
  nallocx
  mallctl
  mallctlnametomib
  mallctlbymib
  malloc_stats_print
  malloc_usable_size
)

foreach(public_in jemalloc_macros.h jemalloc_defs.h jemalloc_protos.h jemalloc_typedefs.h)
  configure_file("${JEMALLOC_SOURCE_DIR}/include/jemalloc/${public_in}.in"
    "${JEMALLOC_BINARY_DIR}/include/jemalloc/${public_in}")
endforeach(public_in)

# Generate main public header
set(JEMALLOC_HDR "${JEMALLOC_BINARY_DIR}/include/jemalloc/jemalloc${install_suffix}.h")

set(JEMALLOC_HDR_LIST
   jemalloc_defs.h
   jemalloc_macros.h
   jemalloc_protos.h
   jemalloc_typedefs.h
)

CreateJemallocHeader("${JEMALLOC_HDR_LIST}" "${JEMALLOC_HDR}")

# Configure internal headers

foreach(internal_in jemalloc_internal_defs.h jemalloc_preamble.h)
  configure_file("${JEMALLOC_SOURCE_DIR}/include/jemalloc/internal/${internal_in}.in"
    "${JEMALLOC_BINARY_DIR}/include/jemalloc/internal/${internal_in}")
endforeach(internal_in)

set(SIZE_CLASSES_HDR "${JEMALLOC_SOURCE_DIR}/include/jemalloc/internal/size_classes.h")
SizeClasses("${LG_QUANTA}" ${LG_TINY_MIN} "${LG_PAGE_SIZES}" "${LG_SIZE_CLASS_GROUP}"
 "${SIZE_CLASSES_HDR}")

# To generate protos_jet
set(je_ jet_)

# replace prefix only
configure_file("${JEMALLOC_SOURCE_DIR}/include/jemalloc/jemalloc_protos.h.in"
    "${JEMALLOC_BINARY_DIR}/include/jemalloc/jemalloc_protos_jet.h")

# revert
set(je_ je_)

set(C_SRCS
${JEMALLOC_SOURCE_DIR}/src/arena.c
${JEMALLOC_SOURCE_DIR}/src/background_thread.c
${JEMALLOC_SOURCE_DIR}/src/base.c
${JEMALLOC_SOURCE_DIR}/src/bin.c
${JEMALLOC_SOURCE_DIR}/src/bitmap.c
${JEMALLOC_SOURCE_DIR}/src/ckh.c
${JEMALLOC_SOURCE_DIR}/src/ctl.c
${JEMALLOC_SOURCE_DIR}/src/div.c
${JEMALLOC_SOURCE_DIR}/src/extent.c
${JEMALLOC_SOURCE_DIR}/src/extent_dss.c
${JEMALLOC_SOURCE_DIR}/src/extent_mmap.c
${JEMALLOC_SOURCE_DIR}/src/hash.c
${JEMALLOC_SOURCE_DIR}/src/hook.c
${JEMALLOC_SOURCE_DIR}/src/jemalloc.c
${JEMALLOC_SOURCE_DIR}/src/jemalloc_cpp.cpp
${JEMALLOC_SOURCE_DIR}/src/large.c
${JEMALLOC_SOURCE_DIR}/src/log.c
${JEMALLOC_SOURCE_DIR}/src/malloc_io.c
${JEMALLOC_SOURCE_DIR}/src/mutex.c
${JEMALLOC_SOURCE_DIR}/src/mutex_pool.c
${JEMALLOC_SOURCE_DIR}/src/nstime.c
${JEMALLOC_SOURCE_DIR}/src/pages.c
${JEMALLOC_SOURCE_DIR}/src/prng.c
${JEMALLOC_SOURCE_DIR}/src/prof.c
${JEMALLOC_SOURCE_DIR}/src/rtree.c
${JEMALLOC_SOURCE_DIR}/src/sc.c
${JEMALLOC_SOURCE_DIR}/src/stats.c
${JEMALLOC_SOURCE_DIR}/src/sz.c
${JEMALLOC_SOURCE_DIR}/src/tcache.c
${JEMALLOC_SOURCE_DIR}/src/test_hooks.c
${JEMALLOC_SOURCE_DIR}/src/ticker.c
${JEMALLOC_SOURCE_DIR}/src/tsd.c
${JEMALLOC_SOURCE_DIR}/src/witness.c
${JEMALLOC_SOURCE_DIR}/src/zone.c
)

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  list(APPEND C_SRCS ${JEMALLOC_SOURCE_DIR}/src/zone.c)
endif()

if(enable_valgrind)
  list(APPEND C_SRCS ${JEMALLOC_SOURCE_DIR}/src/valgrind.c)
endif()

# The original library, delivery product
set(LIBJEMALLOCLIB jemalloc${install_suffix})
add_library(${LIBJEMALLOCLIB} STATIC ${C_SRCS})

target_include_directories(${LIBJEMALLOCLIB} PRIVATE ${JEMALLOC_BINARY_DIR}/include ${JEMALLOC_SOURCE_DIR}/include)

if(with-jemalloc-prefix)
  target_compile_definitions(${LIBJEMALLOCLIB} PRIVATE JEMALLOC_MANGLE)
endif()

