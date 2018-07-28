# This make file supports Windows build of Jemalloc
#
# Prerequisites:
#     You must have Visual Studio 2013 Update 4 installed or Visual Studio 2015 Update 1.
#      Start the Developer Command Prompt window that is a part of Visual Studio installation.
#      This will provide you with the accessible toolchain commands.
#      You must have a path git.exe in your %PATH%.
#
# 1. Create a build directory
#
# 2. Run cmake to generate project files for Windows
#        sample command: cmake -G "Visual Studio 12 Win64"  ..
#        OR for VS Studio 15 cmake -G "Visual Studio 14 Win64"  [optional switches described below] ..
#
# 3. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m ot use all avail cores)
#        msbuild jemalloc.sln
#
# 4. And release mode (/m[:<N>] is also supported)
#        msbuild jemalloc.sln /p:Configuration=Release
#

# Currently this file only enables building on Windows and not Cygwin or MSYS
cmake_minimum_required (VERSION 3.4 FATAL_ERROR)

# Set policy based on the CMake version
cmake_policy(VERSION 3.4.3)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

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
include(TestBigEndian)
include (CheckCSourceCompiles)
include (CTest)

include(${CMAKE_SOURCE_DIR}/Utilities.cmake)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/build-aux")

# TODO: Check the need for CONFIG

# Set library revision
set(rev 2)

# Installation
# Munge install path variables.
# All of these can simply we be passed as
# Cmake arguments
if (NOT install_prefix)
  set(install_prefix "/usr/local")
endif()

if(NOT exec_prefix)
  set(exec_prefix $install_prefix)
endif()

set(PREFIX $install_prefix)

# Support for building documentation.
# find_package(xsltproc)
# if(XSLTPROC_FOUND)
  # set(XSLTPROC ${XSLTPROC_EXECUTABLE})
# endif()

if(EXISTS "/usr/share/xml/docbook/stylesheet/docbook-xsl")
  set(DEFAULT_XSLROOT "/usr/share/xml/docbook/stylesheet/docbook-xsl")
elseif(EXISTS "/usr/share/sgml/docbook/xsl-stylesheets")
  set(DEFAULT_XSLROOT "/usr/share/sgml/docbook/xsl-stylesheets")
else()
  # Documentation building will fail if this default gets used.
  set(DEFAULT_XSLROOT "")
endif()

if(with_xslroot)
  set(XSLROOT "${with_xslroot}")
else()
  set(XSLROOT "${DEFAULT_XSLROOT}")
endif()

# Cmake will always have some CFLAGS set based on the compiler detection
# Until we discover it is not what we want
set(je_cv_cray_prgenv_wrapper False)

if(ENV${PE_ENV})
  # TODO: Check if MATCHES is OK with this syntax
  if(ENV${PE_ENV} MATCHES "CC|cc")
    set(je_cv_cray_prgenv_wrapper True)
  endif()
endif()

message(STATUS "CMAKE_C_COMPILER_ID: ${CMAKE_C_COMPILER_ID}")

# Whether cray compiler version is 8.4
set(je_cv_cray_84 False)
if(CMAKE_C_COMPILER_ID STREQUAL "Cray")
  set(je_cv_cray True)
  # If the version is defined then use it
  if(CMAKE_C_COMPILER_VERSION)
    set(VersionFound True)
    set(VERSION_84 "8.4.0.0")
    set(VERSION_85 "8.5.0.0")
    if(VERSION_84 VERSION_EQUAL ${CMAKE_C_COMPILER_VERSION})
      set(je_cv_cray_84 True)
    elseif(VERSION_84 VERSION_LESS ${CMAKE_C_COMPILER_VERSION} AND
           VERSION_85 VERSION_GREATER ${CMAKE_C_COMPILER_VERSION})
      set(je_cv_cray_84 True)
    endif()
  endif()
  # TODO: If the version is not defined then need to
  # if(NOT VersionFound) COmpile and build the program
endif()

if(NOT CFLAGS)
  if(CMAKE_COMPILER_IS_GNUCC)
    JeCflagsAppend("-std=gnu11" "GFLAGS" "je_cv_cflags_appended")
    if(je_cv_cflags_appended)
      set (JEMALLOC_HAS_RESTRICT 1)
    else()
      JeCflagsAppend("-std=gnu99" "GFLAGS" "je_cv_cflags_appended")
      if(je_cv_cflags_appended)
        set (JEMALLOC_HAS_RESTRICT 1)
      endif()
    endif()
    
    # The errors on these are not checked
    set (GFLAGS "${GFLAGS} -Wall -Werror=declaration-after-statement "
                "-Wshorten-64-to-32 -Wsign-compare -pipe -g3")
  endif()
  
  if(MSVC)
    # Restrict is #defined to _restrict
    set (JEMALLOC_HAS_RESTRICT 1)
    # The rest of MSVC flags are down below
  endif()
  
  if(je_cv_cray)
    # cray compiler 8.4 has an inlining bug
    if(je_cv_cray_84)
      set (GFLAGS "${GFLAGS} -hipa2 -hnognu")
    endif(je_cv_cray_84)
    
   if(enable_cc_silence)
      # ignore unreachable code warning
      set (GFLAGS "${GFLAGS} -hnomessage=128")
      # ignore redefinition of "malloc", "free", etc warning
      set (GFLAGS "${GFLAGS} -hnomessage=1357")
   endif(enable_cc_silence)
 endif(je_cv_cray)
endif()

##################################################################
# Versioning from GIT
# Defaults
set (jemalloc_version "0.0.0-0-g0000000000000000000000000000000000000000")
set (jemalloc_version_major 0)
set (jemalloc_version_minor 0)
set (jemalloc_version_bugfix 0)
set (jemalloc_version_nrev 0)
set (jemalloc_version_gid "0")

find_package(Git)
GetAndParseVersion()

# We do not support exec_prefix until we find it is necessary

CHECK_INCLUDE_FILES (alloca.h JEMALLOC_HAS_ALLOCA_H)

# TODO Determine abi
set(abi "elf")

# Whether malloc_usable_size definition can use const argument
CHECK_INCLUDE_FILES (malloc.h HAVE_MALLOC_H)
if(HAVE_MALLOC_H)
    set(JEMALLOC_USABLE_SIZE_CONST const)
endif()    

CHECK_INCLUDE_FILES (inttypes.h HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES (stdatomic.h JEMALLOC_C11ATOMICS)
CHECK_INCLUDE_FILES (sys/time.h HAVE_SYSTIME_H)

TEST_BIG_ENDIAN(JEMALLOC_BIG_ENDIAN)

UtilCheckTypeSize(void* SIZEOF_VOID_P)
if(SIZEOF_VOID_P)
  lg(${SIZEOF_VOID_P} "LG_SIZEOF_PTR")
  # The latest code hardcodes this on Windows
  # set(LG_SIZEOF_PTR_WIN ${LG_SIZEOF_PTR})
  if((NOT ${LG_SIZEOF_PTR} EQUAL 3) AND
     (NOT ${LG_SIZEOF_PTR} EQUAL 2))
    message(FATAL_ERROR "Unsupported pointer size :${LG_SIZEOF_PTR}")
  endif()
endif()

UtilCheckTypeSize(int SIZEOF_INT)
if(SIZEOF_INT)
  lg(${SIZEOF_INT} "LG_SIZEOF_INT")
  if((NOT ${LG_SIZEOF_INT} EQUAL 3) AND
     (NOT ${LG_SIZEOF_INT} EQUAL 2))
    message(FATAL_ERROR "Unsupported int size :${LG_SIZEOF_INT}")
  endif()
endif()

UtilCheckTypeSize(long SIZEOF_LONG)
if(SIZEOF_LONG)
  lg(${SIZEOF_LONG} "LG_SIZEOF_LONG")
  if((NOT ${LG_SIZEOF_LONG} EQUAL 3) AND
     (NOT ${LG_SIZEOF_LONG} EQUAL 2))
    message(FATAL_ERROR "Unsupported long size :${LG_SIZEOF_LONG}")
  endif()
endif()

UtilCheckTypeSize("long long" SIZEOF_LONG_LONG)
if(SIZEOF_LONG_LONG)
  lg(${SIZEOF_LONG_LONG} "LG_SIZEOF_LONG_LONG")
  if((NOT ${LG_SIZEOF_LONG_LONG} EQUAL 3) AND
     (NOT ${LG_SIZEOF_LONG_LONG} EQUAL 2))
    message(FATAL_ERROR "Unsupported long size :${LG_SIZEOF_LONG_LONG}")
  endif()
endif()

UtilCheckTypeSize(intmax_t SIZEOF_INTMAX_T)
if(SIZEOF_INTMAX_T)
  lg(${SIZEOF_INTMAX_T} "LG_SIZEOF_INTMAX_T")
  if((NOT ${LG_SIZEOF_INTMAX_T} EQUAL 4) AND
     (NOT ${LG_SIZEOF_INTMAX_T} EQUAL 3) AND
     (NOT ${LG_SIZEOF_INTMAX_T} EQUAL 2))
    message(FATAL_ERROR "Unsupported long size :${LG_SIZEOF_INTMAX_T}")
  endif()
endif()

# CPU-specific settings.
message(STATUS "CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")

if(MSVC)
  JeCompilable("pause instruction MSVC" "" "_mm_pause();" je_cv_pause_msvc)
  if(je_cv_pause_msvc)
    set(CPU_SPINWAIT "_mm_pause()")
  else()
    JeCompilable("YieldProcessor() MSVC" "#include <Windows.h>" "YieldProcessor();" je_cv_pause_msvc)
    if(je_cv_pause_msvc)
      set (CPU_SPINWAIT "YieldProcessor()")
    endif()
  endif()
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "i686" OR
   CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64")
  JeCompilable("pause instruction" "" "__asm__ volatile(\"pause\");" je_cv_pause)
  if(je_cv_pause)
    set( CPU_SPINWAIT "__asm__ volatile(\"pause\")")
  endif()
endif()

if(CMAKE_SYSTEM_NAME MATCHES "powerpc")
  set(HAVE_ALTIVEC 1)
endif()


# If defined, use munmap() to unmap freed chunks, rather than storing them for
# later reuse.  This is disabled by default on Linux because common sequences
# of mmap()/munmap() calls will cause virtual memory map holes.
# But it is enabled by default on Windows
set(JEMALLOC_MUNMAP 1)

if(disable-munmap)
  set(JEMALLOC_MUNMAP 0)
endif()

# If defined, adjacent virtual memory mappings with identical attributes
# automatically coalesce, and they fragment when changes are made to subranges.
# This is the normal order of things for mmap()/munmap(), but on Windows
# VirtualAlloc()/VirtualFree() operations must be precisely matched, i.e.
# mappings do *not* coalesce/fragment.
set(JEMALLOC_MAPS_COALESCE 0)

###################################################
# Undefined
# #undef JEMALLOC_HAVE_ATTR
set(JEMALLOC_HAVE_ATTR 0)
# Below is everything under HAVE_ATTR
# #undef JEMALLOC_HAVE_ATTR_ALLOC_SIZE
# #undef JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF
# #undef JEMALLOC_FORMAT_PRINTF
# #undef JEMALLOC_USE_CXX_THROW
#

set (JEMALLOC_OVERRIDE_MEMALIGN 0)
set (JEMALLOC_OVERRIDE_VALLOC 0)

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

if(NOT LG_PAGE OR
   "${LG_PAGE}" STREQUAL "detect")
    GetSystemPageSize("SYSTEM_PAGE_SIZE")
    lg(${SYSTEM_PAGE_SIZE} "LG_PAGE")
endif()

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

set(PUBLIC_SYM_FILE "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/public_symbols.txt")
GeneratePublicSymbolsList("${PUBLIC_SYM}" "${MANGLING_MAP}" ${JEMALLOC_PREFIX}  "${PUBLIC_SYM_FILE}")

foreach(public_in jemalloc_macros.h jemalloc_defs.h jemalloc_protos.h jemalloc_typedefs.h)
  ConfigureFile("${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/${public_in}.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/${public_in}" True)
endforeach(public_in)  

set(JEMALLOC_RENAME_HDR "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/jemalloc_rename.h")
GenerateJemallocRename("${PUBLIC_SYM_FILE}" ${JEMALLOC_RENAME_HDR})

set(JEMALLOC_MANGLE_HDR "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/jemalloc_mangle.h")
GenerateJemallocMangle("${PUBLIC_SYM_FILE}" ${JEMALLOC_PREFIX} ${JEMALLOC_MANGLE_HDR})

# Needed for tests
set(JEMALLOC_MANGLE_JET_HDR "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/jemalloc_mangle_jet.h")
GenerateJemallocMangle("${PUBLIC_SYM_FILE}" ${JEMALLOC_PREFIX_JET} ${JEMALLOC_MANGLE_JET_HDR})

# Generate main public header
set(JEMALLOC_HDR "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/jemalloc${install_suffix}.h")

set(JEMALLOC_HDR_LIST
   jemalloc_defs.h
   jemalloc_rename.h
   jemalloc_macros.h
   jemalloc_protos.h
   jemalloc_typedefs.h
   jemalloc_mangle.h
)

CreateJemallocHeader("${JEMALLOC_HDR_LIST}" "${JEMALLOC_HDR}")

##############################################################################################
## Internal headers generation
set(PUBLIC_NAMESPACE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/public_namespace.h")
PublicNamespace(${PUBLIC_SYM_FILE} "${PUBLIC_NAMESPACE_FILE}")

set(PUBLIC_UNNAMESPACE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/public_unnamespace.h")
PublicUnnamespace(${PUBLIC_SYM_FILE} "${PUBLIC_UNNAMESPACE_FILE}")

# This file comes with repo
set(PRIVATE_SYM_FILE "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/private_symbols.txt")

set(PRIVATE_NAMESPACE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/private_namespace.h")
PrivateNamespace("${PRIVATE_SYM_FILE}" "${PRIVATE_NAMESPACE_FILE}")

set(PRIVATE_UNNAMESPACE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/private_unnamespace.h")
PrivateUnnamespace("${PRIVATE_SYM_FILE}" "${PRIVATE_UNNAMESPACE_FILE}")

# Configure internal headers

# Main internal header does not require #define expansion otherwise it affects real #undefs
ConfigureFile("${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/jemalloc_internal.h.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/jemalloc_internal.h" False)

    
foreach(internal_in jemalloc_internal_defs.h)
  ConfigureFile("${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/${internal_in}.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/${internal_in}" True)
endforeach(internal_in)  

# Test related headers
ConfigureFile("${CMAKE_CURRENT_SOURCE_DIR}/test/include/test/jemalloc_test_defs.h.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/test/include/test/jemalloc_test_defs.h" True)

ConfigureFile("${CMAKE_CURRENT_SOURCE_DIR}/test/include/test/jemalloc_test.h.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/test/include/test/jemalloc_test.h" False)


set(SIZE_CLASSES_HDR "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/internal/size_classes.h")
SizeClasses("${LG_QUANTA}" ${LG_TINY_MIN} "${LG_PAGE_SIZES}" "${LG_SIZE_CLASS_GROUP}"
 "${SIZE_CLASSES_HDR}")

# To generate protos_jet
set(je_ jet_)

# replace prefix only
ConfigureFile("${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/jemalloc_protos.h.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/jemalloc/jemalloc_protos_jet.h" True)

# revert
set(je_ je_)

set(C_SRCS
  src/jemalloc.c
  src/arena.c
  src/atomic.c
  src/base.c
  src/bitmap.c
  src/chunk.c
  src/chunk_dss.c
  src/chunk_mmap.c
  src/ckh.c
  src/ctl.c
  src/extent.c
  src/hash.c
  src/huge.c
  src/mb.c
  src/mutex.c
  src/nstime.c
  src/pages.c
  src/prng.c
  src/prof.c
  src/quarantine.c
  src/rtree.c
  src/stats.c
  src/spin.c
  src/tcache.c
  src/ticker.c
  src/tsd.c
  src/util.c
  src/witness.c
)

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  list(APPEND C_SRCS src/zone.c)
endif()

if(enable_valgrind)
  list(APPEND C_SRCS src/valgrind.c)
endif()

# The original library, delivery product
set(LIBJEMALLOCLIB jemalloc${install_suffix})
add_library(${LIBJEMALLOCLIB} STATIC ${C_SRCS})

# Now add shared library. Needed for integration tests
# and a benchmark
set(LIBJEMALLOCSO jemallocso${install_suffix})
add_library(${LIBJEMALLOCSO} SHARED ${C_SRCS})

if(WIN32)
  # May want to replace  /d2Zi+ to /Zo
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /FC /d2Zi+ /Zi /FS /nologo /W3 /WX /GS /Zc:wchar_t /Zc:forScope /errorReport:queue")

  # Separate line for warnings suppression mostly due to the fact we have ints 32 bits
  # - C4267 due to the fact we have ints 32 bits
  # - C4244 simular typedef unsigned szind_t
  # - C4146 alignment calculation applies negation to a unsigned type
  # - C4334 - result of 32-bit shift implicitly converted to 64 bits
  # - C4090 - some modifiable parameters are passed with a const qualifier
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4267 /wd4244 /wd4146 /wd4334 /wd4090")

  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /Od /RTC1 /Gm /MDd" )
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /MD")

  add_definitions(-D_MBCS)

  include_directories(${PROJECT_SOURCE_DIR}/include)
  include_directories(${PROJECT_SOURCE_DIR}/include/msvc_compat)

  if(NOT HAVE_INTTYPES_H)
    include_directories(${PROJECT_SOURCE_DIR}/include/msvc_compat/C99)
  endif()

  # Need full PDB for those who link to it
  # MSVC does not generate full PDB for static libs by default
  # and disable export from the DLL otherwise JEMALLOC_EXPORT becomes
  # either impport or export which is both wrong for a static library
  set_target_properties(${LIBJEMALLOCLIB}
           PROPERTIES
           COMPILE_DEFINITIONS
           "JEMALLOC_EXPORT="
           CMAKE_COMPILE_PDB_OUTPUT_DIRECTORY_DEBUG
           ${PROJECT_BINARY_DIR}/Debug
           CMAKE_COMPILE_PDB_OUTPUT_DIRECTORY_RELEASE
           ${PROJECT_BINARY_DIR}/Release
           COMPILE_PDB_NAME 
           ${LIBJEMALLOCLIB}
  )

endif()

# Need to export from DLL
target_compile_definitions(${LIBJEMALLOCSO} PRIVATE
    DLLEXPORT)

if(with-jemalloc-prefix)
  target_compile_definitions(${LIBJEMALLOCLIB} PRIVATE
      JEMALLOC_MANGLE)

  target_compile_definitions(${LIBJEMALLOCSO} PRIVATE
      JEMALLOC_MANGLE)
endif()

##################################################################
## Building tests

# Compose compiler defs and options for the tests

set(C_JETLIB_DEFS "JEMALLOC_JET")
set(C_UTIL_INTEGRATION_DEFS "")
set(C_TESTLIB_INTEGRATION_DEFS "JEMALLOC_INTEGRATION_TEST")
set(C_TESTLIB_UNIT_DEFS "JEMALLOC_UNIT_TEST")
set(C_TESTLIB_STRESS_DEF "JEMALLOC_STRESS_TEST;JEMALLOC_STRESS_TESTLIB")
set(C_UNITETEST_DEFS "JEMALLOC_UNIT_TEST")

if(with-jemalloc-prefix)
 set(C_JETLIB_DEFS  "${C_JETLIB_DEFS};JEMALLOC_MANGLE")
 set(C_UTIL_INTEGRATION_DEFS "${C_UTIL_INTEGRATION_DEFS};JEMALLOC_MANGLE")
endif()

# Add empty definition of JEMALLOC_EXPORT as we use them in
# static form on other platforms it is empty defined by default but
# not on windows
if(MSVC)
  set(C_JETLIB_DEFS  "${C_JETLIB_DEFS};JEMALLOC_EXPORT=")
  set(C_TESTLIB_INTEGRATION_DEFS "${C_TESTLIB_INTEGRATION_DEFS};JEMALLOC_EXPORT=")
  set(C_TESTLIB_UNIT_DEFS "${C_TESTLIB_UNIT_DEFS};JEMALLOC_EXPORT=")
  set(C_TESTLIB_STRESS_DEF "${C_TESTLIB_STRESS_DEF};JEMALLOC_EXPORT=")
  set(C_UNITETEST_DEFS "${C_UNITETEST_DEFS};JEMALLOC_EXPORT=")
  
  # C4018 signed/unsigned mismatch in timer.c
  set(COMMON_TESTLIB_CCFLAGS /wd4018)
endif()

###################################################
# JET prefixed version of jemalloc library
# necessary to compliment non-exported symbols
set(C_JETLIB jemalloc_jet${install_suffix})
add_library(${C_JETLIB} STATIC ${C_SRCS})
target_compile_definitions(${C_JETLIB} PRIVATE
    "${C_JETLIB_DEFS}")

##################################################################
# Util integration two object files helper
set(C_UTIL_INTEGRATION_SRCS
    src/nstime.c
    src/util.c
)

set(C_UTIL_INTEGRATION jemalloc_util_int_test${install_suffix})
add_library(${C_UTIL_INTEGRATION} OBJECT ${C_UTIL_INTEGRATION_SRCS})
if(C_UTIL_INTEGRATION_DEFS)
    target_compile_definitions(${C_UTIL_INTEGRATION} PRIVATE
        "${C_UTIL_INTEGRATION_DEFS}")
endif()

##################################################################
# Common source for Unit, Integration and stress test libraries
set(C_TESTLIB_SRCS
    test/src/btalloc.c
    test/src/btalloc_0.c
    test/src/btalloc_1.c
    test/src/math.c
    test/src/mq.c
    test/src/mtx.c
    test/src/SFMT.c
    test/src/test.c
    test/src/thd.c
    test/src/timer.c
)

################################################################
# Unit tests test library
# Unit test have access to all of the interfaces
# and we link to a JET library thus JEMALLOC_JET is automatically
# defined
set(C_TESTLIB_UNIT jemalloc_unit_test${install_suffix})
add_library(${C_TESTLIB_UNIT}  STATIC ${C_TESTLIB_SRCS})
target_include_directories(${C_TESTLIB_UNIT} PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/test/include)

if(COMMON_TESTLIB_CCFLAGS)
  target_compile_options(${C_TESTLIB_UNIT} PRIVATE
      "${COMMON_TESTLIB_CCFLAGS}")
endif()

target_compile_definitions(${C_TESTLIB_UNIT} PRIVATE
    "${C_TESTLIB_UNIT_DEFS}")

###############################################################
# Integration test library
set(C_TESTLIB_INTEGRATION jemalloc_int_test${install_suffix})
add_library(${C_TESTLIB_INTEGRATION} STATIC ${C_TESTLIB_SRCS})
target_include_directories(${C_TESTLIB_INTEGRATION} PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/test/include)

if(COMMON_TESTLIB_CCFLAGS)
  target_compile_options(${C_TESTLIB_INTEGRATION} PRIVATE
      "${COMMON_TESTLIB_CCFLAGS}")
endif()

target_compile_definitions(${C_TESTLIB_INTEGRATION} PRIVATE
    "${C_TESTLIB_INTEGRATION_DEFS}")

###################################################################
# Benchmark
# Stress test library to link with microbench
set(C_TESTLIB_STRESS jemalloc_test_stress${install_suffix})
add_library(${C_TESTLIB_STRESS} STATIC ${C_TESTLIB_SRCS})
target_include_directories(${C_TESTLIB_STRESS} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/test/include)

if(COMMON_TESTLIB_CCFLAGS)
  target_compile_options(${C_TESTLIB_STRESS} PRIVATE
      "${COMMON_TESTLIB_CCFLAGS}")
endif()

target_compile_definitions(${C_TESTLIB_STRESS} PRIVATE
    "${C_TESTLIB_STRESS_DEF}")

###################################################################
# Microbench
set(TESTS_STRESS
    test/stress/microbench.c
)

# Build benchmark
# This consumes C_JETLIB, Link to SO
foreach(sourcefile ${TESTS_STRESS})
  get_filename_component(exename ${sourcefile} NAME_WE)
  set(exename "${exename}${install_suffix}")
  add_executable(${exename} ${sourcefile})
  
  target_include_directories(${exename} PRIVATE 
      ${CMAKE_CURRENT_SOURCE_DIR}/test/include)

  target_compile_definitions(${exename} PRIVATE
          JEMALLOC_STRESS_TEST)

  target_link_libraries(${exename} ${C_JETLIB} ${C_TESTLIB_STRESS}
      ${LIBJEMALLOCSO})
endforeach(sourcefile ${TESTS_UNIT})

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

################################################################
# Build Unit tests
# This links to exported C_JETLIB static version of the library

set(TESTS_UNIT
    test/unit/a0.c
    test/unit/arena_reset.c
    test/unit/atomic.c
    test/unit/bitmap.c
    test/unit/ckh.c
    test/unit/decay.c
    test/unit/fork.c
    test/unit/hash.c
    test/unit/junk.c
    test/unit/junk_alloc.c
    test/unit/junk_free.c
    test/unit/lg_chunk.c
    test/unit/mallctl.c
    test/unit/math.c
    test/unit/mq.c
    test/unit/mtx.c
    test/unit/ph.c
    test/unit/prng.c
    test/unit/prof_accum.c
    test/unit/prof_active.c
    test/unit/prof_gdump.c
    test/unit/prof_idump.c
    test/unit/prof_reset.c
    test/unit/prof_thread_name.c
    test/unit/ql.c
    test/unit/qr.c
    test/unit/quarantine.c
    test/unit/rb.c
    test/unit/rtree.c
    test/unit/run_quantize.c
    test/unit/SFMT.c
    test/unit/size_classes.c
    test/unit/smoothstep.c
    test/unit/stats.c
    test/unit/ticker.c
    test/unit/nstime.c
    test/unit/tsd.c
    test/unit/util.c
    test/unit/witness.c
    test/unit/zero.c
)

# Turn off warnings
# test\unit\tsd.c: 4312: bitmap signed/unsigned
set(C_UNIT_TEST_CFLAGS ${COMMON_TESTLIB_CCFLAGS})
if(MSVC)
  set(C_UNIT_TEST_CFLAGS "${C_UNIT_TEST_CFLAGS};/wd4312")
endif()

foreach(sourcefile ${TESTS_UNIT})
  get_filename_component(exename ${sourcefile} NAME_WE)
  set(exename "${exename}${install_suffix}")
  add_executable(${exename} ${sourcefile}) 
  
  target_include_directories(${exename} PRIVATE 
      ${CMAKE_CURRENT_SOURCE_DIR}/test/include)

  if(C_UNIT_TEST_CFLAGS)
      target_compile_options(${exename} PRIVATE
          "${C_UNIT_TEST_CFLAGS}")
  endif()
  
  set_target_properties(${exename}
             PROPERTIES
             COMPILE_DEFINITIONS 
             "${C_UNITETEST_DEFS}"
  )
  
  target_link_libraries(${exename} ${C_JETLIB} ${C_TESTLIB_UNIT})
  add_test(NAME ${exename} COMMAND ${exename})
  add_dependencies(check ${exename})
endforeach(sourcefile ${TESTS_UNIT})


set(TESTS_INTEGRATION
    test/integration/aligned_alloc.c
    test/integration/allocated.c
    test/integration/sdallocx.c
    test/integration/mallocx.c
    test/integration/MALLOCX_ARENA.c
    test/integration/overflow.c
    test/integration/posix_memalign.c
    test/integration/rallocx.c
    test/integration/thread_arena.c
    test/integration/thread_tcache_enabled.c
    test/integration/xallocx.c
    test/integration/chunk.c
)

#######################################################################
# Build integration tests
# This links to SO library and UTIL_INTEGRATION
foreach(sourcefile ${TESTS_INTEGRATION})
  get_filename_component(exename ${sourcefile} NAME_WE)
  set(exename "${exename}${install_suffix}")
  add_executable(${exename} ${sourcefile} $<TARGET_OBJECTS:${C_UTIL_INTEGRATION}>)

  target_include_directories(${exename} PRIVATE 
      ${CMAKE_CURRENT_SOURCE_DIR}/test/include)
  
  target_compile_definitions(${exename} PRIVATE
          JEMALLOC_INTEGRATION_TEST)
  
  target_link_libraries(${exename} ${C_TESTLIB_INTEGRATION} ${LIBJEMALLOCSO})

  add_test(NAME ${exename} COMMAND ${exename})
  add_dependencies(check ${exename})
endforeach(sourcefile ${TESTS_UNIT})

