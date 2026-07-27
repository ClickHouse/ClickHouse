set (SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/snappy")

if (ARCH_S390X)
    set (SNAPPY_IS_BIG_ENDIAN 1)
else ()
    set (SNAPPY_IS_BIG_ENDIAN 0)
endif()

if (OS_WINDOWS)
    # None of these exist in mingw-w64. Snappy only uses them in its benchmark/test support
    # code (`mmap`-backed test inputs, `sysconf` for the page size), so the library itself
    # builds fine without them, but the headers are included unconditionally when the
    # corresponding `HAVE_*` is set.
    set (HAVE_BYTESWAP_H 0)
    set (HAVE_SYS_MMAN_H 0)
    set (HAVE_SYS_RESOURCE_H 0)
    set (HAVE_SYS_TIME_H 0)
    set (HAVE_SYS_UIO_H 0)
    set (HAVE_UNISTD_H 0)
    set (HAVE_FUNC_MMAP 0)
    set (HAVE_FUNC_SYSCONF 0)
    set (HAVE_WINDOWS_H 1)
else ()
    set (HAVE_BYTESWAP_H 1)
    set (HAVE_SYS_MMAN_H 1)
    set (HAVE_SYS_RESOURCE_H 1)
    set (HAVE_SYS_TIME_H 1)
    set (HAVE_SYS_UIO_H 1)
    set (HAVE_UNISTD_H 1)
    set (HAVE_FUNC_MMAP 1)
    set (HAVE_FUNC_SYSCONF 1)
endif ()

set (HAVE_BUILTIN_EXPECT 1)
set (HAVE_BUILTIN_CTZ 1)

if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 2)
    set (SNAPPY_HAVE_SSSE3 1)
    set (SNAPPY_HAVE_X86_CRC32 1)
else ()
    set (SNAPPY_HAVE_SSSE3 0)
    set (SNAPPY_HAVE_X86_CRC32 0)
endif ()

configure_file(
  "${SOURCE_DIR}/cmake/config.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/config.h")

if (OS_WINDOWS)
    # Selects whether `snappy-stubs-public.h` declares `struct iovec` itself (there is no
    # `sys/uio.h` to pull it in from).
    set(HAVE_SYS_UIO_H_01 0)
else ()
    set(HAVE_SYS_UIO_H_01 1)
endif ()

configure_file(
  "${SOURCE_DIR}/snappy-stubs-public.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/snappy-stubs-public.h")

add_library(_snappy "")
add_library(ch_contrib::snappy ALIAS _snappy)
target_sources(_snappy
  PRIVATE
    "${SOURCE_DIR}/snappy-internal.h"
    "${SOURCE_DIR}/snappy-stubs-internal.h"
    "${SOURCE_DIR}/snappy-c.cc"
    "${SOURCE_DIR}/snappy-sinksource.cc"
    "${SOURCE_DIR}/snappy-stubs-internal.cc"
    "${SOURCE_DIR}/snappy.cc")

target_include_directories(_snappy SYSTEM BEFORE PUBLIC ${SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_compile_definitions(_snappy PRIVATE -DHAVE_CONFIG_H)
