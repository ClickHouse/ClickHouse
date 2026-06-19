# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

# Wire compiler-rt runtimes (builtins/sanitizers/XRay) into the link flags.
include (cmake/compiler_rt_link.cmake)

option (ENABLE_LLVM_LIBC_MATH "Use math from llvm-libc instead of glibc" ON)
if (NOT (ARCH_AMD64 OR ARCH_AARCH64))
    set(ENABLE_LLVM_LIBC_MATH OFF)
endif()

if (ENABLE_LLVM_LIBC_MATH)
    link_directories("${CMAKE_BINARY_DIR}/contrib/libllvmlibc-cmake")
    target_link_libraries(global-libs INTERFACE libllvmlibc)
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")
endif()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -ldl")
elseif (USE_MUSL)
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -static -lc")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

if (NOT OS_ANDROID)
    if (NOT USE_MUSL)
        disable_dummy_launchers_if_needed()
        # Our compatibility layer doesn't build under Android, many errors in musl.
        add_subdirectory(base/glibc-compatibility)
        enable_dummy_launchers_if_needed()
    endif ()
    add_subdirectory(base/harmful)
endif ()

# Force the strong glibc-compatibility `memcpy` to always be linked, so it keeps
# precedence over libllvmlibc's weak `memcpy`. Both are static archives and lld
# resolves a `memcpy` reference from whichever archive it reaches first; because
# CMake can place libllvmlibc ahead of libmemcpy, the `weak` attribute alone
# does not guarantee precedence. `--whole-archive` makes the strong definition
# unconditionally present, so it wins regardless of archive order. The path is
# passed via CMAKE_EXE_LINKER_FLAGS (not the memcpy target) for the same reason
# as compiler-rt: $<LINK_LIBRARY:WHOLE_ARCHIVE,...> does not survive the
# global-libs INTERFACE_LINK_LIBRARIES indirection. memcpy stays in global-libs
# (above) for the build-order dependency.
#
# Not under sanitizers: there, compiler_rt_link.cmake already force-links the
# sanitizer runtime's own `memcpy` interceptor with `--whole-archive`. Adding a
# second `--whole-archive` `memcpy` lets the custom one defeat the interceptor,
# bypassing the sanitizer's shadow-memory handling and segfaulting (the binary's
# perf does not matter under sanitizers, so the interceptor must win).
if (TARGET memcpy AND NOT SANITIZE)
    # Literal path (not the target / $<TARGET_FILE:...>) because CMAKE_EXE_LINKER_FLAGS
    # does not expand generator expressions, same as compiler_rt_link.cmake.
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${CMAKE_BINARY_DIR}/base/glibc-compatibility/memcpy/libmemcpy.a -Wl,--no-whole-archive")
endif ()
