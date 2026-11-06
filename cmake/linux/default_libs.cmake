# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

# Wire compiler-rt runtimes (builtins/sanitizers/XRay) into the link flags.
include (cmake/compiler_rt_link.cmake)

# `libllvmlibc` supplies both the math functions and the SIMD memory functions
# (`memcpy`/`memmove`/`memset`/`memcmp`/`bcmp`/`memmem`). Disabling it on
# x86_64/aarch64 reverts all of them to the system libc, including `memcpy` —
# which then carries a versioned glibc symbol again (no portability shim).
option (ENABLE_LLVM_LIBC_MATH "Use math and memory functions from llvm-libc instead of glibc" ON)
if (NOT (ARCH_AMD64 OR ARCH_AARCH64))
    set(ENABLE_LLVM_LIBC_MATH OFF)
endif()

if (ENABLE_LLVM_LIBC_MATH)
    link_directories("${CMAKE_BINARY_DIR}/contrib/libllvmlibc-cmake")
    target_link_libraries(global-libs INTERFACE libllvmlibc)
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")

    if (NOT SANITIZE)
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-u,memcpy -Wl,-u,memmove -Wl,-u,memset -Wl,-u,memcmp")
    endif()
endif()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -ldl")
elseif (USE_MUSL)
    # musl itself is linked in cmake/musl.cmake. -nostartfiles: don't use glibc's crt*.o
    # from the sysroot; musl's own startup files (copied to stable paths in
    # contrib/musl-cmake) are wired here in the canonical order: crt1.o and crti.o go
    # through CMAKE_EXE_LINKER_FLAGS, which the link line places before all object
    # files, and crtn.o goes at the end of DEFAULT_LIBS, after all libraries — crti and
    # crtn provide the prologue/epilogue of the `.init`/`.fini` sections and must wrap
    # every other contribution to them.
    set (MUSL_CRT_DIR "${CMAKE_BINARY_DIR}/contrib/musl-cmake")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${MUSL_CRT_DIR}/crt1.o ${MUSL_CRT_DIR}/crti.o")
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -static -nostartfiles ${MUSL_CRT_DIR}/crtn.o")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
if (USE_MUSL)
    # musl provides pthread in libc.
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES musl)
else ()
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)
endif ()

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

if (USE_MUSL)
    include (cmake/musl.cmake)
endif()

if (NOT OS_ANDROID)
    if (NOT USE_MUSL)
        disable_dummy_launchers_if_needed()
        # Our compatibility layer doesn't build under Android, many errors in musl.
        add_subdirectory(base/glibc-compatibility)
        enable_dummy_launchers_if_needed()
    endif ()
    add_subdirectory(base/harmful)
endif ()
