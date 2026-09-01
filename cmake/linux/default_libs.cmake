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

    if (ARCH_AARCH64)
        # AdvSIMD/MTE string routines from ARM's optimized-routines
        # (see contrib/optimized-routines-cmake).
        link_directories("${CMAKE_BINARY_DIR}/contrib/optimized-routines-cmake")
        target_link_libraries(global-libs INTERFACE aor)
        set (DEFAULT_LIBS "${DEFAULT_LIBS} -laor")
    endif()

    if (NOT SANITIZE)
        # Force every llvm-libc member into the link ahead of all objects and archives
        # (linker flags precede them on the link line; -L placement does not matter to ld).
        # Symbol resolution in archives is first-definition-wins in command-line order, and
        # the Rust static libraries precede libllvmlibc there, so without this the libm
        # functions Rust's compiler_builtins exports (cbrt, fmod, fma - baseline-CPU builds)
        # would shadow the llvm-libc ones whenever symbol localization is bypassed. With all
        # members pre-loaded, later archives can never supply an already-defined symbol, so
        # this also covers the mem functions without per-symbol -u flags.
        #
        # Skipped under sanitizers, where the interceptors must wrap the libc mem functions
        # instead (same reason the -u forcing was skipped before).
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive -llibllvmlibc -Wl,--no-whole-archive")
        if (ARCH_AARCH64)
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive -laor -Wl,--no-whole-archive")
        endif()
        # Redundant with --whole-archive above, but kept as a backstop for the mem functions
        # in case the whole-archive link is ever weakened or repositioned.
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-u,memcpy -Wl,-u,memmove -Wl,-u,memset -Wl,-u,memcmp")
    endif()
endif()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -ldl")
elseif (USE_MUSL)
    # musl itself is linked in cmake/musl.cmake. -nostartfiles: don't use glibc's crt*.o
    # from the sysroot; musl's own are added per executable (MUSL_CRT_START_OBJECTS/MUSL_CRT_END_OBJECTS).
    # -static / -static-pie is added to CMAKE_EXE_LINKER_FLAGS in the main CMakeLists.
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -nostartfiles")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
if (USE_MUSL)
    # Sanitizer builds link the copy of musl with the intercepted functions
    # renamed to __real_* (see contrib/compiler-rt-cmake). Standalone UBSan is
    # the one sanitizer without libc interceptors (no REAL() bindings), so it
    # links the plain musl; contrib/compiler-rt-cmake does not produce
    # musl_intercepted for it. Any future sanitizer mode intentionally falls
    # into the musl_intercepted branch: if compiler-rt does not build the
    # archive for it, the configure fails loudly instead of silently linking
    # a libc whose functions the runtime expects to intercept.
    if (SANITIZE AND NOT SANITIZE STREQUAL "undefined")
        set (MUSL_LIBC_TARGET musl_intercepted)
    else ()
        set (MUSL_LIBC_TARGET musl)
    endif ()
    # musl provides pthread in libc.
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES ${MUSL_LIBC_TARGET})
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
