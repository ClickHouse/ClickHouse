# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

set (BUILTINS_LIBRARY "")
if (USE_SYSTEM_COMPILER_RT)
    # We need builtins from Clang
    execute_process (COMMAND
        ${CMAKE_CXX_COMPILER} --target=${CMAKE_CXX_COMPILER_TARGET} --print-libgcc-file-name --rtlib=compiler-rt
        OUTPUT_VARIABLE BUILTINS_LIBRARY
        COMMAND_ERROR_IS_FATAL ANY
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif ()

if (NOT EXISTS "${BUILTINS_LIBRARY}")
    # In the past we used to fallback into using libgcc, which then required sysroot to include gcc libraries.
    # Now we build the compiler-rt from source instead so we are independent of the target system's gcc.
    include (cmake/build_clang_builtin.cmake)
    build_clang_builtin(${CMAKE_CXX_COMPILER_TARGET} BUILTINS_LIBRARY)
elseif (SANITIZE STREQUAL undefined)
    # Apparently, in clang-19, the UBSan support library for C++ was moved out into ubsan_standalone_cxx.a, so we have to include both.
    string(REPLACE "builtins.a" "ubsan_standalone_cxx.a" EXTRA_BUILTINS_LIBRARY "${BUILTINS_LIBRARY}")
endif ()

if (ENABLE_LLVM_LIBC_MATH)
    include (cmake/libllvmlibc.cmake)

    set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")
endif()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -ldl")
elseif (USE_MUSL)
    # musl is linked via target_link_libraries in cmake/musl.cmake
    # Use -nostartfiles to prevent linker from using glibc's crt*.o from sysroot
    # We will provide musl's CRT objects explicitly via cmake/musl.cmake
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -static -nostartfiles")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")
message(STATUS "Builtins library: ${BUILTINS_LIBRARY}")

# Link them first to have proper order for static linking.
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${BUILTINS_LIBRARY} -Wl,--no-whole-archive")

# Other libraries go last
set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
if (USE_MUSL)
    # musl includes pthread implementation in libmusl.a, no separate library needed
    # Just link to musl target which is already set up
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES musl)
else ()
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)
endif ()

# Set MUSL_ARCH early, BEFORE including cxx.cmake which needs it for libcxx/libcxxabi includes
# This must be done before the cxx.cmake include because libcxxabi-cmake/CMakeLists.txt uses MUSL_ARCH
if (USE_MUSL)
    if (ARCH_AMD64)
        set(MUSL_ARCH "x86_64" CACHE INTERNAL "Musl architecture")
    elseif (ARCH_AARCH64)
        set(MUSL_ARCH "aarch64" CACHE INTERNAL "Musl architecture")
    elseif (ARCH_PPC64LE)
        set(MUSL_ARCH "powerpc64" CACHE INTERNAL "Musl architecture")
    elseif (ARCH_S390X)
        set(MUSL_ARCH "s390x" CACHE INTERNAL "Musl architecture")
    elseif (ARCH_RISCV64)
        set(MUSL_ARCH "riscv64" CACHE INTERNAL "Musl architecture")
    elseif (ARCH_LOONGARCH64)
        set(MUSL_ARCH "loongarch64" CACHE INTERNAL "Musl architecture")
    else()
        message(FATAL_ERROR "Unsupported architecture for musl: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
    message(STATUS "MUSL_ARCH set early to: ${MUSL_ARCH}")
endif()

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

# Include musl build and link configuration
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
