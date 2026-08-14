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
    # UBSan support library for C++ is in ubsan_standalone_cxx.a, so we have to include both.
    string(REPLACE "builtins.a" "ubsan_standalone_cxx.a" EXTRA_BUILTINS_LIBRARY "${BUILTINS_LIBRARY}")
endif ()

option (ENABLE_LLVM_LIBC_MATH "Use math from llvm-libc instead of glibc" ON)
if (NOT (ARCH_AMD64 OR ARCH_AARCH64))
    set(ENABLE_LLVM_LIBC_MATH OFF)
endif()

if (ENABLE_LLVM_LIBC_MATH)
    link_directories("${CMAKE_BINARY_DIR}/contrib/libllvmlibc-cmake")

    if (ARCH_AMD64)
        if (X86_ARCH_LEVEL VERSION_LESS 2)
            # Compat mode: single library, no dispatch
            target_link_libraries(global-libs INTERFACE libllvmlibc)
            set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")
        else()
            # Dispatch mode: v2/v3 variants with runtime CPU detection
            target_link_libraries(global-libs INTERFACE llvmlibc_dispatch libllvmlibc_x86_64_v2 libllvmlibc_x86_64_v3)
            set (DEFAULT_LIBS "${DEFAULT_LIBS} -lllvmlibc_dispatch -llibllvmlibc_x86_64_v2 -llibllvmlibc_x86_64_v3")
        endif()
    elseif (ARCH_AARCH64)
        target_link_libraries(global-libs INTERFACE libllvmlibc)
        set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")
    endif()
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
message(STATUS "Builtins library: ${BUILTINS_LIBRARY}")

# Link them first to have proper order for static linking.
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${BUILTINS_LIBRARY} -Wl,--no-whole-archive")

# Other libraries go last
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
