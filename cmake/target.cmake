if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "Android")
    # This is a toy configuration and not in CI, so expect it to be broken.
    # Use cmake flags such as: -DCMAKE_TOOLCHAIN_FILE=~/ch2/android-ndk-r21d/build/cmake/android.toolchain.cmake -DANDROID_ABI=arm64-v8a -DANDROID_PLATFORM=28
    set (OS_ANDROID 1)
    add_definitions(-D OS_ANDROID)
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    set (OS_FREEBSD 1)
    add_definitions(-D OS_FREEBSD)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_DARWIN 1)
    add_definitions(-D OS_DARWIN)
    # For MAP_ANON/MAP_ANONYMOUS
    add_definitions(-D _DARWIN_C_SOURCE)
elseif (CMAKE_SYSTEM_NAME MATCHES "SunOS")
    set (OS_SUNOS 1)
    add_definitions(-D OS_SUNOS)
else ()
    message (FATAL_ERROR "Platform ${CMAKE_SYSTEM_NAME} is not supported")
endif ()

# Since we always use toolchain files to generate hermetic builds, cmake will
# always think it's a cross-compilation, See
# https://cmake.org/cmake/help/latest/variable/CMAKE_CROSSCOMPILING.html
#
# This will slow down cmake configuration and compilation. For instance, LLVM
# will try to configure NATIVE LLVM targets with all tests enabled (You'll see
# Building native llvm-tblgen...).
#
# Here, we set it manually by checking the system name and processor.
if (${CMAKE_SYSTEM_NAME} STREQUAL ${CMAKE_HOST_SYSTEM_NAME} AND ${CMAKE_SYSTEM_PROCESSOR} STREQUAL ${CMAKE_HOST_SYSTEM_PROCESSOR})
    set (CMAKE_CROSSCOMPILING 0)
endif ()

if (CMAKE_CROSSCOMPILING)
    if (OS_DARWIN)
        set (ENABLE_FASTOPS OFF CACHE INTERNAL "")
    elseif (OS_LINUX OR OS_ANDROID)
        if (ARCH_PPC64LE)
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
        elseif (ARCH_RISCV64)
            # RISC-V support is preliminary
            set (GLIBC_COMPATIBILITY OFF CACHE INTERNAL "")
            set (ENABLE_LDAP OFF CACHE INTERNAL "")
            set (OPENSSL_NO_ASM ON CACHE INTERNAL "")
            set (ENABLE_JEMALLOC ON CACHE INTERNAL "")
            set (ENABLE_PARQUET OFF CACHE INTERNAL "")
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_HDFS OFF CACHE INTERNAL "")
            set (ENABLE_MYSQL OFF CACHE INTERNAL "")
            # It might be ok, but we need to update 'sysroot'
            set (ENABLE_RUST OFF CACHE INTERNAL "")
        elseif (ARCH_S390X)
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_RUST OFF CACHE INTERNAL "")
    elseif (ARCH_LOONGARCH64)
            set (GLIBC_COMPATIBILITY OFF CACHE INTERNAL "")
            set (ENABLE_LDAP OFF CACHE INTERNAL "")
            set (OPENSSL_NO_ASM ON CACHE INTERNAL "")
            set (ENABLE_JEMALLOC OFF CACHE INTERNAL "")
            set (ENABLE_PARQUET OFF CACHE INTERNAL "")
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_HDFS OFF CACHE INTERNAL "")
            set (ENABLE_MYSQL OFF CACHE INTERNAL "")
            set (ENABLE_RUST OFF CACHE INTERNAL "")
            set (ENABLE_LIBPQXX OFF CACHE INTERNAL "")
            set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
            set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
            set (ENABLE_BLAKE3 OFF CACHE INTERNAL "")
        endif ()
    elseif (OS_FREEBSD)
        # FIXME: broken dependencies
        set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
        set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILER_TARGET}")
endif ()
