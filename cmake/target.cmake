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
elseif (CMAKE_SYSTEM_NAME MATCHES "Windows")
    set (OS_WINDOWS 1)
    add_definitions(-D OS_WINDOWS)
    # Ask `windows.h` for the leaner set of declarations and, in particular, to not define
    # the `min`/`max` macros, which break every `std::min`/`std::max` in a translation unit
    # that includes it.
    add_definitions(-D WIN32_LEAN_AND_MEAN -D NOMINMAX)
    # The minimum supported Windows version, which gates which API declarations the headers
    # expose. 0x0A00 / 0x0A000000 is Windows 10. `NTDDI_VERSION` is the finer-grained
    # (per-update) counterpart of `_WIN32_WINNT` and has to be set explicitly to stay
    # consistent: left alone, mingw-w64 defaults it to the newest version its headers know
    # about rather than deriving it from `_WIN32_WINNT`, which both exposes APIs above our
    # stated baseline and selects header paths that mingw-w64 does not actually implement
    # (cctz's WinRT time-zone lookup pulls in a `windows.globalization.h` whose
    # `IReference<BYTE>` specializations do not compile).
    add_definitions(-D WINVER=0x0A00 -D _WIN32_WINNT=0x0A00 -D NTDDI_VERSION=0x0A000000)
    # Note: deliberately no `_POSIX_C_SOURCE`/`_GNU_SOURCE` here. mingw-w64 already exposes
    # the POSIX-flavoured CRT names we use (`strdup`, `fileno`, `getpid`, ...) without them,
    # and claiming POSIX conformance on a platform that does not have it makes third-party
    # feature detection pick branches that then fail to link - OpenSSL, for instance,
    # switches `CRYPTO_aligned_alloc` to `posix_memalign`, which mingw-w64 does not provide.
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
            set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
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
            set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
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
        elseif (ARCH_E2K)
            # added for future use
            # for now, we're compiling it natively.
        endif ()
    elseif (OS_FREEBSD)
        # FIXME: broken dependencies
        set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
        set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
    elseif (OS_WINDOWS)
        # The Windows port covers `clickhouse-client` and `clickhouse-local` only, and the
        # components below have no Windows support at all, so they are off unconditionally
        # rather than being left for the user to switch off. Everything that is merely
        # optional is instead governed by `ENABLE_LIBRARIES`, which the CI build sets to
        # `OFF`; see `ci/jobs/build_clickhouse.py`.
        #
        # `jemalloc` builds for mingw but its `pages_map` uses a Windows-specific path that
        # cannot honour our `MADV_DONTNEED`-based purging, and we do not need a custom
        # allocator for a client.
        set (ENABLE_JEMALLOC OFF CACHE INTERNAL "")
        set (ENABLE_TCMALLOC OFF CACHE INTERNAL "")
        # No Rust `x86_64-pc-windows-gnu` target in the CI image.
        set (ENABLE_RUST OFF CACHE INTERNAL "")
        # `Keeper` is server-side and pulls in `NuRaft`, which needs `epoll`.
        set (ENABLE_NURAFT OFF CACHE INTERNAL "")
        # The JIT and the DWARF parser both need a lot more of LLVM than we build here, and
        # neither applies to a PE binary.
        set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
        set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
        # `libfiu` injects failures through `dlsym` interposition.
        set (ENABLE_LIBFIU OFF CACHE INTERNAL "")
        # libarchive does have Windows support upstream, but it needs a hand-written
        # `config.h` for it (the one in `contrib/libarchive-cmake` describes a POSIX host).
        # Until that exists, `clickhouse-local` on Windows cannot read archives.
        set (ENABLE_LIBARCHIVE OFF CACHE INTERNAL "")
        # `liburing`/`numactl`/`librseq` are Linux kernel interfaces.
        set (ENABLE_LIBURING OFF CACHE INTERNAL "")
        set (ENABLE_NUMACTL OFF CACHE INTERNAL "")
        set (ENABLE_RSEQ OFF CACHE INTERNAL "")
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILER_TARGET}")
endif ()
