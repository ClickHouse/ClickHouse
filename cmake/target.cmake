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
elseif (CMAKE_SYSTEM_NAME MATCHES "Emscripten")
    # WebAssembly, through the Emscripten toolchain. Configure with `emcmake cmake ...`, which
    # sets CMAKE_SYSTEM_NAME and points CMAKE_TOOLCHAIN_FILE at Emscripten's own toolchain file.
    # This is experimental and not in CI, so expect it to be broken.
    set (OS_WASM 1)
    add_definitions(-D OS_WASM)
    # Note: unlike the other platforms, no `_GNU_SOURCE`. Emscripten's musl-derived libc declares
    # everything this tree needs without it (`MAP_ANONYMOUS`, for one), and defining it makes
    # OpenSSL select the GNU `strerror_r`, which returns `char *` and which musl does not have.
else ()
    message (FATAL_ERROR "Platform ${CMAKE_SYSTEM_NAME} is not supported")
endif ()

if (OS_WASM)
    # ClickHouse assumes a 64-bit `size_t` and 64-bit pointers pervasively - `1e12uz` literals in
    # `Core/Defines.h`, sizeof-equality static_asserts in ProfileEvents, and so on - so build for
    # the 64-bit Memory64 ABI rather than wasm32. Needs a recent engine (Node >= 23, Chrome >= 133).
    # This is not a choice, hence not an option: a wasm32 build of this tree is not expected to
    # work, and offering the knob would only move the failure to some unrelated place later.
    add_compile_options (-sMEMORY64=1)
    add_link_options (-sMEMORY64=1)

    # ClickHouse catches exceptions everywhere. Emscripten only emits throws by default and turns
    # every `catch` into a no-op, so enable the native WebAssembly exception-handling proposal.
    # It is an ABI flag: it has to be on for every translation unit and at the link.
    add_compile_options (-fwasm-exceptions)
    add_link_options (-fwasm-exceptions)

    # Emscripten implements pthreads on Web Workers plus SharedArrayBuffer, which needs the page
    # to be cross-origin isolated. Also an ABI flag, so compile and link both.
    add_compile_options (-pthread)
    add_link_options (-pthread)

    # Nothing here can work in a WebAssembly sandbox: there are no raw sockets, no subprocesses,
    # no `dlopen`, no JIT and no architecture-specific code paths.
    set (ENABLE_JEMALLOC OFF CACHE INTERNAL "")
    set (ENABLE_GRPC OFF CACHE INTERNAL "")
    # Protobuf needs a `protoc` that runs on the host, and the nested native configure at the
    # bottom of the top-level `CMakeLists.txt` would be handed `emcc` as its host compiler.
    # Arrow, Parquet and ORC hard-depend on it, so they go with it.
    set (ENABLE_PROTOBUF OFF CACHE INTERNAL "")
    set (ENABLE_PARQUET OFF CACHE INTERNAL "")
    set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
    set (ENABLE_HDFS OFF CACHE INTERNAL "")
    set (ENABLE_MYSQL OFF CACHE INTERNAL "")
    set (ENABLE_LIBPQXX OFF CACHE INTERNAL "")
    set (ENABLE_NURAFT OFF CACHE INTERNAL "")
    set (ENABLE_KAFKA OFF CACHE INTERNAL "")
    set (ENABLE_AMQPCPP OFF CACHE INTERNAL "")
    set (ENABLE_NATS OFF CACHE INTERNAL "")
    set (ENABLE_CASSANDRA OFF CACHE INTERNAL "")
    set (ENABLE_AZURE_BLOB_STORAGE OFF CACHE INTERNAL "")
    set (ENABLE_AWS_S3 OFF CACHE INTERNAL "")
    set (ENABLE_S3 OFF CACHE INTERNAL "")
    set (ENABLE_HIVE OFF CACHE INTERNAL "")
    set (ENABLE_ODBC OFF CACHE INTERNAL "")
    set (ENABLE_LDAP OFF CACHE INTERNAL "")
    set (ENABLE_KRB5 OFF CACHE INTERNAL "")
    set (ENABLE_GSASL_LIBRARY OFF CACHE INTERNAL "")
    set (ENABLE_CURL OFF CACHE INTERNAL "")
    # `libssh` needs raw sockets, and its config headers are pregenerated per platform.
    set (ENABLE_SSH OFF CACHE INTERNAL "")
    set (ENABLE_RUST OFF CACHE INTERNAL "")
    set (ENABLE_DELTA_KERNEL_RS OFF CACHE INTERNAL "")
    set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
    # A host WebAssembly runtime inside a WebAssembly sandbox: `WasmEdge` runs guest modules
    # from native code that this target cannot provide. `wasmtime`, the other engine, is Rust,
    # so `ENABLE_RUST` above already covers it.
    set (ENABLE_WASMEDGE OFF CACHE INTERNAL "")
    set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
    set (ENABLE_ROCKSDB OFF CACHE INTERNAL "")
    set (ENABLE_VECTORSCAN OFF CACHE INTERNAL "")
    set (ENABLE_FASTOPS OFF CACHE INTERNAL "")
    # BLAKE3 is Rust, and its build pulls in a subset of llvm-project.
    set (ENABLE_BLAKE3 OFF CACHE INTERNAL "")
    # No assembler, so OpenSSL has to come from its portable C sources.
    set (OPENSSL_NO_ASM ON CACHE INTERNAL "")
    # No `libunwind`, and WebAssembly cannot walk its own call stack from user code.
    set (USE_UNWIND OFF CACHE INTERNAL "")
    set (GLIBC_COMPATIBILITY OFF CACHE INTERNAL "")
    # The Emscripten sysroot supplies its own math.
    set (ENABLE_LLVM_LIBC_MATH OFF CACHE INTERNAL "")
    # Emscripten's libc++ is not the patched one from `contrib/libcxx-cmake`, so an exception
    # carries no stack trace. See `base/defines.h`.
    add_definitions (-D STD_EXCEPTION_HAS_STACK_TRACE=0)
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
    elseif (OS_WASM)
        # Handled in the OS_WASM block above: it has to run before this one, because the
        # `CMAKE_CROSSCOMPILING` check below it needs `OS_WASM` to already be set.
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILER_TARGET}")
endif ()
