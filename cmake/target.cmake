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
elseif (CMAKE_SYSTEM_NAME MATCHES "SunOS")
    set (OS_SUNOS 1)
    add_definitions(-D OS_SUNOS)
endif ()

if (CMAKE_CROSSCOMPILING)
    if (OS_DARWIN)
        # FIXME: broken dependencies
        set (ENABLE_GRPC OFF CACHE INTERNAL "") # no protobuf -> no grpc
        set (ENABLE_ICU OFF CACHE INTERNAL "")
        set (ENABLE_FASTOPS OFF CACHE INTERNAL "")
    elseif (OS_LINUX OR OS_ANDROID)
        if (ARCH_AARCH64)
            # FIXME: broken dependencies
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_SENTRY OFF CACHE INTERNAL "")
        elseif (ARCH_PPC64LE)
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_SENTRY OFF CACHE INTERNAL "")
        endif ()
    elseif (OS_FREEBSD)
        # FIXME: broken dependencies
        set (ENABLE_PARQUET OFF CACHE INTERNAL "")
        set (ENABLE_ORC OFF CACHE INTERNAL "")
        set (ENABLE_GRPC OFF CACHE INTERNAL "")
        set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
    elseif (EMSCRIPTEN)
        # == General options ==
        # emcmake cmake .. -DEMSCRIPTEN_SYSTEM_PROCESSOR=x86_64 to start cross-compiling
        # (by default emscripten in interpreted as 32bit arch which is not supported by ClickHouse)

        # __EMSCRIPTEN__ macro and EMSCRIPTEN global var are set automatically
        # when cross-compiling with emscripten-toolchain

        set (CMAKE_SYSROOT ${EMSCRIPTEN_SYSROOT})
        # Trying MVP build in the first integration
        set (ENABLE_UTILS 0)
        set (ENABLE_TESTS OFF)

        # TODO: maybe replace enable_only flag with generic one
        # TODO: pay attention to CAN_ADDRESS_2GB option
        # is used to select ClickHouse Build modes
        set (ENABLE_ONLY_EMSCRIPTEN_COMPATIBLE ON)

        # compiling in 64-bit mode
        # FIXME: time_t is still interpreted as int, because of the WasmVM limitations
        #        maybe use defines or explicit casts...
        # Exception handling is unsupported in 64-bit mode...
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sMEMORY64=2")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -sMEMORY64=2")
        set (CMAKE_LIBRARY_ARCHITECTURE "wasm64-emscripten")
        set (CMAKE_CXX_COMPILER_TARGET "wasm64-unknown-emscripten")
        set (CMAKE_C_COMPILER_TARGET "wasm64-unknown-emscripten")

        # == SIMD support ==
        # Enabling Emscripten SIMD support
        # should also pass --experimental-wasm-simd to Node emulator
        # (not all instructions from sets are supported, so enabling it is pretty risky)

        # FIXME: fails with unknown errors in llvm backend when enabled
        # other sse flags can be tested and added in cpu_features.cmake
        # set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msimd128 -msse -msse2")

        # == THREADS support ==
        set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
        # TODO: make alternative as threads work only in a limited number of browsers
        # TODO: try to compile without thread support at all
        # -pthread flag is automatically added by emcc if USE_PTHREADS is specified
        # set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sUSE_PTHREADS")

        # == COMPATIBILITY options ==
        # TODO: maybe should pass extra V8 flags to Node Here
        # set (CMAKE_CROSSCOMPILING_EMULATOR "${CMAKE_CROSSCOMPILING_EMULATOR};--simd memory64")

        # these macros have been removed from emcc (https://github.com/emscripten-core/emscripten/pull/11087)
        # but are required by some libs (e.g. Poco::Foundation)
        add_compile_definitions (FE_INVALID=1 __FE_DENORM=2 FE_DIVBYZERO=4 FE_OVERFLOW=8 FE_UNDERFLOW=16 FE_INEXACT=32 FE_ALL_EXCEPT=63)

        # emcc can only compile llvm assembly and inline assembly is not even portable
        set (OPENSSL_NO_ASM "1")

        # no such header in Emscripten
        add_compile_definitions (POCO_NO_LINUX_IF_PACKET_H)

        # Emscripten uses different mechanics for exception handling (planning to add libunwind)
        set (NEED_GCCEH false)
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    if (USE_MUSL)
        set (ENABLE_SENTRY OFF CACHE INTERNAL "")
        set (ENABLE_ODBC OFF CACHE INTERNAL "")
        set (ENABLE_GRPC OFF CACHE INTERNAL "")
        set (ENABLE_HDFS OFF CACHE INTERNAL "")
        set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
    endif ()

    # Don't know why but CXX_STANDARD doesn't work for cross-compilation
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILE_TARGET}")
endif ()
