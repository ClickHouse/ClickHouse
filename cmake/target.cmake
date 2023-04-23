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
else ()
    message (FATAL_ERROR "Platform ${CMAKE_SYSTEM_NAME} is not supported")
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
