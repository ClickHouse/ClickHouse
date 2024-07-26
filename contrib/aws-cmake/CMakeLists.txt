set(ENABLE_AWS_S3_DEFAULT OFF)

if(ENABLE_LIBRARIES AND (OS_LINUX OR OS_DARWIN) AND TARGET OpenSSL::Crypto)
    set(ENABLE_AWS_S3_DEFAULT ON)
endif()

option(ENABLE_AWS_S3 "Enable AWS S3" ${ENABLE_AWS_S3_DEFAULT})

if(ENABLE_AWS_S3)
    if(NOT TARGET OpenSSL::Crypto)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS SDK without OpenSSL")
    elseif(NOT (OS_LINUX OR OS_DARWIN))
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS SDK with platform ${CMAKE_SYSTEM_NAME}")
    endif()
endif()

if(NOT ENABLE_AWS_S3)
    message(STATUS "Not using AWS S3")
    return()
endif()


# Utilities.
include("${ClickHouse_SOURCE_DIR}/contrib/aws-cmake/AwsFeatureTests.cmake")
include("${ClickHouse_SOURCE_DIR}/contrib/aws-cmake/AwsThreadAffinity.cmake")
include("${ClickHouse_SOURCE_DIR}/contrib/aws-cmake/AwsThreadName.cmake")
include("${ClickHouse_SOURCE_DIR}/contrib/aws-cmake/AwsSIMD.cmake")
include("${ClickHouse_SOURCE_DIR}/contrib/aws-crt-cpp/cmake/AwsGetVersion.cmake")


# Gather sources and options.
set(AWS_SOURCES)
set(AWS_PUBLIC_INCLUDES)
set(AWS_PRIVATE_INCLUDES)
set(AWS_PUBLIC_COMPILE_DEFS)
set(AWS_PRIVATE_COMPILE_DEFS)
set(AWS_PRIVATE_LIBS)

list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DINTEL_NO_ITTNOTIFY_API")

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DDEBUG_BUILD")
endif()

set(ENABLE_OPENSSL_ENCRYPTION ON)
if (ENABLE_OPENSSL_ENCRYPTION)
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DENABLE_OPENSSL_ENCRYPTION")
endif()

set(USE_S2N ON)
if (USE_S2N)
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DUSE_S2N")
endif()


# Directories.
SET(AWS_SDK_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws")
SET(AWS_SDK_CORE_DIR "${AWS_SDK_DIR}/src/aws-cpp-sdk-core")
SET(AWS_SDK_S3_DIR "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-s3")

SET(AWS_AUTH_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-auth")
SET(AWS_CAL_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-cal")
SET(AWS_CHECKSUMS_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-checksums")
SET(AWS_COMMON_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-common")
SET(AWS_COMPRESSION_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-compression")
SET(AWS_CRT_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-crt-cpp")
SET(AWS_EVENT_STREAM_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-event-stream")
SET(AWS_HTTP_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-http")
SET(AWS_IO_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-io")
SET(AWS_MQTT_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-mqtt")
SET(AWS_S2N_TLS_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-s2n-tls")
SET(AWS_S3_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-s3")
SET(AWS_SDKUTILS_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-sdkutils")


# aws-cpp-sdk-core
file(GLOB AWS_SDK_CORE_SRC
    "${AWS_SDK_CORE_DIR}/source/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/bearer-token-provider/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/signer/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/signer-provider/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/client/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/config/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/config/defaults/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/endpoint/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/endpoint/internal/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/external/cjson/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/external/tinyxml2/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/http/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/http/crt/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/http/standard/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/internal/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/monitoring/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/net/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/net/linux-shared/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/platform/linux-shared/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/smithy/tracing/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/base64/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/component-registry/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/crypto/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/crypto/factory/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/crypto/openssl/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/event/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/json/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/logging/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/memory/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/memory/stl/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/stream/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/threading/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/xml/*.cpp"
)

if(OS_LINUX OR OS_DARWIN)
    file(GLOB AWS_SDK_CORE_NET_SRC "${AWS_SDK_CORE_DIR}/source/net/linux-shared/*.cpp")
    file(GLOB AWS_SDK_CORE_PLATFORM_SRC "${AWS_SDK_CORE_DIR}/source/platform/linux-shared/*.cpp")
else()
    file(GLOB AWS_SDK_CORE_NET_SRC "${AWS_SDK_CORE_DIR}/source/net/*.cpp")
    set(AWS_SDK_CORE_PLATFORM_SRC)
endif()

OPTION(USE_AWS_MEMORY_MANAGEMENT "Aws memory management" OFF)
configure_file("${AWS_SDK_CORE_DIR}/include/aws/core/SDKConfig.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/core/SDKConfig.h" @ONLY)

aws_get_version(AWS_CRT_CPP_VERSION_MAJOR AWS_CRT_CPP_VERSION_MINOR AWS_CRT_CPP_VERSION_PATCH FULL_VERSION GIT_HASH)
configure_file("${AWS_CRT_DIR}/include/aws/crt/Config.h.in" "${CMAKE_CURRENT_BINARY_DIR}/include/aws/crt/Config.h" @ONLY)

list(APPEND AWS_SOURCES ${AWS_SDK_CORE_SRC} ${AWS_SDK_CORE_NET_SRC} ${AWS_SDK_CORE_PLATFORM_SRC})

list(APPEND AWS_PUBLIC_INCLUDES
    "${AWS_SDK_CORE_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include"
)


# aws-cpp-sdk-s3
file(GLOB AWS_SDK_S3_SRC
    "${AWS_SDK_S3_DIR}/source/*.cpp"
    "${AWS_SDK_S3_DIR}/source/model/*.cpp"
)

list(APPEND AWS_SOURCES ${AWS_SDK_S3_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_SDK_S3_DIR}/include/")


# aws-c-auth
file(GLOB AWS_AUTH_SRC
    "${AWS_AUTH_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_AUTH_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_AUTH_DIR}/include/")


# aws-c-cal
file(GLOB AWS_CAL_SRC
    "${AWS_CAL_DIR}/source/*.c"
)

if (ENABLE_OPENSSL_ENCRYPTION)
    file(GLOB AWS_CAL_OS_SRC
        "${AWS_CAL_DIR}/source/unix/*.c"
    )
    list(APPEND AWS_PRIVATE_LIBS OpenSSL::Crypto)
endif()

list(APPEND AWS_SOURCES ${AWS_CAL_SRC} ${AWS_CAL_OS_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_CAL_DIR}/include/")


# aws-c-event-stream
file(GLOB AWS_EVENT_STREAM_SRC
    "${AWS_EVENT_STREAM_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_EVENT_STREAM_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_EVENT_STREAM_DIR}/include/")


# aws-c-common
file(GLOB AWS_COMMON_SRC
    "${AWS_COMMON_DIR}/source/*.c"
    "${AWS_COMMON_DIR}/source/external/*.c"
    "${AWS_COMMON_DIR}/source/posix/*.c"
    "${AWS_COMMON_DIR}/source/linux/*.c"
)

file(GLOB AWS_COMMON_ARCH_SRC
    "${AWS_COMMON_DIR}/source/arch/generic/*.c"
)

if (AWS_ARCH_INTEL)
    file(GLOB AWS_COMMON_ARCH_SRC
        "${AWS_COMMON_DIR}/source/arch/intel/cpuid.c"
        "${AWS_COMMON_DIR}/source/arch/intel/asm/*.c"
    )
elseif (AWS_ARCH_ARM64 OR AWS_ARCH_ARM32)
    if (AWS_HAVE_AUXV)
        file(GLOB AWS_COMMON_ARCH_SRC
            "${AWS_COMMON_DIR}/source/arch/arm/asm/*.c"
        )
    endif()
endif()

set(AWS_COMMON_AVX2_SRC)
if (HAVE_AVX2_INTRINSICS)
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DUSE_SIMD_ENCODING")
    set(AWS_COMMON_AVX2_SRC "${AWS_COMMON_DIR}/source/arch/intel/encoding_avx2.c")
    set_source_files_properties(${AWS_COMMON_AVX2_SRC} PROPERTIES COMPILE_FLAGS "${AVX2_CFLAGS}")
endif()

configure_file("${AWS_COMMON_DIR}/include/aws/common/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/common/config.h" @ONLY)

list(APPEND AWS_SOURCES ${AWS_COMMON_SRC} ${AWS_COMMON_ARCH_SRC} ${AWS_COMMON_AVX2_SRC})

list(APPEND AWS_PUBLIC_INCLUDES
    "${AWS_COMMON_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include"
)


# aws-checksums
file(GLOB AWS_CHECKSUMS_SRC
    "${AWS_CHECKSUMS_DIR}/source/*.c"
    "${AWS_CHECKSUMS_DIR}/source/intel/*.c"
    "${AWS_CHECKSUMS_DIR}/source/intel/asm/*.c"
    "${AWS_CHECKSUMS_DIR}/source/arm/*.c"
)

if(AWS_ARCH_INTEL AND AWS_HAVE_GCC_INLINE_ASM)
    file(GLOB AWS_CHECKSUMS_ARCH_SRC
            "${AWS_CHECKSUMS_DIR}/source/intel/asm/*.c"
    )
endif()

if (AWS_ARCH_ARM64)
    file(GLOB AWS_CHECKSUMS_ARCH_SRC
        "${AWS_CHECKSUMS_DIR}/source/arm/*.c"
    )
    set_source_files_properties("${AWS_CHECKSUMS_DIR}/source/arm/crc32c_arm.c" PROPERTIES COMPILE_FLAGS -march=armv8-a+crc)
elseif (AWS_ARCH_ARM32)
    if (AWS_ARM32_CRC)
        file(GLOB AWS_CHECKSUMS_ARCH_SRC
            "${AWS_CHECKSUMS_DIR}/source/arm/*.c"
            "${AWS_CHECKSUMS_DIR}/source/arm/asm/*.c"
        )
        set_source_files_properties(source/arm/crc32c_arm.c PROPERTIES COMPILE_FLAGS -march=armv8-a+crc)
    endif()
endif()

list(APPEND AWS_SOURCES ${AWS_CHECKSUMS_SRC} ${AWS_CHECKSUMS_ARCH_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_CHECKSUMS_DIR}/include/")


# aws-c-io
file(GLOB AWS_IO_SRC
    "${AWS_IO_DIR}/source/*.c"
)

if (OS_LINUX)
    file(GLOB AWS_IO_OS_SRC
        "${AWS_IO_DIR}/source/linux/*.c"
        "${AWS_IO_DIR}/source/posix/*.c"
    )
elseif (OS_DARWIN)
    file(GLOB AWS_IO_OS_SRC
        "${AWS_IO_DIR}/source/bsd/*.c"
        "${AWS_IO_DIR}/source/posix/*.c"
    )
endif()

set(AWS_IO_TLS_SRC)
if (USE_S2N)
    file(GLOB AWS_IO_TLS_SRC
            "${AWS_IO_DIR}/source/s2n/*.c"
    )
endif()

list(APPEND AWS_SOURCES ${AWS_IO_SRC} ${AWS_IO_OS_SRC} ${AWS_IO_TLS_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_IO_DIR}/include/")


# aws-s2n-tls
if (USE_S2N)
    file(GLOB AWS_S2N_TLS_SRC
        "${AWS_S2N_TLS_DIR}/crypto/*.c"
        "${AWS_S2N_TLS_DIR}/error/*.c"
        "${AWS_S2N_TLS_DIR}/stuffer/*.c"
        "${AWS_S2N_TLS_DIR}/pq-crypto/*.c"
        "${AWS_S2N_TLS_DIR}/pq-crypto/kyber_r3/*.c"
        "${AWS_S2N_TLS_DIR}/tls/*.c"
        "${AWS_S2N_TLS_DIR}/tls/extensions/*.c"
        "${AWS_S2N_TLS_DIR}/utils/*.c"
    )

    list(APPEND AWS_SOURCES ${AWS_S2N_TLS_SRC})

    list(APPEND AWS_PRIVATE_INCLUDES
        "${AWS_S2N_TLS_DIR}/"
        "${AWS_S2N_TLS_DIR}/api/"
    )
endif()


# aws-crt-cpp
file(GLOB AWS_CRT_SRC
    "${AWS_CRT_DIR}/source/*.cpp"
    "${AWS_CRT_DIR}/source/auth/*.cpp"
    "${AWS_CRT_DIR}/source/crypto/*.cpp"
    "${AWS_CRT_DIR}/source/endpoints/*.cpp"
    "${AWS_CRT_DIR}/source/external/*.cpp"
    "${AWS_CRT_DIR}/source/http/*.cpp"
    "${AWS_CRT_DIR}/source/io/*.cpp"
)

list(APPEND AWS_SOURCES ${AWS_CRT_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_CRT_DIR}/include/")


# aws-c-mqtt
file(GLOB AWS_MQTT_SRC
    "${AWS_MQTT_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_MQTT_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_MQTT_DIR}/include/")


# aws-c-http
file(GLOB AWS_HTTP_SRC
    "${AWS_HTTP_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_HTTP_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_HTTP_DIR}/include/")


# aws-c-compression
file(GLOB AWS_COMPRESSION_SRC
    "${AWS_COMPRESSION_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_COMPRESSION_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_COMPRESSION_DIR}/include/")


# aws-c-s3
file(GLOB AWS_S3_SRC
    "${AWS_S3_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_S3_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_S3_DIR}/include/")


# aws-c-sdkutils
file(GLOB AWS_SDKUTILS_SRC
    "${AWS_SDKUTILS_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_SDKUTILS_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_SDKUTILS_DIR}/include/")


# Add library.
add_library(_aws ${AWS_SOURCES})

target_include_directories(_aws SYSTEM BEFORE PUBLIC ${AWS_PUBLIC_INCLUDES})
target_include_directories(_aws SYSTEM BEFORE PRIVATE ${AWS_PRIVATE_INCLUDES})
target_compile_definitions(_aws PUBLIC ${AWS_PUBLIC_COMPILE_DEFS})
target_compile_definitions(_aws PRIVATE ${AWS_PRIVATE_COMPILE_DEFS})
target_link_libraries(_aws PRIVATE ${AWS_PRIVATE_LIBS})

aws_set_thread_affinity_method(_aws)
aws_set_thread_name_method(_aws)

# The library is large - avoid bloat.
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_aws PRIVATE -g0)
endif()

add_library(ch_contrib::aws_s3 ALIAS _aws)
