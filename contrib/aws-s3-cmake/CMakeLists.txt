if(NOT OS_FREEBSD)
    option(ENABLE_S3 "Enable S3" ${ENABLE_LIBRARIES})
elseif(ENABLE_S3)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use S3 on FreeBSD")
endif()

if(NOT ENABLE_S3)
    message(STATUS "Not using S3")
    return()
endif()

SET(AWS_S3_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3")
SET(AWS_CORE_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core")
SET(AWS_CRT_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp")
SET(AWS_CRT_S2N_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/s2n")
SET(AWS_CRT_C_IO_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-io")
SET(AWS_CRT_C_MQTT_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-mqtt")
SET(AWS_CRT_C_AUTH_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-auth")
SET(AWS_CRT_C_SDKUTILS_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-sdkutils")
SET(AWS_CRT_C_CAL_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-cal")
SET(AWS_CRT_S2N_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/s2n")
SET(AWS_CRT_C_HTTP_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-http")
SET(AWS_CRT_C_COMPRESSION_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-compression")
SET(AWS_CRT_C_S3_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/crt/aws-crt-cpp/crt/aws-c-s3")
SET(AWS_CHECKSUMS_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-checksums")
SET(AWS_COMMON_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-common")
SET(AWS_EVENT_STREAM_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-c-event-stream")

OPTION(USE_AWS_MEMORY_MANAGEMENT "Aws memory management" OFF)
configure_file("${AWS_CORE_LIBRARY_DIR}/include/aws/core/SDKConfig.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/core/SDKConfig.h" @ONLY)

configure_file("${AWS_COMMON_LIBRARY_DIR}/include/aws/common/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/common/config.h" @ONLY)

file(GLOB AWS_CORE_SOURCES
    "${AWS_CORE_LIBRARY_DIR}/source/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/auth/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/auth/signer/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/client/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/http/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/http/standard/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/config/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/config/defaults/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/external/cjson/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/external/tinyxml2/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/internal/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/monitoring/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/net/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/linux-shared/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/platform/linux-shared/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/base64/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/event/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/crypto/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/crypto/openssl/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/crypto/factory/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/json/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/logging/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/memory/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/memory/stl/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/stream/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/threading/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/utils/xml/*.cpp"
)

file(GLOB AWS_S3_SOURCES
    "${AWS_S3_LIBRARY_DIR}/source/*.cpp"
)

file (GLOB AWS_CRT_C_MQTT_SOURCES
    "${AWS_CRT_C_MQTT_LIBRARY_DIR}/source/*.c"
)

file (GLOB AWS_CRT_C_SDKUTILS_SOURCES
    "${AWS_CRT_C_SDKUTILS_LIBRARY_DIR}/source/*.c"
)

file(GLOB AWS_S3_MODEL_SOURCES
    "${AWS_S3_LIBRARY_DIR}/source/model/*.cpp"
)

file(GLOB AWS_EVENT_STREAM_SOURCES
    "${AWS_EVENT_STREAM_LIBRARY_DIR}/source/*.c"
)

file(GLOB AWS_COMMON_SOURCES
    "${AWS_COMMON_LIBRARY_DIR}/source/*.c"
    "${AWS_COMMON_LIBRARY_DIR}/source/posix/*.c"
)

file (GLOB_RECURSE AWS_CRT_C_IO_SOURCES "${AWS_CRT_C_IO_LIBRARY_DIR}/source/*.c")
list (FILTER AWS_CRT_C_IO_SOURCES EXCLUDE REGEX "/windows/|/darwin/|/bsd/")

file (GLOB AWS_CRT_C_HTTP_SOURCES "${AWS_CRT_C_HTTP_LIBRARY_DIR}/source/*.c")

file (GLOB AWS_CRT_C_COMPRESSION_SOURCES "${AWS_CRT_C_COMPRESSION_LIBRARY_DIR}/source/*.c")

file (GLOB AWS_CRT_C_CAL_SOURCES
    "${AWS_CRT_C_CAL_LIBRARY_DIR}/source/*.c"
    "${AWS_CRT_C_CAL_LIBRARY_DIR}/source/unix/*.c"
)

file (GLOB AWS_CRT_C_S3_SOURCES
    "${AWS_CRT_C_S3_LIBRARY_DIR}/source/*.c"
)

file (GLOB AWS_CRT_C_AUTH_SOURCES
    "${AWS_CRT_C_AUTH_LIBRARY_DIR}/source/*.c"
    "${AWS_CRT_C_AUTH_LIBRARY_DIR}/source/external/*.c"
)

file (GLOB AWS_CRT_S2N_SOURCES
    "${AWS_CRT_S2N_LIBRARY_DIR}/utils/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/error/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/tls/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/tls/extensions/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/crypto/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/kyber_r3/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/kyber_r2/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/kyber_90s_r2/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/sike_r3/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/sike_r1/fp_generic_r1.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/sike_r1/P503_r1.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/sike_r1/sike_r1_kem.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/sike_r1/fips202_r1.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/bike_r1/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/bike_r2/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/pq-crypto/bike_r3/*.c"
    "${AWS_CRT_S2N_LIBRARY_DIR}/stuffer/*.c"
)

file (GLOB AWS_CRT_SOURCES
    "${AWS_CRT_LIBRARY_DIR}/source/*.cpp"
    "${AWS_CRT_LIBRARY_DIR}/source/io/*.cpp"
    "${AWS_CRT_LIBRARY_DIR}/source/external/*.cpp"
    "${AWS_CRT_LIBRARY_DIR}/source/http/*.cpp"
    "${AWS_CRT_LIBRARY_DIR}/source/auth/*.cpp"
)

file(GLOB AWS_CHECKSUMS_SOURCES
    "${AWS_CHECKSUMS_LIBRARY_DIR}/source/*.c"
    "${AWS_CHECKSUMS_LIBRARY_DIR}/source/intel/*.c"
    "${AWS_CHECKSUMS_LIBRARY_DIR}/source/arm/*.c"
)

file(GLOB S3_UNIFIED_SRC
    ${AWS_EVENT_STREAM_SOURCES}
    ${AWS_COMMON_SOURCES}
    ${AWS_S3_SOURCES}
    ${AWS_S3_MODEL_SOURCES}
    ${AWS_CORE_SOURCES}
    ${AWS_CRT_SOURCES}
    ${AWS_CRT_C_HTTP_SOURCES}
    ${AWS_CRT_C_COMPRESSION_SOURCES}
    ${AWS_CRT_C_IO_SOURCES}
    ${AWS_CRT_C_CAL_SOURCES}
    ${AWS_CRT_C_S3_SOURCES}
    ${AWS_CRT_S2N_SOURCES}
    ${AWS_CRT_C_MQTT_SOURCES}
    ${AWS_CRT_C_AUTH_SOURCES}
    ${AWS_CRT_C_SDKUTILS_SOURCES}
)

set(S3_INCLUDES
    "${AWS_COMMON_LIBRARY_DIR}/include/"
    "${AWS_CRT_LIBRARY_DIR}/include/"
    "${AWS_CRT_S2N_LIBRARY_DIR}/api/"
    "${AWS_CRT_C_IO_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_MQTT_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_AUTH_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_SDKUTILS_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_CAL_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_HTTP_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_COMPRESSION_LIBRARY_DIR}/include/"
    "${AWS_CRT_C_S3_LIBRARY_DIR}/include/"
    "${AWS_CRT_S2N_LIBRARY_DIR}/"
    "${AWS_EVENT_STREAM_LIBRARY_DIR}/include/"
    "${AWS_S3_LIBRARY_DIR}/include/"
    "${AWS_CORE_LIBRARY_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include/"
)

add_library(_aws_s3_checksums ${AWS_CHECKSUMS_SOURCES})
target_include_directories(_aws_s3_checksums SYSTEM PUBLIC "${AWS_CHECKSUMS_LIBRARY_DIR}/include/")
if(CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    target_compile_definitions(_aws_s3_checksums PRIVATE "-DDEBUG_BUILD")
endif()
set_target_properties(_aws_s3_checksums PROPERTIES LINKER_LANGUAGE C)
set_property(TARGET _aws_s3_checksums PROPERTY C_STANDARD 99)

add_library(_aws_s3 ${S3_UNIFIED_SRC})

include ("${AWS_COMMON_LIBRARY_DIR}/cmake/AwsThreadAffinity.cmake")
aws_set_thread_affinity_method (_aws_s3)

target_compile_definitions(_aws_s3 PUBLIC "AWS_SDK_VERSION_MAJOR=1")
target_compile_definitions(_aws_s3 PUBLIC "AWS_SDK_VERSION_MINOR=7")
target_compile_definitions(_aws_s3 PUBLIC "AWS_SDK_VERSION_PATCH=231")
target_include_directories(_aws_s3 SYSTEM BEFORE PUBLIC ${S3_INCLUDES})

if (TARGET OpenSSL::SSL)
    target_compile_definitions(_aws_s3 PUBLIC -DENABLE_OPENSSL_ENCRYPTION -DOPENSSL_IS_AWSLC)
    target_link_libraries(_aws_s3 PRIVATE OpenSSL::Crypto OpenSSL::SSL)
endif()

target_link_libraries(_aws_s3 PRIVATE _aws_s3_checksums)

# The library is large - avoid bloat.
target_compile_options (_aws_s3 PRIVATE -g0)
target_compile_options (_aws_s3_checksums PRIVATE -g0)

add_library(ch_contrib::aws_s3 ALIAS _aws_s3)
