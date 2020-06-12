SET(AWS_S3_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3)
SET(AWS_CORE_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core)
SET(AWS_CHECKSUMS_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws-checksums)
SET(AWS_COMMON_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws-c-common)
SET(AWS_EVENT_STREAM_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws-c-event-stream)

OPTION(USE_AWS_MEMORY_MANAGEMENT "Aws memory management" OFF)
configure_file("${AWS_CORE_LIBRARY_DIR}/include/aws/core/SDKConfig.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/core/SDKConfig.h" @ONLY)

configure_file("${AWS_COMMON_LIBRARY_DIR}/include/aws/common/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/common/config.h" @ONLY)


file(GLOB AWS_CORE_SOURCES
    "${AWS_CORE_LIBRARY_DIR}/source/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/auth/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/client/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/http/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/http/standard/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/http/curl/*.cpp"
    "${AWS_CORE_LIBRARY_DIR}/source/config/*.cpp"
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
)

set(S3_INCLUDES
    "${AWS_COMMON_LIBRARY_DIR}/include/"
    "${AWS_EVENT_STREAM_LIBRARY_DIR}/include/"
    "${AWS_S3_LIBRARY_DIR}/include/"
    "${AWS_CORE_LIBRARY_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include/"
)

add_library(aws_s3_checksums ${AWS_CHECKSUMS_SOURCES})
target_include_directories(aws_s3_checksums PUBLIC "${AWS_CHECKSUMS_LIBRARY_DIR}/include/")
if(CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    target_compile_definitions(aws_s3_checksums PRIVATE "-DDEBUG_BUILD")
endif()
set_target_properties(aws_s3_checksums PROPERTIES LINKER_LANGUAGE C)
set_property(TARGET aws_s3_checksums PROPERTY C_STANDARD 99)

add_library(aws_s3 ${S3_UNIFIED_SRC})

target_compile_definitions(aws_s3 PUBLIC -DENABLE_CURL_CLIENT)
target_compile_definitions(aws_s3 PUBLIC "AWS_SDK_VERSION_MAJOR=1")
target_compile_definitions(aws_s3 PUBLIC "AWS_SDK_VERSION_MINOR=7")
target_compile_definitions(aws_s3 PUBLIC "AWS_SDK_VERSION_PATCH=231")
target_include_directories(aws_s3 PUBLIC ${S3_INCLUDES})

if (OPENSSL_FOUND)
    target_compile_definitions(aws_s3 PUBLIC -DENABLE_OPENSSL_ENCRYPTION)
    target_link_libraries(aws_s3 PRIVATE ${OPENSSL_LIBRARIES})
endif()

target_link_libraries(aws_s3 PRIVATE aws_s3_checksums curl)
