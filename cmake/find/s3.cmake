if(NOT OS_FREEBSD AND NOT APPLE AND NOT ARCH_ARM)
    option(ENABLE_S3 "Enable S3" ${ENABLE_LIBRARIES})
endif()

if(ENABLE_S3)
    option(USE_INTERNAL_AWS_S3_LIBRARY "Set to FALSE to use system S3 instead of bundled" ${NOT_UNBUNDLED})

    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3")
        message (WARNING "submodule contrib/aws is missing. to fix try run: \n git submodule update --init --recursive")
        set (MISSING_AWS_S3 1)
    endif ()

    if (USE_INTERNAL_AWS_S3_LIBRARY AND NOT MISSING_AWS_S3)
        set(AWS_S3_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3/include")
        set(AWS_S3_CORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core/include")
        set(AWS_S3_LIBRARY aws_s3)
        set(USE_INTERNAL_AWS_S3_LIBRARY 1)
        set(USE_AWS_S3 1)
    else()
        set(USE_INTERNAL_AWS_S3_LIBRARY 0)
        set(USE_AWS_S3 0)
    endif ()

endif()

message (STATUS "Using aws_s3=${USE_AWS_S3}: ${AWS_S3_INCLUDE_DIR} : ${AWS_S3_LIBRARY}")
