option (USE_AWS_S3 "Set to FALSE to use system libbrotli library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3")
    if (USE_AWS_S3)
        message (WARNING "submodule contrib/aws is missing. to fix try run: \n git submodule update --init --recursive")
        set (USE_AWS_S3 0)
    endif ()
    set (MISSING_AWS_S3 1)
endif ()

if (USE_AWS_S3 AND NOT MISSING_AWS_S3)
    set(AWS_S3_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3/include")
    set(AWS_S3_CORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core/include")
    set(AWS_S3_LIBRARY aws_s3)
    set(USE_INTERNAL_AWS_S3_LIBRARY 1)
    set(USE_AWS_S3 1)
endif ()

message (STATUS "Using aws_s3=${USE_AWS_S3}: ${AWS_S3_INCLUDE_DIR} : ${AWS_S3_LIBRARY}")
