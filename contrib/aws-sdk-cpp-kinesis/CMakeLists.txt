set(ENABLE_AWS_KINESIS_DEFAULT OFF)

if (ENABLE_LIBRARIES AND (OS_LINUX OR OS_DARWIN) AND TARGET OpenSSL::Crypto AND TARGET ch_contrib::aws_s3)
    set(ENABLE_AWS_KINESIS_DEFAULT ON)
endif ()

option(ENABLE_AWS_KINESIS "Enable AWS Kinesis" ${ENABLE_AWS_KINESIS_DEFAULT})

if (ENABLE_AWS_KINESIS)
    if (NOT TARGET ch_contrib::aws_s3)
        message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS Kinesis without AWS S3")
    elseif (NOT (OS_LINUX OR OS_DARWIN))
        message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS Kinesis with platform ${CMAKE_SYSTEM_NAME}")
    endif ()
endif ()

if (NOT ENABLE_AWS_KINESIS)
    message(STATUS "Not using AWS Kinesis")
    return()
endif ()

set(AWS_SDK_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws")
set(AWS_SDK_KINESIS_DIR "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-kinesis")

file(GLOB AWS_SDK_KINESIS_SRC
    "${AWS_SDK_KINESIS_DIR}/source/*.cpp"
    "${AWS_SDK_KINESIS_DIR}/source/model/*.cpp"
)

add_library(aws_sdk_kinesis STATIC ${AWS_SDK_KINESIS_SRC})
add_library(ch_contrib::aws_sdk_kinesis ALIAS aws_sdk_kinesis)

target_include_directories(aws_sdk_kinesis SYSTEM PUBLIC "${AWS_SDK_KINESIS_DIR}/include/")

target_compile_options(aws_sdk_kinesis PRIVATE
    -Wno-documentation
    -Wno-documentation-unknown-command
    -Wno-error=documentation
    -Wno-suggest-destructor-override
    -Wno-error=suggest-destructor-override
    -Wno-inconsistent-missing-destructor-override
    -Wno-error=inconsistent-missing-destructor-override
    -Wno-error=documentation-html
    -Wno-documentation-html
    -Wno-reserved-identifier
)

target_link_libraries(aws_sdk_kinesis PUBLIC ch_contrib::aws_s3)

message(STATUS "Using AWS Kinesis")
