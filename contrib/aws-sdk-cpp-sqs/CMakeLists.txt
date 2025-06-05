set(ENABLE_AWS_SQS_DEFAULT OFF)

if(ENABLE_LIBRARIES AND (OS_LINUX OR OS_DARWIN) AND TARGET OpenSSL::Crypto AND TARGET ch_contrib::aws_s3)
    set(ENABLE_AWS_SQS_DEFAULT ON)
endif()

option(ENABLE_AWS_SQS "Enable AWS SQS" ${ENABLE_AWS_SQS_DEFAULT})

if(ENABLE_AWS_SQS)
    if(NOT TARGET ch_contrib::aws_s3)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS SQS without AWS S3")
    elseif(NOT (OS_LINUX OR OS_DARWIN))
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS SQS with platform ${CMAKE_SYSTEM_NAME}")
    endif()
endif()

if(NOT ENABLE_AWS_SQS)
    message(STATUS "Not using AWS SQS")
    return()
endif()

# Directories
SET(AWS_SDK_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws")
SET(AWS_SDK_SQS_DIR "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-sqs")

# aws-cpp-sdk-sqs
file(GLOB AWS_SDK_SQS_SRC
    "${AWS_SDK_SQS_DIR}/source/*.cpp"
    "${AWS_SDK_SQS_DIR}/source/model/*.cpp"
)

# Build library
add_library(aws_sdk_sqs STATIC ${AWS_SDK_SQS_SRC})
add_library(ch_contrib::aws_sdk_sqs ALIAS aws_sdk_sqs)

# Process includes as system headers to avoid warnings
target_include_directories(aws_sdk_sqs SYSTEM PUBLIC "${AWS_SDK_SQS_DIR}/include/")

# Disable specific warnings for AWS SDK code
target_compile_options(aws_sdk_sqs PRIVATE 
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

target_link_libraries(aws_sdk_sqs PUBLIC ch_contrib::aws_s3)

message(STATUS "Using AWS SQS") 
