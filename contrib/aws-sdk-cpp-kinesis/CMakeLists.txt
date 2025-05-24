set(ENABLE_AWS_KINESIS_DEFAULT OFF)

if(ENABLE_LIBRARIES AND (OS_LINUX OR OS_DARWIN) AND TARGET OpenSSL::Crypto AND TARGET ch_contrib::aws_s3)
    set(ENABLE_AWS_KINESIS_DEFAULT ON)
endif()

find_library(AWS_C_EVENT_STREAM_LIBRARY NAMES aws-c-event-stream)
find_path(AWS_C_EVENT_STREAM_INCLUDE_DIR NAMES aws/event-stream/event_stream.h)

if(NOT AWS_C_EVENT_STREAM_LIBRARY OR NOT AWS_C_EVENT_STREAM_INCLUDE_DIR)
    message(WARNING "aws-c-event-stream not found, Kinesis Enhanced Fan-Out will be disabled")
    add_definitions(-DAWS_DISABLE_EVENT_STREAM)
else()
    message(STATUS "Found aws-c-event-stream: ${AWS_C_EVENT_STREAM_LIBRARY}")
    target_include_directories(aws_sdk_kinesis SYSTEM PUBLIC ${AWS_C_EVENT_STREAM_INCLUDE_DIR})
endif()

option(ENABLE_AWS_KINESIS "Enable AWS Kinesis" ${ENABLE_AWS_KINESIS_DEFAULT})

if(ENABLE_AWS_KINESIS)
    if(NOT TARGET ch_contrib::aws_s3)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS Kinesis without AWS S3")
    elseif(NOT (OS_LINUX OR OS_DARWIN))
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS Kinesis with platform ${CMAKE_SYSTEM_NAME}")
    endif()
endif()

if(NOT ENABLE_AWS_KINESIS)
    message(STATUS "Not using AWS Kinesis")
    return()
endif()

# Directories
SET(AWS_SDK_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws")
SET(AWS_SDK_KINESIS_DIR "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-kinesis")

# aws-cpp-sdk-kinesis
file(GLOB AWS_SDK_KINESIS_SRC
    "${AWS_SDK_KINESIS_DIR}/source/*.cpp"
    "${AWS_SDK_KINESIS_DIR}/source/model/*.cpp"
)

# Build library
add_library(aws_sdk_kinesis STATIC ${AWS_SDK_KINESIS_SRC})
add_library(ch_contrib::aws_sdk_kinesis ALIAS aws_sdk_kinesis)

# Process includes as system headers to avoid warnings
target_include_directories(aws_sdk_kinesis SYSTEM PUBLIC "${AWS_SDK_KINESIS_DIR}/include/")

# Disable specific warnings for AWS SDK code
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

if(AWS_C_EVENT_STREAM_LIBRARY)
    target_link_libraries(aws_sdk_kinesis PUBLIC ch_contrib::aws_s3 ${AWS_C_EVENT_STREAM_LIBRARY})
else()
    target_link_libraries(aws_sdk_kinesis PUBLIC ch_contrib::aws_s3)
endif()

message(STATUS "Using AWS Kinesis") 
