set(ENABLE_GOOGLE_CLOUD_CPP_DEFAULT OFF)

if(ENABLE_LIBRARIES AND CLICKHOUSE_CLOUD AND OS_LINUX)
    set(ENABLE_GOOGLE_CLOUD_CPP_DEFAULT ON)
endif()

option(ENABLE_GOOGLE_CLOUD_CPP "Enable Google Cloud Cpp" ${ENABLE_GOOGLE_CLOUD_CPP_DEFAULT})

if(NOT ENABLE_GOOGLE_CLOUD_CPP)
    message(STATUS "Not using Google Cloud Cpp")
    return()
endif()

if(NOT ENABLE_GRPC)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use Google Cloud Cpp without gRPC")
endif()
if (NOT ENABLE_PROTOBUF)
    message( ${RECONFIGURE_MESSAGE_LEVEL} "Can't use Google Cloud Cpp without protobuf")
endif()

# Gather sources and options.
set(GOOGLE_CLOUD_CPP_SOURCES)
set(GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES)
set(GOOGLE_CLOUD_CPP_PRIVATE_INCLUDES)
set(GOOGLE_CLOUD_CPP_PRIVATE_LIBS)

# Directories.
SET(GOOGLE_CLOUD_CPP_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-cloud-cpp" )
list(APPEND GOOGLE_CLOUD_CPP_PRIVATE_INCLUDES "${GOOGLE_CLOUD_CPP_DIR}")

# Set the PROJECT_SOURCE_DIR so that all Google Cloud cmake files work
set(PROJECT_SOURCE_DIR_BAK ${PROJECT_SOURCE_DIR})
set(PROJECT_SOURCE_DIR ${GOOGLE_CLOUD_CPP_DIR})

list(APPEND CMAKE_MODULE_PATH "${GOOGLE_CLOUD_CPP_DIR}/cmake")

# Building this target results in all protobufs being compiled.
add_custom_target(google-cloud-cpp-protos)

include("GoogleCloudCppLibrary")

# Set some variables required for googleapis CMakeLists.txt to work.
set(GOOGLE_CLOUD_CPP_ENABLE_GRPC ON)
set(PROJECT_VERSION "1")
set(PROJECT_VERSION_MAJOR "1")
set(PROTO_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")
set(GOOGLE_CLOUD_CPP_GRPC_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

include(GoogleApis.cmake)

add_library(gRPC::grpc++ ALIAS _ch_contrib_grpc)
add_library(gRPC::grpc ALIAS _ch_contrib_grpc)

# google-cloud-cpp-kms.
google_cloud_cpp_add_library_protos(kms)

include(google_cloud_cpp_common.cmake)
include(google_cloud_cpp_grpc_utils.cmake)

SET(GOOGLE_CLOUD_CPP_KMS_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud/kms")

file(GLOB GOOGLE_CLOUD_CPP_KMS_SRC
    "${GOOGLE_CLOUD_CPP_KMS_DIR}/v1/*.cc"
    "${GOOGLE_CLOUD_CPP_KMS_DIR}/v1/internal/*.cc"
    "${GOOGLE_CLOUD_CPP_KMS_DIR}/inventory/v1/*.cc"
)

list(APPEND GOOGLE_CLOUD_CPP_SOURCES ${GOOGLE_CLOUD_CPP_KMS_SRC})
list(APPEND GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES "${GOOGLE_CLOUD_CPP_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")

set(GRPC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
list(APPEND GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES "${GRPC_INCLUDE_DIR}/include" "${GRPC_INCLUDE_DIR}/spm-cpp-include")

# Restore the PROJECT_SOURCE_DIR.
set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR_BAK})

# Link against external libraries.
list(APPEND GOOGLE_CLOUD_CPP_PRIVATE_LIBS
    google_cloud_cpp_common
    google_cloud_cpp_grpc_utils
    google_cloud_cpp_kms_protos
    google_cloud_cpp_cloud_location_locations_protos
    google_cloud_cpp_iam_v1_iam_policy_protos
    gRPC::grpc++
    absl::optional
)

list(APPEND GOOGLE_CLOUD_CPP_PUBLIC_LIBS
    absl::optional
    gRPC::grpc++
)

# Add library.
add_library(_gcloud ${GOOGLE_CLOUD_CPP_SOURCES})

target_include_directories(_gcloud SYSTEM PUBLIC ${GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES})
target_include_directories(_gcloud SYSTEM PRIVATE ${GOOGLE_CLOUD_CPP_PRIVATE_INCLUDES})
target_link_libraries(_gcloud PRIVATE ${GOOGLE_CLOUD_CPP_PRIVATE_LIBS})

# The library is large - avoid bloat.
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(_gcloud PRIVATE -g0)
endif()

add_library(ch_contrib::google_cloud_cpp ALIAS _gcloud)
