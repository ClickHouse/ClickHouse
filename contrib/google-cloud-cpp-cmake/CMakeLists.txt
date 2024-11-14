set(ENABLE_GOOGLE_CLOUD_DEFAULT OFF)

if(ENABLE_LIBRARIES AND (OS_LINUX OR OS_DARWIN) AND TARGET OpenSSL::Crypto)
    set(ENABLE_GOOGLE_CLOUD_DEFAULT ON)
endif()

option(ENABLE_GOOGLE_CLOUD "Enable Google Cloud SDK" ${ENABLE_GOOGLE_CLOUD_DEFAULT})

if(NOT ENABLE_GOOGLE_CLOUD)
    message(STATUS "Not using Google Cloud")
    return()
endif()

if(NOT ENABLE_GRPC)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use Google Cloud without gRPC")
endif()
if (NOT ENABLE_PROTOBUF)
    message( ${RECONFIGURE_MESSAGE_LEVEL} "Can't use Google Cloud without protobuf")
endif()

# Gather sources and options.
set(GOOGLE_CLOUD_SOURCES)
set(GOOGLE_CLOUD_PUBLIC_INCLUDES)
set(GOOGLE_CLOUD_PRIVATE_INCLUDES)
set(GOOGLE_CLOUD_PRIVATE_LIBS)

# -- The build needs protobuf files. The original build scripts download them from a remote server (see target 'googleapis_download').
#    This is too unreliable in the context of ClickHouse ... we instead ship the downloaded archive with the ClickHouse source and
#    extract it into the build directory directly.

# Note: The hash must match the Google Cloud Api version, otherwise funny things will happen.
# Find the right hash in "strip-prefix" in MODULE.bazel in the subrepository
set(PB_HASH "e60db19f11f94175ac682c5898cce0f77cc508ea")
set(PB_ARCHIVE "${PB_HASH}.tar.gz")
set(PB_DIR "googleapis-${PB_HASH}")

# Terrible CMake below but it works.
file(COPY "${ClickHouse_SOURCE_DIR}/contrib/google-cloud-cpp-cmake/${PB_ARCHIVE}"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/../../external/googleapis/src/"
)

file(ARCHIVE_EXTRACT INPUT
    "${CMAKE_CURRENT_BINARY_DIR}/../../external/googleapis/src/${PB_ARCHIVE}"
    DESTINATION
    "${CMAKE_CURRENT_BINARY_DIR}/../../external/googleapis/src")

file(RENAME
    "${CMAKE_CURRENT_BINARY_DIR}/../../external/googleapis/src/${PB_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/../../external/googleapis/src/googleapis_download"
)
# ---

# Directories.
SET(GOOGLE_CLOUD_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-cloud-cpp" )
list(APPEND GOOGLE_CLOUD_PRIVATE_INCLUDES "${GOOGLE_CLOUD_DIR}")

# Set the PROJECT_SOURCE_DIR so that all Google Cloud cmake files work
set(PROJECT_SOURCE_DIR_BAK ${PROJECT_SOURCE_DIR})
set(PROJECT_SOURCE_DIR ${GOOGLE_CLOUD_DIR})

list(APPEND CMAKE_MODULE_PATH "${GOOGLE_CLOUD_DIR}/cmake")

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

SET(GOOGLE_CLOUD_KMS_DIR "${GOOGLE_CLOUD_DIR}/google/cloud/kms")

file(GLOB GOOGLE_CLOUD_KMS_SRC
    "${GOOGLE_CLOUD_KMS_DIR}/v1/*.cc"
    "${GOOGLE_CLOUD_KMS_DIR}/v1/internal/*.cc"
    "${GOOGLE_CLOUD_KMS_DIR}/inventory/v1/*.cc"
)

list(APPEND GOOGLE_CLOUD_SOURCES ${GOOGLE_CLOUD_KMS_SRC})
list(APPEND GOOGLE_CLOUD_PUBLIC_INCLUDES "${GOOGLE_CLOUD_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")

set(GRPC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
list(APPEND GOOGLE_CLOUD_PUBLIC_INCLUDES "${GRPC_INCLUDE_DIR}/include" "${GRPC_INCLUDE_DIR}/spm-cpp-include")

# Restore the PROJECT_SOURCE_DIR.
set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR_BAK})

# Link against external libraries.
list(APPEND GOOGLE_CLOUD_PRIVATE_LIBS
    google_cloud_cpp_common
    google_cloud_cpp_grpc_utils
    google_cloud_cpp_kms_protos
    google_cloud_cpp_cloud_location_locations_protos
    google_cloud_cpp_iam_v1_iam_policy_protos
    gRPC::grpc++
    absl::optional
)

list(APPEND GOOGLE_CLOUD_PUBLIC_LIBS
    absl::optional
    gRPC::grpc++
)

# Add library.
add_library(_gcloud ${GOOGLE_CLOUD_SOURCES})

target_include_directories(_gcloud SYSTEM PUBLIC ${GOOGLE_CLOUD_PUBLIC_INCLUDES})
target_include_directories(_gcloud SYSTEM PRIVATE ${GOOGLE_CLOUD_PRIVATE_INCLUDES})
target_link_libraries(_gcloud PRIVATE ${GOOGLE_CLOUD_PRIVATE_LIBS})

# The library is large - avoid bloat.
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_gcloud PRIVATE -g0)
endif()

add_library(ch_contrib::google_cloud ALIAS _gcloud)
