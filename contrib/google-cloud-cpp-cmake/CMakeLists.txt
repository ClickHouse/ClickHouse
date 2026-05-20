if(NOT ENABLE_GOOGLE_CLOUD_CPP)
    message(STATUS "Not using Google Cloud Cpp")
    return()
endif()

if(CLICKHOUSE_CLOUD)
    set(ENABLE_GOOGLE_CLOUD_CPP_KMS_DEFAULT ON)
endif()

option(ENABLE_GOOGLE_CLOUD_CPP_KMS "Enable Google Cloud Cpp KMS" ${ENABLE_GOOGLE_CLOUD_CPP_KMS_DEFAULT})

if(NOT ENABLE_GRPC)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use Google Cloud Cpp without gRPC")
    return()
endif()
if(NOT ENABLE_PROTOBUF)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use Google Cloud Cpp without protobuf")
    return()
endif()

# Gather sources and options.
set(GOOGLE_CLOUD_CPP_SOURCES)
set(GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES)
set(GOOGLE_CLOUD_CPP_PRIVATE_INCLUDES)
set(GOOGLE_CLOUD_CPP_PRIVATE_LIBS)

# Directories.
SET(GOOGLE_CLOUD_CPP_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-cloud-cpp" )
list(APPEND GOOGLE_CLOUD_CPP_PRIVATE_INCLUDES "${GOOGLE_CLOUD_CPP_DIR}")
list(APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/google-cloud-cpp-cmake/cmake")

# Set the PROJECT_SOURCE_DIR so that all Google Cloud cmake files work
set(PROJECT_SOURCE_DIR_BAK ${PROJECT_SOURCE_DIR})
set(PROJECT_SOURCE_DIR ${GOOGLE_CLOUD_CPP_DIR})

# Building this target results in all protobufs being compiled.
add_custom_target(google-cloud-cpp-protos)

include(GoogleCloudCppLibrary)

# Set some variables required for googleapis CMakeLists.txt to work.
set(GOOGLE_CLOUD_CPP_ENABLE_GRPC ON)
set(PROJECT_VERSION "1")
set(PROJECT_VERSION_MAJOR "1")
set(PROTO_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")

if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR)
    set(Protobuf_PROTOC_EXECUTABLE "${PROJECT_BINARY_DIR}/native/contrib/google-protobuf-cmake/protoc")
    set(GOOGLE_CLOUD_CPP_GRPC_PLUGIN_EXECUTABLE "${PROJECT_BINARY_DIR}/native/contrib/grpc-cmake/grpc_cpp_plugin")
else ()
    set(Protobuf_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
    set(GOOGLE_CLOUD_CPP_GRPC_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
endif()

include(GoogleApis)

add_library(gRPC::grpc++ ALIAS _ch_contrib_grpc)
add_library(gRPC::grpc ALIAS _ch_contrib_grpc)

include(google_cloud_cpp_common)
include(google_cloud_cpp_grpc_utils)

# KMS
if(ENABLE_GOOGLE_CLOUD_CPP_KMS)
    google_cloud_cpp_add_library_protos(kms)

    SET(GOOGLE_CLOUD_CPP_KMS_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud/kms")

    file(GLOB GOOGLE_CLOUD_CPP_KMS_SRC
        "${GOOGLE_CLOUD_CPP_KMS_DIR}/v1/*.cc"
        "${GOOGLE_CLOUD_CPP_KMS_DIR}/v1/internal/*.cc"
        "${GOOGLE_CLOUD_CPP_KMS_DIR}/inventory/v1/*.cc"
    )
endif()

# Storage
google_cloud_cpp_add_library_protos(storage)

# Storage needs rest_internal, and both need nlohmann_json

# Let google_cloud_cpp use our own version of curl and crc32c
add_library(CURL::libcurl ALIAS _curl)
add_library(Crc32c::crc32c ALIAS _crc32c)
add_library(nlohmann_json::nlohmann_json ALIAS _nlohmann_json)

include(google_cloud_cpp_rest_internal)
include(google_cloud_cpp_storage)

set(GOOGLE_CLOUD_CPP_STORAGE_GRPC_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud/storage")
file(GLOB GOOGLE_CLOUD_CPP_STORAGE_GRPC_SRC CONFIGURE_DEPENDS
    "${GOOGLE_CLOUD_CPP_STORAGE_GRPC_DIR}/grpc_plugin.cc"
    "${GOOGLE_CLOUD_CPP_STORAGE_GRPC_DIR}/async/*.cc"
    "${GOOGLE_CLOUD_CPP_STORAGE_GRPC_DIR}/internal/async/*.cc"
    "${GOOGLE_CLOUD_CPP_STORAGE_GRPC_DIR}/internal/grpc/*.cc"
    "${GOOGLE_CLOUD_CPP_STORAGE_GRPC_DIR}/internal/storage_*.cc"
)
list(FILTER GOOGLE_CLOUD_CPP_STORAGE_GRPC_SRC EXCLUDE REGEX "_test\\.cc$")
list(FILTER GOOGLE_CLOUD_CPP_STORAGE_GRPC_SRC EXCLUDE REGEX "/storage_connection\\.cc$")
add_library(google_cloud_cpp_storage_grpc ${GOOGLE_CLOUD_CPP_STORAGE_GRPC_SRC})
target_link_libraries(
    google_cloud_cpp_storage_grpc
    PUBLIC google-cloud-cpp::storage
           google-cloud-cpp::storage_protos
           google-cloud-cpp::grpc_utils
           google-cloud-cpp::common
           nlohmann_json::nlohmann_json
           gRPC::grpc++
           absl::optional
           absl::strings
           absl::time
           Threads::Threads)

target_include_directories(google_cloud_cpp_storage_grpc PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
target_compile_options(google_cloud_cpp_storage_grpc PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})
target_compile_definitions(google_cloud_cpp_storage_grpc PUBLIC GOOGLE_CLOUD_CPP_STORAGE_HAVE_GRPC)
set_target_properties(
    google_cloud_cpp_storage_grpc
    PROPERTIES EXPORT_NAME "google-cloud-cpp::storage_grpc"
               VERSION ${PROJECT_VERSION}
               SOVERSION ${PROJECT_VERSION_MAJOR})
add_library(google-cloud-cpp::storage_grpc ALIAS google_cloud_cpp_storage_grpc)

# Common
list(APPEND GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES "${GOOGLE_CLOUD_CPP_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")

set(GRPC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
list(APPEND GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES "${GRPC_INCLUDE_DIR}/include" "${GRPC_INCLUDE_DIR}/spm-cpp-include")

# Restore the PROJECT_SOURCE_DIR.
set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR_BAK})

# Link against external libraries.
list(APPEND GOOGLE_CLOUD_CPP_PRIVATE_LIBS
    google_cloud_cpp_common
    google_cloud_cpp_grpc_utils
    google_cloud_cpp_rest_internal
    google_cloud_cpp_storage
    google_cloud_cpp_storage_grpc
    google_cloud_cpp_storage_protos
)

list(APPEND GOOGLE_CLOUD_CPP_PRIVATE_LIBS_EXTRA
    ch_contrib::crc32c
    ch_contrib::curl
    ch_contrib::nlohmann_json
)

if(ENABLE_GOOGLE_CLOUD_CPP_KMS)
    list(APPEND GOOGLE_CLOUD_CPP_SOURCES ${GOOGLE_CLOUD_CPP_KMS_SRC})
    add_library(_google_cloud_cpp_kms ${GOOGLE_CLOUD_CPP_SOURCES})
    set(GOOGLE_CLOUD_CPP_KMS_DEPS google_cloud_cpp_grpc_utils google_cloud_cpp_kms_protos google_cloud_cpp_iam_v1_iam_policy_protos google_cloud_cpp_cloud_location_locations_protos)

    add_dependencies(_google_cloud_cpp_kms ${GOOGLE_CLOUD_CPP_KMS_DEPS})
    list(APPEND GOOGLE_CLOUD_CPP_PRIVATE_LIBS _google_cloud_cpp_kms ${GOOGLE_CLOUD_CPP_KMS_DEPS})

    target_include_directories(_google_cloud_cpp_kms SYSTEM PRIVATE ${GOOGLE_CLOUD_CPP_PRIVATE_INCLUDES})
    target_include_directories(_google_cloud_cpp_kms SYSTEM PUBLIC ${GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES})
    target_link_libraries(_google_cloud_cpp_kms absl::optional ch_contrib::protobuf)
endif()

add_library(_gcloud INTERFACE)
target_include_directories(_gcloud SYSTEM INTERFACE ${GOOGLE_CLOUD_CPP_PUBLIC_INCLUDES})
target_link_libraries(_gcloud INTERFACE ${GOOGLE_CLOUD_CPP_PRIVATE_LIBS} ${GOOGLE_CLOUD_CPP_PRIVATE_LIBS_EXTRA})

# The library is large - avoid bloat.
if(OMIT_HEAVY_DEBUG_SYMBOLS)
    foreach(lib ${GOOGLE_CLOUD_CPP_PRIVATE_LIBS})
        target_compile_options(${lib} PRIVATE -g1)
    endforeach()
endif()

add_library(ch_contrib::google_cloud_cpp ALIAS _gcloud)
