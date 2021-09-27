set(AZURE_SDK_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/azure/sdk")

file(GLOB AZURE_SDK_CORE_SRC
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/cryptography/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/http/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/http/curl/*.hpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/http/curl/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/winhttp/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/io/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/private/*.hpp"
)

file(GLOB AZURE_SDK_IDENTITY_SRC
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/src/private/*.hpp"
)

file(GLOB AZURE_SDK_STORAGE_COMMON_SRC
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/src/private/*.cpp"
)

file(GLOB AZURE_SDK_STORAGE_BLOBS_SRC
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/src/private/*.hpp"
)

file(GLOB AZURE_SDK_UNIFIED_SRC
    ${AZURE_SDK_CORE_SRC}
    ${AZURE_SDK_IDENTITY_SRC}
    ${AZURE_SDK_STORAGE_COMMON_SRC}
    ${AZURE_SDK_STORAGE_BLOBS_SRC}
)

set(AZURE_SDK_INCLUDES
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/inc/"
)

include(AzureTransportAdapters.cmake)

add_library(azure_sdk ${AZURE_SDK_UNIFIED_SRC})

# # original snippet:
# if(WIN32)
#     target_link_libraries(azure-core PRIVATE bcrypt crypt32)
# else()
#     find_package(OpenSSL REQUIRED)
#     target_link_libraries(azure-core PRIVATE OpenSSL::SSL)
# endif()

if (OPENSSL_FOUND)
    target_link_libraries(azure_sdk PRIVATE ${OPENSSL_LIBRARIES})
endif()

# # azure core is originally built as:
#
# if(BUILD_TRANSPORT_CURL)
#   target_link_libraries(azure-core PUBLIC CURL::libcurl)
# endif()
# if(BUILD_TRANSPORT_WINHTTP)
#   target_link_libraries(azure-core PRIVATE winhttp)
# endif()

target_link_libraries(azure_sdk PRIVATE ${CURL_LIBRARY})

target_link_libraries(azure_sdk PRIVATE ${LIBXML2_LIBRARIES})
target_include_directories(azure_sdk PUBLIC ${LIBXML2_INCLUDE_DIRS})

target_include_directories(azure_sdk PUBLIC ${AZURE_SDK_INCLUDES})

