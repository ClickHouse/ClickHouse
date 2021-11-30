set(AZURE_DIR "${ClickHouse_SOURCE_DIR}/contrib/azure")
set(AZURE_SDK_LIBRARY_DIR "${AZURE_DIR}/sdk")

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

include("${AZURE_DIR}/cmake-modules/AzureTransportAdapters.cmake")

add_library(azure_sdk ${AZURE_SDK_UNIFIED_SRC})

target_compile_options(azure_sdk PUBLIC
    -Wno-deprecated-copy-dtor
    -Wno-extra-semi
    -Wno-suggest-destructor-override
    -Wno-inconsistent-missing-destructor-override
    -Wno-error=unknown-warning-option
    -Wno-reserved-identifier
)

# Originally, on Windows azure-core is built with bcrypt and crypt32 by default
if (OPENSSL_FOUND)
    target_link_libraries(azure_sdk PRIVATE ${OPENSSL_LIBRARIES})
endif()

# Originally, on Windows azure-core is built with winhttp by default
if (CURL_FOUND)
    target_link_libraries(azure_sdk PRIVATE ${CURL_LIBRARY})
endif()

target_link_libraries(azure_sdk PRIVATE ${LIBXML2_LIBRARIES})

target_include_directories(azure_sdk PUBLIC ${AZURE_SDK_INCLUDES})
