option(ENABLE_LIBOAI "Enable liboai library for OpenAI API" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBOAI)
    message(STATUS "Not using liboai")
    return()
endif()

# Check dependencies
if (NOT TARGET ch_contrib::curl)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use liboai without CURL")
    return()
endif()

set(LIBOAI_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liboai")
set(LIBOAI_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/liboai")

# Download nlohmann_json header
set(NLOHMANN_JSON_VERSION "v3.11.2")
set(NLOHMANN_JSON_URL "https://github.com/nlohmann/json/releases/download/${NLOHMANN_JSON_VERSION}/json.hpp")
set(NLOHMANN_JSON_INCLUDE_DIR "${LIBOAI_BINARY_DIR}/nlohmann")
set(NLOHMANN_JSON_FILE "${NLOHMANN_JSON_INCLUDE_DIR}/nlohmann/json.hpp")

if (NOT EXISTS "${NLOHMANN_JSON_FILE}")
    message(STATUS "Downloading nlohmann_json ${NLOHMANN_JSON_VERSION}")
    file(DOWNLOAD
        "${NLOHMANN_JSON_URL}"
        "${NLOHMANN_JSON_FILE}"
        SHOW_PROGRESS
        STATUS download_status
    )
    list(GET download_status 0 status_code)
    if (NOT status_code EQUAL 0)
        message(FATAL_ERROR "Failed to download nlohmann_json")
    endif()
endif()

# Create nlohmann_json interface library
add_library(nlohmann_json INTERFACE)
target_include_directories(nlohmann_json INTERFACE "${NLOHMANN_JSON_INCLUDE_DIR}")
add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)

# Collect liboai sources
set(LIBOAI_SOURCES
    ${LIBOAI_SOURCE_DIR}/liboai/components/audio.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/azure.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/chat.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/completions.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/edits.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/embeddings.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/files.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/fine_tunes.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/images.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/models.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/components/moderations.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/core/authorization.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/core/netimpl.cpp
    ${LIBOAI_SOURCE_DIR}/liboai/core/response.cpp
)

# Create liboai library
add_library(_liboai ${LIBOAI_SOURCES})

target_include_directories(_liboai SYSTEM PUBLIC
    ${LIBOAI_SOURCE_DIR}/liboai/include
)

target_compile_features(_liboai PUBLIC cxx_std_17)

target_link_libraries(_liboai 
    PUBLIC
        nlohmann_json::nlohmann_json
        ch_contrib::curl
)

# Disable warnings for third-party code
target_compile_options(_liboai PRIVATE -w)

add_library(ch_contrib::liboai ALIAS _liboai) 