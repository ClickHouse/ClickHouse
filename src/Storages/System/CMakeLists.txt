# The file StorageSystemContributors.cpp is generated at release time and committed to the source tree.
# You can also regenerate it manually this way:
# execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/StorageSystemContributors.sh)

set (CONFIG_BUILD ${CMAKE_CURRENT_BINARY_DIR}/StorageSystemBuildOptions.generated.cpp)
get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)
get_property (BUILD_INCLUDE_DIRECTORIES DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
string (TIMESTAMP BUILD_DATE "%Y-%m-%d" UTC)
configure_file (StorageSystemBuildOptions.generated.cpp.in ${CONFIG_BUILD})

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(storages_system .)
list (APPEND storages_system_sources ${CONFIG_BUILD})

add_custom_target(generate-contributors
    ./StorageSystemContributors.sh
    SOURCES StorageSystemContributors.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    # BYPRODUCTS StorageSystemContributors.generated.cpp
)

if(NOT TARGET generate-source)
    add_custom_target(generate-source)
endif()

add_dependencies(generate-source generate-contributors)

set(GENERATED_LICENSES_SRC ${CMAKE_CURRENT_BINARY_DIR}/StorageSystemLicenses.generated.cpp)

add_custom_command(
    OUTPUT StorageSystemLicenses.generated.cpp
    COMMAND ./StorageSystemLicenses.sh > ${GENERATED_LICENSES_SRC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

list (APPEND storages_system_sources ${GENERATED_LICENSES_SRC})
# Overlength strings
set_source_files_properties(${GENERATED_LICENSES_SRC} PROPERTIES COMPILE_FLAGS -w)

add_library(clickhouse_storages_system ${storages_system_headers} ${storages_system_sources})
target_link_libraries(clickhouse_storages_system PRIVATE
    dbms
    common
    string_utils
    clickhouse_common_zookeeper
    clickhouse_parsers
    Poco::JSON
)
