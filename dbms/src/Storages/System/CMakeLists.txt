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
add_library(clickhouse_storages_system ${storages_system_headers} ${storages_system_sources})
target_link_libraries(clickhouse_storages_system PRIVATE dbms common string_utils clickhouse_common_zookeeper)
