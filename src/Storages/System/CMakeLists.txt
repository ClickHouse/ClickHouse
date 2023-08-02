# The file StorageSystemContributors.cpp is generated at release time and committed to the source tree.
# You can also regenerate it manually this way:
# execute_process(COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/StorageSystemContributors.sh")

include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set (CONFIG_BUILD "${CMAKE_CURRENT_BINARY_DIR}/StorageSystemBuildOptions.generated.cpp")
get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)

get_property(TZDATA_VERSION GLOBAL PROPERTY TZDATA_VERSION_PROP)


find_package(Git)
if(Git_FOUND)
    # The commit's git hash, and whether the building workspace was dirty or not
    execute_process(COMMAND
      "${GIT_EXECUTABLE}" rev-parse HEAD
      WORKING_DIRECTORY "${ClickHouse_SOURCE_DIR}"
      OUTPUT_VARIABLE GIT_HASH
      ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    # Git branch name
    execute_process(COMMAND
      "${GIT_EXECUTABLE}" rev-parse --abbrev-ref HEAD
      WORKING_DIRECTORY "${ClickHouse_SOURCE_DIR}"
      OUTPUT_VARIABLE GIT_BRANCH
      ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    # The date of the commit
    SET(ENV{TZ} "UTC")
    execute_process(COMMAND
      "${GIT_EXECUTABLE}" log -1 --format=%ad --date=iso-local
      WORKING_DIRECTORY "${ClickHouse_SOURCE_DIR}"
      OUTPUT_VARIABLE GIT_DATE
      ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    # The subject of the commit
    execute_process(COMMAND
      "${GIT_EXECUTABLE}" log -1 --format=%s
      WORKING_DIRECTORY "${ClickHouse_SOURCE_DIR}"
      OUTPUT_VARIABLE GIT_COMMIT_SUBJECT
      ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

function(generate_system_build_options)
    include(${ClickHouse_SOURCE_DIR}/src/configure_config.cmake)
    include(${ClickHouse_SOURCE_DIR}/src/Functions/configure_config.cmake)
    include(${ClickHouse_SOURCE_DIR}/src/Formats/configure_config.cmake)
    configure_file(StorageSystemBuildOptions.generated.cpp.in ${CONFIG_BUILD})
endfunction()
generate_system_build_options()

include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
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

set(GENERATED_LICENSES_SRC "${CMAKE_CURRENT_BINARY_DIR}/StorageSystemLicenses.generated.cpp")
set(GENERATED_TIMEZONES_SRC "${CMAKE_CURRENT_BINARY_DIR}/StorageSystemTimeZones.generated.cpp")

add_custom_command(
    OUTPUT StorageSystemLicenses.generated.cpp
    COMMAND ./StorageSystemLicenses.sh > ${GENERATED_LICENSES_SRC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

list (APPEND storages_system_sources ${GENERATED_LICENSES_SRC})
list (APPEND storages_system_sources ${GENERATED_TIMEZONES_SRC})

# Overlength strings
set_source_files_properties(${GENERATED_LICENSES_SRC} PROPERTIES COMPILE_FLAGS -w)

clickhouse_embed_binaries(
        TARGET information_schema_metadata
        RESOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/InformationSchema/"
        RESOURCES schemata.sql tables.sql views.sql columns.sql
)

list (SORT storages_system_sources) # Reproducible build
add_library(clickhouse_storages_system ${storages_system_sources})

add_dependencies(clickhouse_storages_system information_schema_metadata)

target_link_libraries(clickhouse_storages_system PRIVATE
    dbms
    common
    string_utils
    clickhouse_common_zookeeper
    clickhouse_parsers
    Poco::JSON
    INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:information_schema_metadata> -Wl,${NO_WHOLE_ARCHIVE}"
)
