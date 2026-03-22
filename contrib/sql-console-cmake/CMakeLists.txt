set(SQL_CONSOLE_SOURCE_DIR_DEFAULT "${ClickHouse_SOURCE_DIR}/contrib/sql-console/apps/web/dist-embedded")
set(SQL_CONSOLE_SOURCE_DIR "${SQL_CONSOLE_SOURCE_DIR_DEFAULT}" CACHE PATH "Path to built SQL Console assets directory")

 if (NOT EXISTS "${SQL_CONSOLE_SOURCE_DIR}")
    message(FATAL_ERROR
        "SQL Console assets not found at ${SQL_CONSOLE_SOURCE_DIR}. "
        "Build the standalone bundle with `EMBEDDED_BASE_PATH=/sql-console/` and pass "
        "`-DSQL_CONSOLE_SOURCE_DIR=/absolute/path/to/dist-embedded`, "
        "or place the built assets in `contrib/sql-console/apps/web/dist-embedded`.")
endif()

set(SQL_CONSOLE_DIR "${CMAKE_CURRENT_BINARY_DIR}/sql_console_compressed")
file(MAKE_DIRECTORY "${SQL_CONSOLE_DIR}")

set(SQL_CONSOLE_RESOURCES_GENERATED "${CMAKE_CURRENT_BINARY_DIR}/SQLConsoleResources.generated.h")

# Collect all source files for dependencies.
file(GLOB_RECURSE SQL_CONSOLE_FILES
    CONFIGURE_DEPENDS
    "${SQL_CONSOLE_SOURCE_DIR}/*"
)

add_custom_command(
    OUTPUT "${SQL_CONSOLE_RESOURCES_GENERATED}"
    COMMAND ${CMAKE_COMMAND}
        -DSQL_CONSOLE_SOURCE_DIR="${SQL_CONSOLE_SOURCE_DIR}"
        -DSQL_CONSOLE_DIR="${SQL_CONSOLE_DIR}"
        -DGENERATED_FILE="${SQL_CONSOLE_RESOURCES_GENERATED}"
        -P "${CMAKE_CURRENT_LIST_DIR}/embed_sql_console_resources.cmake"
    DEPENDS
        "${CMAKE_CURRENT_LIST_DIR}/embed_sql_console_resources.cmake"
        ${SQL_CONSOLE_FILES}
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    COMMENT "Generating embedded SQL Console resources"
)

add_custom_target(sql_console_resources_gen
    DEPENDS "${SQL_CONSOLE_RESOURCES_GENERATED}"
)

add_library(_sql_console_resources INTERFACE)
target_include_directories(_sql_console_resources SYSTEM INTERFACE "${CMAKE_CURRENT_BINARY_DIR}")
add_dependencies(_sql_console_resources sql_console_resources_gen)

add_library(ch_contrib::sql_console_resources ALIAS _sql_console_resources)
