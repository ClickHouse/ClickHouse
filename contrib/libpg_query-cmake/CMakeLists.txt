option (ENABLE_LIBPG_QUERY "Enable libpg_query" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBPG_QUERY)
    message(STATUS "Not using libpg_query")
    return()
endif()

set (LIBRARY_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpg_query")
set (LIBRARY_BINARY_DIR "${LIBRARY_SOURCE_DIR}/build")
set (LIBRARY_STATIC_LIB "${LIBRARY_BINARY_DIR}/libpg_query.a")


add_custom_command(
  OUTPUT ${LIBRARY_STATIC_LIB}
  COMMAND make
  WORKING_DIRECTORY ${LIBRARY_SOURCE_DIR}
  COMMENT "Building external library libpg_query"
)

add_library(_libpg_query STATIC IMPORTED)
set_property(TARGET _libpg_query PROPERTY IMPORTED_LOCATION ${LIBRARY_STATIC_LIB})
add_dependencies(_libpg_query ${LIBRARY_STATIC_LIB})


# target_include_directories(_libpg_query INTERFACE "${LIBRARY_SOURCE_DIR}/src/include")
target_include_directories(_libpg_query INTERFACE "${LIBRARY_SOURCE_DIR}")

add_library(ch_contrib::libpg_query ALIAS _libpg_query)
