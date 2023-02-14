option(ENABLE_DUCKDB "Enable duckdb" ${ENABLE_LIBRARIES})

if (NOT ENABLE_DUCKDB)
    message(STATUS "Not using duckdb")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/duckdb-amalgamation")

set(SRCS ${LIBRARY_DIR}/duckdb.cpp)

add_library(_duckdb ${SRCS})
target_include_directories(_duckdb SYSTEM PUBLIC "${LIBRARY_DIR}")
add_library(ch_contrib::duckdb ALIAS _duckdb)
