option(ENABLE_DUCKDB "Enable duckdb" ${ENABLE_LIBRARIES})

set (DUCKDB_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/duckdb-amalgamation")

set (SRCS
    "${DUCKDB_LIBRARY_DIR}/duckdb-1.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-2.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-3.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-4.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-5.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-6.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-7.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-8.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-9.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-10.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-fastpforlib.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-fmt.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-fsst.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-hyperloglog.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-libpg_query.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-mbedtls.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-miniz.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-re2.cpp"
    "${DUCKDB_LIBRARY_DIR}/duckdb-utf8proc.cpp"
)

add_library(_duckdb ${SRCS})
target_include_directories(_duckdb SYSTEM PUBLIC "${DUCKDB_LIBRARY_DIR}")
add_library(ch_contrib::duckdb ALIAS _duckdb)
