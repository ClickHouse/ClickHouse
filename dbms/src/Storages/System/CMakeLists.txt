
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(storages_system .)
add_library(storages_system ${storages_system_sources})
