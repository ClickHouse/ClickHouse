
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_path_sources(storages_system .)
add_library(storages_system ${storages_system_sources})
