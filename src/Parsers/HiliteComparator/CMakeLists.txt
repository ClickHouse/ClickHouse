include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(hilite_comparator .)

add_library(hilite_comparator ${hilite_comparator_sources})

target_link_libraries(hilite_comparator PRIVATE dbms)

clickhouse_add_executable(hilite_comparator_main hilite_comparator_main.cpp)
target_link_libraries(hilite_comparator_main PRIVATE hilite_comparator)
target_link_libraries(hilite_comparator_main PRIVATE dbms)