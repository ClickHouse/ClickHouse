include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(hilite_comparator .)

add_library(hilite_comparator ${hilite_comparator_sources})

target_link_libraries(hilite_comparator PRIVATE dbms)
