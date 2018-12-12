#FUTURE:
#include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
#add_headers_and_sources(clickhouse_datastreams .)
#add_library(clickhouse_datastreams ${LINK_MODE} ${clickhouse_datastreams_sources})
#target_link_libraries(clickhouse_datastreams PRIVATE clickhouse_common_io pocoext)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
