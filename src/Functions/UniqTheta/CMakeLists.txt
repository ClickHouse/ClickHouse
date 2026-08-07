include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_library(clickhouse_functions_uniqtheta FunctionsUniqTheta.cpp)

target_link_libraries(clickhouse_functions_uniqtheta PRIVATE dbms)

if (TARGET ch_contrib::datasketches)
    target_link_libraries (clickhouse_functions_uniqtheta PRIVATE ch_contrib::datasketches)
endif ()
