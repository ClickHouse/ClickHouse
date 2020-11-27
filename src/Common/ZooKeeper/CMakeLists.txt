include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_subdirectory (etcd_protos)

add_headers_and_sources(clickhouse_common_zookeeper .)

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(clickhouse_common_zookeeper ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources})

target_link_libraries (clickhouse_common_zookeeper PUBLIC  clickhouse_etcd_protos clickhouse_common_io common PRIVATE string_utils)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()