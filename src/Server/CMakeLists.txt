include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_server .)

add_headers_and_sources(clickhouse_server "${CMAKE_CURRENT_BINARY_DIR}/generated/")

add_library(clickhouse_server ${clickhouse_server_sources})

if (TARGET ch_contrib::grpc)
    add_subdirectory (grpc_protos)
endif()

if (TARGET ch_contrib::acme-lw)
    target_link_libraries(clickhouse_server PRIVATE ch_contrib::acme-lw)
endif()
