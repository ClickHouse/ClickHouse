# configure_file(config_jsonparsers.h.in ${ConfigIncludePath}/config_jsonparsers.h)

# include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

# add_headers_and_sources(clickhouse_common_jsonparsers .)

# add_library(clickhouse_common_jsonparsers ${clickhouse_common_jsonparsers_headers} ${clickhouse_common_jsonparsers_sources})

# if(USE_SIMDJSON)
#     target_link_libraries(clickhouse_common_jsonparsers PRIVATE simdjson)
# endif()

# if(USE_RAPIDJSON)
#     target_include_directories(clickhouse_common_jsonparsers SYSTEM PRIVATE ${RAPIDJSON_INCLUDE_DIR})
# endif()
