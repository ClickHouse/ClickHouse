# In-process `clickhouse local` harness shared by the targets that execute SQL.
add_library(clickhouse_local_fuzzer_runner STATIC LocalFuzzerRunner.cpp)
target_include_directories(clickhouse_local_fuzzer_runner PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(clickhouse_local_fuzzer_runner PRIVATE ..)
target_link_libraries(clickhouse_local_fuzzer_runner PUBLIC clickhouse-local-lib common dbms)
if (TARGET ch_contrib::jemalloc)
    target_link_libraries(clickhouse_local_fuzzer_runner PUBLIC ch_contrib::jemalloc)
endif()

# Byte-level: the input is the query text.
clickhouse_add_executable(clickhouse_fuzzer clickhouse_fuzzer.cpp)
target_link_libraries(clickhouse_fuzzer PRIVATE clickhouse_local_fuzzer_runner)

# Structure-aware: the input is a protobuf JSON AST (see src/Parsers/fuzzers/json_ast_sql_parser_fuzzer),
# executed against the fixture in json_ast_sql_execution_fuzzer_schema.sql.
if (TARGET json_ast_fuzzer_proto)
    # Embed the fixture into the binary, so the fuzzer needs no files next to it in CI.
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/json_ast_sql_execution_fuzzer_schema.sql" JSON_AST_SQL_EXECUTION_FUZZER_SCHEMA)
    configure_file(json_ast_sql_execution_fuzzer_schema.h.in "${CMAKE_CURRENT_BINARY_DIR}/json_ast_sql_execution_fuzzer_schema.h" @ONLY)

    clickhouse_add_executable(json_ast_sql_execution_fuzzer json_ast_sql_execution_fuzzer.cpp)
    target_include_directories(json_ast_sql_execution_fuzzer PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
    # See src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/CMakeLists.txt for these flags.
    target_compile_options(json_ast_sql_execution_fuzzer PRIVATE -Wno-newline-eof -Wno-missing-prototypes)
    target_link_libraries(json_ast_sql_execution_fuzzer PRIVATE clickhouse_local_fuzzer_runner json_ast_fuzzer_proto ch_contrib::protobuf_mutator)
endif()
