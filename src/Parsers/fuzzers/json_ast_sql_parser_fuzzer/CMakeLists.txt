# Structure-aware fuzzer: binary protobuf (json_ast.proto) -> JSON AST -> IAST -> SQL -> parser.
# See README.md in this directory.

# `json_ast.proto` imports `google/protobuf/descriptor.proto` for the `(json_text)` enum value option.
set(Protobuf_IMPORT_DIRS "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")
set(PROTOBUF_GENERATE_CPP_APPEND_PATH TRUE)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS json_ast.proto)
set_source_files_properties(${PROTO_SRCS} PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier -Wno-extra-semi-stmt -Wno-used-but-marked-unused -Wno-unreachable-code-return -Wno-nullability-extension -Wno-missing-prototypes -Wno-redundant-parens")

# Shared by this fuzzer, `json_ast_sql_execution_fuzzer` (programs/local/fuzzers) and the seed corpus converter:
# the generated protobuf code, the protobuf <-> JSON conversion and the JSON -> AST -> SQL pipeline stages.
add_library(json_ast_fuzzer_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS} JSONASTProtoConverter.cpp JSONASTFuzzerPipeline.cpp)
target_include_directories(json_ast_fuzzer_proto SYSTEM BEFORE PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(json_ast_fuzzer_proto PUBLIC ch_contrib::protobuf Poco::JSON clickhouse_parsers)

clickhouse_add_executable(json_ast_sql_parser_fuzzer json_ast_sql_parser_fuzzer.cpp)
# contrib/libprotobuf-mutator/src/libfuzzer/libfuzzer_macro.h has no newline at end of file, and
# DEFINE_BINARY_PROTO_FUZZER expands to extern "C" functions without prior declarations.
target_compile_options(json_ast_sql_parser_fuzzer PRIVATE -Wno-newline-eof -Wno-missing-prototypes)
# `dbms` is needed like for `select_parser_fuzzer`: formatting literals and identifiers pulls in `Field` and `Context` symbols.
target_link_libraries(json_ast_sql_parser_fuzzer PRIVATE json_ast_fuzzer_proto ch_contrib::protobuf_mutator clickhouse_parsers dbms)

# Not a fuzzer (the name deliberately does not end with `_fuzzer`, so the `fuzzers` bundle does not
# pick it up), but it is compiled with -fsanitize=fuzzer-no-link like every target of a fuzzing
# build, so it needs the SanitizerCoverage callbacks from the libFuzzer runtime.
clickhouse_add_executable(json_ast_seed_converter json_ast_seed_converter.cpp)
target_link_libraries(json_ast_seed_converter PRIVATE json_ast_fuzzer_proto ch_contrib::fuzzer_no_main)
