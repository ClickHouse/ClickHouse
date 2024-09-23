PROTOBUF_GENERATE_CPP(GRAMMAR_SRC GRAMMAR_HDR ch-fuzz/ast/sql_grammar.proto)

set (CLICKHOUSE_CLIENT_SOURCES
    Client.cpp
    # CH fuzz
    ch-fuzz/ast/sql_query_proto_to_string.cpp
    ch-fuzz/generator/sql_query.cpp
    ch-fuzz/generator/sql_expression.cpp
    ch-fuzz/generator/sql_types.cpp
    ch-fuzz/generator/statement_generator.cpp
    ch-fuzz/generator/random_settings.cpp
    ${GRAMMAR_SRC}
)
set_source_files_properties(${CLICKHOUSE_CLIENT_SOURCES} PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier -Wno-extra-semi-stmt -Wno-used-but-marked-unused")

set (CLICKHOUSE_CLIENT_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        ch_contrib::protoc
)

if (TARGET ch_rust::skim)
    list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_rust::skim)
endif()

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE readpassphrase)

set (CLICKHOUSE_CLIENT_INCLUDE
    PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/ch-fuzz/ast"
    "${CMAKE_CURRENT_BINARY_DIR}/ch-fuzz/generator"
)

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-client" COMPONENT clickhouse-client RENAME config.xml)
