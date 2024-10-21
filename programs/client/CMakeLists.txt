PROTOBUF_GENERATE_CPP(GRAMMAR_SRC GRAMMAR_HDR buzz-house/ast/sql_grammar.proto)

set (CLICKHOUSE_CLIENT_SOURCES
    Client.cpp
    # BuzzHouse
    buzz-house/ast/sql_query_proto_to_string.cpp
    buzz-house/generator/sql_query.cpp
    buzz-house/generator/sql_expression.cpp
    buzz-house/generator/sql_types.cpp
    buzz-house/generator/statement_generator.cpp
    buzz-house/generator/random_settings.cpp
    buzz-house/generator/query_oracle.cpp
    buzz-house/third_party/hugeint.cpp
    buzz-house/third_party/uhugeint.cpp
    ${GRAMMAR_SRC}
)
set_source_files_properties(${CLICKHOUSE_CLIENT_SOURCES} PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier -Wno-extra-semi-stmt -Wno-used-but-marked-unused -Wno-covered-switch-default")

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
    buzz-house/ast
    buzz-house/generator
    buzz-house/third_party
    "${CMAKE_CURRENT_BINARY_DIR}"
)

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-client" COMPONENT clickhouse-client RENAME config.xml)
