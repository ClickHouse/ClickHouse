PROTOBUF_GENERATE_CPP(GRAMMAR_SRC GRAMMAR_HDR BuzzHouse/AST/SQLGrammar.proto)

set (CLICKHOUSE_CLIENT_SOURCES
    Client.cpp
    # BuzzHouse
    BuzzHouse/AST/SQLProtoStr.cpp
    BuzzHouse/Generator/SQLQuery.cpp
    BuzzHouse/Generator/SQLExpression.cpp
    BuzzHouse/Generator/SQLTable.cpp
    BuzzHouse/Generator/SQLTypes.cpp
    BuzzHouse/Generator/StatementGenerator.cpp
    BuzzHouse/Generator/RandomSettings.cpp
    BuzzHouse/Generator/QueryOracle.cpp
    BuzzHouse/Generator/ExternalIntegrations.cpp
    BuzzHouse/Utils/Hugeint.cpp
    BuzzHouse/Utils/UHugeint.cpp
    ${GRAMMAR_SRC}
)
set_source_files_properties(${CLICKHOUSE_CLIENT_SOURCES} PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier -Wno-extra-semi-stmt")

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
if (TARGET OpenSSL::Crypto)
    list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE OpenSSL::Crypto)
endif()

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE readpassphrase)

set (CLICKHOUSE_CLIENT_INCLUDE
    PRIVATE
    BuzzHouse/AST
    BuzzHouse/Generator
    BuzzHouse/Utils
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/BuzzHouse/AST"
)

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-client" COMPONENT clickhouse-client RENAME config.xml)
