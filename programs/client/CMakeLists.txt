set (CLICKHOUSE_CLIENT_SOURCES
    Client.cpp
)

set (CLICKHOUSE_CLIENT_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
)

if (TARGET ch_rust::skim)
    list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_rust::skim)
endif()
# Always use internal readpassphrase
list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE readpassphrase)

if (ENABLE_BUZZHOUSE)
    protobuf_generate_cpp(GRAMMAR_SRC GRAMMAR_HDR BuzzHouse/AST/SQLGrammar.proto)

    list(APPEND CLICKHOUSE_CLIENT_SOURCES
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

    # Needed because of protobuf
    set_source_files_properties(${GRAMMAR_SRC} PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier -Wno-extra-semi-stmt")

    list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_contrib::protoc)
    if (TARGET OpenSSL::Crypto)
        list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE OpenSSL::Crypto)
    endif()
    if (TARGET ch_contrib::mysqlxx)
        list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_contrib::mysqlxx)
    endif()
    if (TARGET ch_contrib::libpqxx)
        list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_contrib::libpqxx)
    endif()
    if (TARGET ch_contrib::sqlite)
        list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_contrib::sqlite)
    endif()
    if (TARGET ch_contrib::mongocxx)
        list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_contrib::mongocxx)
    endif()

    set (CLICKHOUSE_CLIENT_INCLUDE
        PRIVATE
        BuzzHouse/AST
        BuzzHouse/Generator
        BuzzHouse/Utils
        "${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/BuzzHouse/AST"
    )
endif()

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-client" COMPONENT clickhouse-client RENAME config.xml)
