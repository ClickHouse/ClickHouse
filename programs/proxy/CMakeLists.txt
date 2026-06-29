set(CLICKHOUSE_PROXY_SOURCES
    ProxyServer.cpp
)

set (CLICKHOUSE_PROXY_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io

    PUBLIC
        daemon
)
if (TARGET ch_contrib::jemalloc)
    list(APPEND CLICKHOUSE_PROXY_LINK PRIVATE ch_contrib::jemalloc)
endif()

clickhouse_program_add(proxy)

install(FILES config.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-proxy" COMPONENT clickhouse)
