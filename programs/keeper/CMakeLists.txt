include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set(CLICKHOUSE_KEEPER_SOURCES
    Keeper.cpp
)

if (OS_LINUX)
    set (LINK_RESOURCE_LIB INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_keeper_configs> -Wl,${NO_WHOLE_ARCHIVE}")
endif ()

set (CLICKHOUSE_KEEPER_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        daemon
        dbms

    ${LINK_RESOURCE_LIB}
)

clickhouse_program_add(keeper)

install (FILES keeper_config.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-keeper" COMPONENT clickhouse-keeper)

clickhouse_embed_binaries(
    TARGET clickhouse_keeper_configs
    RESOURCES keeper_config.xml keeper_embedded.xml
)
add_dependencies(clickhouse-keeper-lib clickhouse_keeper_configs)
