set(CLICKHOUSE_KEEPER_SOURCES
    keeper_main.cpp
    Keeper.cpp
)

set (CLICKHOUSE_KEEPER_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper_base
        clickhouse_common_zookeeper
        daemon
        clickhouse-keeper-converter-lib
        clickhouse-keeper-client-lib
        clickhouse_functions
)

clickhouse_program_add(keeper)

install(FILES keeper_config.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-keeper" COMPONENT clickhouse-keeper)

if (BUILD_STANDALONE_KEEPER)
    # Straight list of all required sources
    clickhouse_add_executable(clickhouse-keeper ${CLICKHOUSE_KEEPER_SOURCES})
    target_include_directories(clickhouse-keeper PRIVATE .)
    target_link_libraries(clickhouse-keeper PUBLIC ${CLICKHOUSE_KEEPER_LINK})

    set_target_properties(clickhouse-keeper PROPERTIES RUNTIME_OUTPUT_DIRECTORY ../)
    if (SPLIT_DEBUG_SYMBOLS)
        clickhouse_split_debug_symbols(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLIT_DEBUG_SYMBOLS_DIR} BINARY_PATH ../clickhouse-keeper)
    else()
        clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLIT_DEBUG_SYMBOLS_DIR})
        install(TARGETS clickhouse-keeper RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif()

    if (ENABLE_CLICKHOUSE_KEEPER_CLIENT)
        add_custom_command (TARGET clickhouse-keeper POST_BUILD COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse-keeper clickhouse-keeper-client)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-keeper-client" DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT clickhouse-keeper)
    endif()
endif()
