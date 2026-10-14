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

# Track dependencies for #embed directives so that changes to embedded files trigger recompilation.
set_source_files_properties(Keeper.cpp PROPERTIES OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/keeper_embedded.xml")

clickhouse_program_add(keeper)

if (BUILD_STANDALONE_KEEPER)
    # Straight list of all required sources
    clickhouse_add_executable(clickhouse-keeper ${CLICKHOUSE_KEEPER_SOURCES})
    target_include_directories(clickhouse-keeper PRIVATE .)
    target_link_libraries(clickhouse-keeper PUBLIC ${CLICKHOUSE_KEEPER_LINK})

    set_target_properties(clickhouse-keeper PROPERTIES RUNTIME_OUTPUT_DIRECTORY ../)
    if (SPLIT_DEBUG_SYMBOLS)
        clickhouse_split_debug_symbols(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLIT_DEBUG_SYMBOLS_DIR} BINARY_PATH ../clickhouse-keeper)
    endif()
endif()
