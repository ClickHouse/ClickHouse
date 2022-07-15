include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

if (OS_LINUX)
    set (LINK_RESOURCE_LIB INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_keeper_configs> -Wl,${NO_WHOLE_ARCHIVE}")
    # for some reason INTERFACE linkage doesn't work for standalone binary
    set (LINK_RESOURCE_LIB_STANDALONE_KEEPER "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_keeper_configs> -Wl,${NO_WHOLE_ARCHIVE}")
endif ()

clickhouse_embed_binaries(
    TARGET clickhouse_keeper_configs
    RESOURCES keeper_config.xml keeper_embedded.xml
)

set(CLICKHOUSE_KEEPER_SOURCES
    Keeper.cpp
    TinyContext.cpp
)

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

install(FILES keeper_config.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-keeper" COMPONENT clickhouse-keeper)
add_dependencies(clickhouse-keeper-lib clickhouse_keeper_configs)

if (BUILD_STANDALONE_KEEPER)
    # Sraight list of all required sources
    set(CLICKHOUSE_KEEPER_STANDALONE_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/ACLMap.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/Changelog.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/CoordinationSettings.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/FourLetterCommand.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/InMemoryLogStore.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperConnectionStats.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperDispatcher.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperLogStore.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperSnapshotManager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperStateMachine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperStateManager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperStorage.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/pathUtils.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/SessionExpiryQueue.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/SummingStateMachine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/WriteBufferFromNuraftBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/ZooKeeperDataReader.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/SettingsFields.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/BaseSettings.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/Field.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/SettingsEnums.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/ServerUUID.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/UUID.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/KeeperTCPHandler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/TCPServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/ProtocolServerAdapter.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CachedCompressedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CheckingCompressedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedReadBufferBase.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedReadBufferFromFile.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedWriteBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecLZ4.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecMultiple.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecNone.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecZSTD.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionFactory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/ICompressionCodec.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/LZ4_decompress_faster.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/IKeeper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/TestKeeper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperCommon.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperConstants.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperImpl.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperIO.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperLock.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperNodeCache.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Daemon/BaseDaemon.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Daemon/SentryWriter.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Daemon/GraphiteWriter.cpp

        Keeper.cpp
        TinyContext.cpp
        clickhouse-keeper.cpp

    )

    add_executable(clickhouse-keeper ${CLICKHOUSE_KEEPER_STANDALONE_SOURCES})

    # Remove some redundant dependencies
    target_compile_definitions (clickhouse-keeper PRIVATE -DKEEPER_STANDALONE_BUILD)

    target_include_directories(clickhouse-keeper PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../src") # uses includes from src directory
    target_include_directories(clickhouse-keeper PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/../../src/Core/include") # uses some includes from core
    target_include_directories(clickhouse-keeper PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/../../src") # uses some includes from common

    target_link_libraries(clickhouse-keeper
        PRIVATE
            ch_contrib::abseil_swiss_tables
            ch_contrib::nuraft
            ch_contrib::lz4
            ch_contrib::zstd
            ch_contrib::cityhash
            common ch_contrib::double_conversion
            ch_contrib::dragonbox_to_chars
            pcg_random
            ch_contrib::pdqsort
            ch_contrib::miniselect
            clickhouse_common_config_no_zookeeper_log
            loggers_no_text_log
            clickhouse_common_io
            clickhouse_parsers # Otherwise compression will not built. FIXME.

        ${LINK_RESOURCE_LIB_STANDALONE_KEEPER}
    )

    add_dependencies(clickhouse-keeper clickhouse_keeper_configs)
    set_target_properties(clickhouse-keeper PROPERTIES RUNTIME_OUTPUT_DIRECTORY ../)

    if (SPLIT_DEBUG_SYMBOLS)
        clickhouse_split_debug_symbols(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH ../clickhouse-keeper)
    else()
        clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR})
        install(TARGETS clickhouse-keeper RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif()
endif()
