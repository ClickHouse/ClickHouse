set(CLICKHOUSE_KEEPER_SOURCES
    Keeper.cpp
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

if (BUILD_STANDALONE_KEEPER)
    # Straight list of all required sources
    set(CLICKHOUSE_KEEPER_STANDALONE_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperReconfiguration.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/RaftServerConfig.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/ACLMap.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/Changelog.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/CoordinationSettings.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/FourLetterCommand.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/InMemoryLogStore.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperConnectionStats.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperDispatcher.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperLogStore.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperContext.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperFeatureFlags.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperSnapshotManager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperSnapshotManagerS3.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperStateMachine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperContext.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperStateManager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperStorage.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/KeeperAsynchronousMetrics.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/pathUtils.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/SessionExpiryQueue.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/SummingStateMachine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/WriteBufferFromNuraftBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/ZooKeeperDataReader.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/SettingsFields.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/BaseSettings.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/ServerSettings.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/Field.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/SettingsEnums.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/ServerUUID.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/UUID.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Core/BackgroundSchedulePool.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/IO/ReadBuffer.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTPPathHints.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/KeeperTCPHandler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/TCPServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/NotFoundHandler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/ProtocolServerAdapter.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/CertificateReloader.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/PrometheusRequestHandler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/PrometheusMetricsWriter.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/waitServersToFinish.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/ServerType.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTPRequestHandlerFactoryMain.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/HTTPServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/ReadHeaders.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/HTTPServerConnection.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/HTTPServerRequest.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/HTTPServerResponse.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/HTTPServerConnectionFactory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CachedCompressedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CheckingCompressedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedReadBufferBase.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedReadBufferFromFile.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressedWriteBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecEncrypted.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecLZ4.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecMultiple.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecNone.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionCodecZSTD.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/CompressionFactory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/ICompressionCodec.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Compression/LZ4_decompress_faster.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/CurrentThread.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/NamedCollections/NamedCollections.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/NamedCollections/NamedCollectionConfiguration.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/IKeeper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/TestKeeper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperCommon.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperConstants.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperImpl.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperIO.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperLock.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Common/ZooKeeper/ZooKeeperNodeCache.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/registerDisks.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IDisk.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/DiskFactory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/DiskSelector.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/DiskLocal.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/DiskLocalCheckThread.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/LocalDirectorySyncGuard.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/TemporaryFileOnDisk.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/loadLocalDiskConfig.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/IObjectStorage.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/MetadataStorageFromDisk.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/MetadataFromDiskTransactionState.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/DiskObjectStorageMetadata.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/DiskObjectStorage.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/DiskObjectStorageTransaction.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/DiskObjectStorageRemoteMetadataRestoreHelper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/DiskObjectStorageCommon.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/ObjectStorageIteratorAsync.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/ObjectStorageIterator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/StoredObject.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/S3/registerDiskS3.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/S3/S3ObjectStorage.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/S3/S3Capabilities.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/ObjectStorages/S3/diskSettings.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/createReadBufferFromFileBase.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/ReadBufferFromRemoteFSGather.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/IOUringReader.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/WriteBufferFromTemporaryFile.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/WriteBufferWithFinalizeCallback.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/AsynchronousBoundedReadBuffer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/getThreadPoolReader.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/ThreadPoolRemoteFSReader.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Disks/IO/ThreadPoolReader.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Storages/StorageS3Settings.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Daemon/BaseDaemon.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Daemon/SentryWriter.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Daemon/GraphiteWriter.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Daemon/GitHash.generated.cpp

        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/Standalone/Context.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/Standalone/Settings.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Coordination/Standalone/ThreadStatusExt.cpp

        Keeper.cpp
        clickhouse-keeper.cpp
    )

    # List of resources for clickhouse-keeper client
    if (ENABLE_CLICKHOUSE_KEEPER_CLIENT)
        list(APPEND CLICKHOUSE_KEEPER_STANDALONE_SOURCES
            ${CMAKE_CURRENT_SOURCE_DIR}/../../programs/keeper-client/KeeperClient.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/../../programs/keeper-client/Commands.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/../../programs/keeper-client/Parser.cpp

            ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Client/LineReader.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/../../src/Client/ReplxxLineReader.cpp
        )
    endif()

    clickhouse_add_executable(clickhouse-keeper ${CLICKHOUSE_KEEPER_STANDALONE_SOURCES})

    # Remove some redundant dependencies
    target_compile_definitions (clickhouse-keeper PRIVATE -DCLICKHOUSE_PROGRAM_STANDALONE_BUILD)
    target_compile_definitions (clickhouse-keeper PUBLIC -DWITHOUT_TEXT_LOG)

    if (ENABLE_CLICKHOUSE_KEEPER_CLIENT AND TARGET ch_rust::skim)
        target_link_libraries(clickhouse-keeper PRIVATE ch_rust::skim)
    endif()

    target_link_libraries(clickhouse-keeper
        PRIVATE
            ch_contrib::abseil_swiss_tables
            ch_contrib::nuraft
            ch_contrib::lz4
            ch_contrib::zstd
            ch_contrib::cityhash
            ch_contrib::jemalloc
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

    set_target_properties(clickhouse-keeper PROPERTIES RUNTIME_OUTPUT_DIRECTORY ../)

    if (SPLIT_DEBUG_SYMBOLS)
        clickhouse_split_debug_symbols(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH ../clickhouse-keeper)
    else()
        clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse-keeper DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR})
        install(TARGETS clickhouse-keeper RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif()
endif()
