include(${ClickHouse_SOURCE_DIR}/cmake/split_debug_symbols.cmake)

set (CLICKHOUSE_LIBRARY_BRIDGE_SOURCES
    CatBoostLibraryHandler.cpp
    CatBoostLibraryHandlerFactory.cpp
    ExternalDictionaryLibraryAPI.cpp
    ExternalDictionaryLibraryHandler.cpp
    ExternalDictionaryLibraryHandlerFactory.cpp
    LibraryBridge.cpp
    LibraryBridgeHandlerFactory.cpp
    LibraryBridgeHandlers.cpp
    SharedLibrary.cpp
    library-bridge.cpp
)

clickhouse_add_executable(clickhouse-library-bridge ${CLICKHOUSE_LIBRARY_BRIDGE_SOURCES})

target_link_libraries(clickhouse-library-bridge PRIVATE
    daemon
    dbms
    bridge
)

set_target_properties(clickhouse-library-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

install(TARGETS clickhouse-library-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
