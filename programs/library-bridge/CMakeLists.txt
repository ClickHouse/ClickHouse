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
    createFunctionBaseCast.cpp
)

clickhouse_add_executable(clickhouse-library-bridge ${CLICKHOUSE_LIBRARY_BRIDGE_SOURCES})

target_link_libraries(clickhouse-library-bridge PRIVATE
    daemon
    dbms
    bridge
)

set_target_properties(clickhouse-library-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

if (SPLIT_DEBUG_SYMBOLS)
    clickhouse_split_debug_symbols(TARGET clickhouse-library-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH ../clickhouse-library-bridge)
else()
    clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse-library-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR})
    install(TARGETS clickhouse-library-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif()
