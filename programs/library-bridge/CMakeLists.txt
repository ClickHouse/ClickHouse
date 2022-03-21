include(${ClickHouse_SOURCE_DIR}/cmake/strip_binary.cmake)

set (CLICKHOUSE_LIBRARY_BRIDGE_SOURCES
    library-bridge.cpp
    LibraryInterface.cpp
    LibraryBridge.cpp
    Handlers.cpp
    HandlerFactory.cpp
    SharedLibraryHandler.cpp
    SharedLibraryHandlerFactory.cpp
)

if (OS_LINUX)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-export-dynamic")
endif ()

add_executable(clickhouse-library-bridge ${CLICKHOUSE_LIBRARY_BRIDGE_SOURCES})

target_link_libraries(clickhouse-library-bridge PRIVATE
    daemon
    dbms
    bridge
)

set_target_properties(clickhouse-library-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

if (BUILD_STRIPPED_BINARIES_PREFIX)
    clickhouse_strip_binary(TARGET clickhouse-library-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${BUILD_STRIPPED_BINARIES_PREFIX} BINARY_PATH ../clickhouse-library-bridge)
endif()

if (NOT BUILD_STRIPPED_BINARIES_PREFIX)
    install(TARGETS clickhouse-library-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif()
