include(${ClickHouse_SOURCE_DIR}/cmake/strip_binary.cmake)

set (CLICKHOUSE_LIBRARY_BRIDGE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/library-bridge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/LibraryInterface.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/LibraryBridge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Handlers.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/HandlerFactory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/SharedLibraryHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/SharedLibraryHandlerFactory.cpp
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

if (INSTALL_STRIPPED_BINARIES)
    clickhouse_strip_binary(TARGET clickhouse-library-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${STRIPPED_BINARIES_OUTPUT} BINARY_PATH ../clickhouse-library-bridge)
else()
    clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse-library-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${STRIPPED_BINARIES_OUTPUT})
    install(TARGETS clickhouse-library-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif()
