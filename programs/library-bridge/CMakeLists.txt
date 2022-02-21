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

install(TARGETS clickhouse-library-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
