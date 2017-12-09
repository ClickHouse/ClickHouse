# 'clickhouse' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.

add_library (clickhouse-server-lib
    ConfigReloader.cpp
    HTTPHandler.cpp
    InterserverIOHTTPHandler.cpp
    MetricsTransmitter.cpp
    NotFoundHandler.cpp
    PingRequestHandler.cpp
    ReplicasStatusHandler.cpp
    RootRequestHandler.cpp
    Server.cpp
    StatusFile.cpp
    TCPHandler.cpp
    )

target_link_libraries (clickhouse-server-lib clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)
target_include_directories (clickhouse-server-lib PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

add_library (clickhouse-local-lib LocalServer.cpp)
target_link_libraries (clickhouse-local-lib clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)

add_library (clickhouse-extract-from-config-lib ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (clickhouse-extract-from-config-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-client-lib Client.cpp)
target_link_libraries (clickhouse-client-lib clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)

add_library (clickhouse-benchmark-lib ${SPLIT_SHARED} Benchmark.cpp)
target_link_libraries (clickhouse-benchmark-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-benchmark-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (clickhouse-performance-test-lib ${SPLIT_SHARED} PerformanceTest.cpp)
target_link_libraries (clickhouse-performance-test-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-performance-test-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (clickhouse-compressor-lib ${SPLIT_SHARED} Compressor.cpp)
target_link_libraries (clickhouse-compressor-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-format-lib ${SPLIT_SHARED} Format.cpp)
target_link_libraries (clickhouse-format-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (USE_EMBEDDED_COMPILER)
    link_directories (${LLVM_LIBRARY_DIRS})
    add_subdirectory ("Compiler-${LLVM_VERSION}")
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-server clickhouse-server.cpp)
    target_link_libraries (clickhouse-server clickhouse-server-lib)
    add_executable (clickhouse-client clickhouse-client.cpp)
    target_link_libraries (clickhouse-client clickhouse-client-lib)
    add_executable (clickhouse-local clickhouse-local.cpp)
    target_link_libraries (clickhouse-local clickhouse-local-lib)
    add_executable (clickhouse-benchmark clickhouse-benchmark.cpp)
    target_link_libraries (clickhouse-benchmark clickhouse-benchmark-lib clickhouse_aggregate_functions)
    add_executable (clickhouse-performance-test clickhouse-performance-test.cpp)
    target_link_libraries (clickhouse-performance-test clickhouse-performance-test-lib clickhouse-client-lib)
    add_executable (clickhouse-extract-from-config clickhouse-extract-from-config.cpp)
    target_link_libraries (clickhouse-extract-from-config clickhouse-extract-from-config-lib)
    # now in utils
    #add_executable (clickhouse-compressor clickhouse-compressor.cpp)
    #target_link_libraries (clickhouse-compressor clickhouse-compressor-lib)
    add_executable (clickhouse-format clickhouse-format.cpp)
    target_link_libraries (clickhouse-format clickhouse-format-lib dbms)

    if (USE_EMBEDDED_COMPILER)
        add_executable (clickhouse-clang clickhouse-clang.cpp)
        target_link_libraries (clickhouse-clang clickhouse-compiler-lib)
        add_executable (clickhouse-lld clickhouse-lld.cpp)
        target_link_libraries (clickhouse-lld clickhouse-compiler-lib)
    endif ()

    add_custom_target (clickhouse ALL DEPENDS clickhouse-server clickhouse-client clickhouse-local clickhouse-benchmark clickhouse-performance-test clickhouse-extract-from-config clickhouse-compressor clickhouse-format)
else ()

add_executable (clickhouse main.cpp)
target_include_directories (clickhouse BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
target_link_libraries (clickhouse
    clickhouse-server-lib
    clickhouse-client-lib
    clickhouse-local-lib
    clickhouse-benchmark-lib
    clickhouse-performance-test-lib
    clickhouse-extract-from-config-lib
    clickhouse-compressor-lib
    clickhouse-format-lib
    dbms
    )

if (USE_EMBEDDED_COMPILER)
    target_link_libraries (clickhouse clickhouse-compiler-lib)
endif ()

install (TARGETS clickhouse RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)

add_custom_target (clickhouse-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-server DEPENDS clickhouse)
add_custom_target (clickhouse-client ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-client DEPENDS clickhouse)
add_custom_target (clickhouse-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-local DEPENDS clickhouse)
add_custom_target (clickhouse-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-benchmark DEPENDS clickhouse)
add_custom_target (clickhouse-performance-test ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-performance-test DEPENDS clickhouse)
add_custom_target (clickhouse-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-format DEPENDS clickhouse)
# install always because depian package want this files:
add_custom_target (clickhouse-clang ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-clang DEPENDS clickhouse)
add_custom_target (clickhouse-lld ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-lld DEPENDS clickhouse)

endif ()

install (
    FILES config.xml users.xml
    DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server
    COMPONENT clickhouse)
