# 'clickhouse' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.

add_library (libclickhouse-server
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

target_link_libraries (libclickhouse-server clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)
target_include_directories (libclickhouse-server PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

add_library (libclickhouse-local LocalServer.cpp)
target_link_libraries (libclickhouse-local libclickhouse-server clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)

add_library (libclickhouse-extract-from-config ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (libclickhouse-extract-from-config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (libclickhouse-client Client.cpp)
target_link_libraries (libclickhouse-client clickhouse_functions clickhouse_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)

add_library (libclickhouse-benchmark ${SPLIT_SHARED} Benchmark.cpp)
target_link_libraries (libclickhouse-benchmark clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (libclickhouse-benchmark PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (libclickhouse-performance-test ${SPLIT_SHARED} PerformanceTest.cpp)
target_link_libraries (libclickhouse-performance-test clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (libclickhouse-performance-test PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (libclickhouse-compressor ${SPLIT_SHARED} Compressor.cpp)
target_link_libraries (libclickhouse-compressor clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (libclickhouse-format ${SPLIT_SHARED} Format.cpp)
target_link_libraries (libclickhouse-format clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (USE_EMBEDDED_COMPILER)
    link_directories (${LLVM_LIBRARY_DIRS})
endif ()

add_executable (clickhouse main.cpp)
target_include_directories (clickhouse BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
target_link_libraries (clickhouse
    libclickhouse-server
    libclickhouse-client
    libclickhouse-local
    libclickhouse-benchmark
    libclickhouse-performance-test
    libclickhouse-extract-from-config
    libclickhouse-compressor
    libclickhouse-format
    dbms
    )

if (USE_EMBEDDED_COMPILER)
    add_subdirectory ("Compiler-${LLVM_VERSION}")
    target_link_libraries (clickhouse libclickhouse-compiler)
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

install (
    FILES config.xml users.xml
    DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server
    COMPONENT clickhouse)
