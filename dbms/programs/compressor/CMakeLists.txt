add_library (clickhouse-compressor-lib ${LINK_MODE} Compressor.cpp)
target_link_libraries (clickhouse-compressor-lib PRIVATE clickhouse_compression clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (CLICKHOUSE_SPLIT_BINARY)
    # Also in utils
    add_executable (clickhouse-compressor clickhouse-compressor.cpp)
    target_link_libraries (clickhouse-compressor PRIVATE clickhouse-compressor-lib)
    install (TARGETS clickhouse-compressor ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif ()
