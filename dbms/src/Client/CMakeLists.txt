add_library (clickhouse-client Client.cpp)
target_link_libraries (clickhouse-client dbms ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
install (FILES config.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client)

add_library (clickhouse-benchmark Benchmark.cpp)
target_link_libraries (clickhouse-benchmark dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (ENABLE_TESTS)
	add_subdirectory (tests)
endif (ENABLE_TESTS)
