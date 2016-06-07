add_executable(clickhouse-client Client.cpp)
add_executable(clickhouse-benchmark Benchmark.cpp)

target_link_libraries (clickhouse-client dbms readline libboost_program_options.a)
target_link_libraries (clickhouse-benchmark dbms libboost_program_options.a)

INSTALL(TARGETS clickhouse-client RUNTIME DESTINATION bin COMPONENT clickhouse-client)
INSTALL(FILES config.xml DESTINATION /etc/clickhouse-client  COMPONENT clickhouse-client)

INSTALL(TARGETS clickhouse-benchmark RUNTIME DESTINATION bin  COMPONENT clickhouse-benchmark)

IF(TESTS)
	add_subdirectory (tests)
ENDIF(TESTS)
