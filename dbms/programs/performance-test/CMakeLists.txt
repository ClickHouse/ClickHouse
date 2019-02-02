add_library (clickhouse-performance-test-lib ${LINK_MODE}
  JSONString.cpp
  StopConditionsSet.cpp
  TestStopConditions.cpp
  TestStats.cpp
  ConfigPreprocessor.cpp
  PerformanceTest.cpp
  PerformanceTestInfo.cpp
  executeQuery.cpp
  applySubstitutions.cpp
  ReportBuilder.cpp
  PerformanceTestSuite.cpp
)
target_link_libraries (clickhouse-performance-test-lib PRIVATE dbms clickhouse_common_io clickhouse_common_config ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-performance-test-lib SYSTEM PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-performance-test clickhouse-performance-test.cpp)
    target_link_libraries (clickhouse-performance-test PRIVATE clickhouse-performance-test-lib)
    install (TARGETS clickhouse-performance-test ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif ()
