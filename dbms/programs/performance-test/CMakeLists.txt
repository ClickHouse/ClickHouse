set(CLICKHOUSE_PERFORMANCE_TEST_SOURCES 
  ${CMAKE_CURRENT_SOURCE_DIR}/JSONString.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/StopConditionsSet.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/TestStopConditions.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/TestStats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ConfigPreprocessor.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/PerformanceTest.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/PerformanceTestInfo.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/executeQuery.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/applySubstitutions.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ReportBuilder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/PerformanceTestSuite.cpp
 )

set(CLICKHOUSE_PERFORMANCE_TEST_LINK PRIVATE dbms clickhouse_common_io clickhouse_common_config ${Boost_PROGRAM_OPTIONS_LIBRARY})
set(CLICKHOUSE_PERFORMANCE_TEST_INCLUDE SYSTEM PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

clickhouse_program_add(performance-test)
