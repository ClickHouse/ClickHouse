include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_aggregate_functions .)

list(REMOVE_ITEM clickhouse_aggregate_functions_sources
    AggregateFunctionFactory.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionState.cpp
    parseAggregateFunctionParameters.cpp
    FactoryHelpers.cpp
)

list(REMOVE_ITEM clickhouse_aggregate_functions_headers
    IAggregateFunction.h
    IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    AggregateFunctionCombinatorFactory.h
    AggregateFunctionState.h
    parseAggregateFunctionParameters.h
    FactoryHelpers.h
)

add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_sources})
target_link_libraries(clickhouse_aggregate_functions dbms)
target_include_directories (clickhouse_aggregate_functions BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
