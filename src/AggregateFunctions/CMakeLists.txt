include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)
add_headers_and_sources(clickhouse_aggregate_functions combinators)

extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
    IAggregateFunction.cpp
    AggregateFunctionFactory.cpp
    combinators/AggregateFunctionCombinatorFactory.cpp
    combinators/AggregateFunctionState.cpp
    AggregateFunctionCount.cpp
    parseAggregateFunctionParameters.cpp
)
extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
    IAggregateFunction.h
    combinators/IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    combinators/AggregateFunctionCombinatorFactory.h
    combinators/AggregateFunctionState.h
    AggregateFunctionCount.cpp
    FactoryHelpers.h
    parseAggregateFunctionParameters.h
)

add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_headers} ${clickhouse_aggregate_functions_sources})
target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash)

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
