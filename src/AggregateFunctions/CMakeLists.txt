include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)
add_headers_and_sources(clickhouse_aggregate_functions Combinators)

extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
    AggregateFunctionFactory.cpp # AggregateFunctionFactory::instance() Used many times
    Combinators/AggregateFunctionCombinatorFactory.cpp # AggregateFunctionCombinatorFactory::instance() Used by AggregateFunctionFactory.cpp
    AggregateFunctionCount.cpp # Used in optimizeUseAggregateProjection.cpp
    IAggregateFunction.cpp # For AggregateFunctionCount.cpp, WindowTransform.cpp, FunctionsConversion.cpp...
    parseAggregateFunctionParameters.cpp # getAggregateFunctionNameAndParametersArray, getAggregateFunctionParametersArray (Graphite.cpp, ExpressionAnalyzer.cpp)
)
extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
    IAggregateFunction.h
    Combinators/IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    Combinators/AggregateFunctionCombinatorFactory.h
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
