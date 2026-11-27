include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)
add_headers_and_sources(clickhouse_aggregate_functions Combinators)
add_headers_and_sources(clickhouse_aggregate_functions TimeSeries)

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

# TODO: at v3 the `UniquesHashSet::merge` probe loop lowers `good(hash) = (hash >> s) << s == hash`
# to BMI2 `bzhi`/`shlxq`/`shrxq`, which is slightly slower than the legacy `shl %cl + not + test`
# sequence on Sapphire Rapids (regresses `uniq_stored #1` by ~9%). Disabling BMI1/BMI2 on this TU
# keeps the rest of v3 (AVX2/FMA/etc.) while restoring the legacy shift codegen.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
    set_source_files_properties(AggregateFunctionUniq.cpp PROPERTIES COMPILE_FLAGS "-mno-bmi -mno-bmi2")
endif()

add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_headers} ${clickhouse_aggregate_functions_sources})
target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash)

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
