include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_generate_function.cmake)

set (FUNCTIONS_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated/)

generate_function_register(Arithmetic
    FunctionPlus
    FunctionMinus
    FunctionMultiply
    FunctionDivideFloating
    FunctionDivideIntegral
    FunctionDivideIntegralOrZero
    FunctionModulo
    FunctionNegate
    FunctionAbs
    FunctionBitAnd
    FunctionBitOr
    FunctionBitXor
    FunctionBitNot
    FunctionBitShiftLeft
    FunctionBitShiftRight
    FunctionBitRotateLeft
    FunctionBitRotateRight
    FunctionLeast
    FunctionGreatest
    FunctionBitTest
    FunctionBitTestAny
    FunctionBitTestAll
    FunctionGCD
    FunctionLCM
    FunctionIntExp2
    FunctionIntExp10
)

generate_function_register(Array
    FunctionArray
    FunctionArrayElement
    FunctionHas
    FunctionIndexOf
    FunctionCountEqual
    FunctionArrayEnumerate
    FunctionArrayEnumerateUniq
    FunctionArrayEnumerateDense
    FunctionArrayUniq
    FunctionArrayDistinct
    FunctionEmptyArrayUInt8
    FunctionEmptyArrayUInt16
    FunctionEmptyArrayUInt32
    FunctionEmptyArrayUInt64
    FunctionEmptyArrayInt8
    FunctionEmptyArrayInt16
    FunctionEmptyArrayInt32
    FunctionEmptyArrayInt64
    FunctionEmptyArrayFloat32
    FunctionEmptyArrayFloat64
    FunctionEmptyArrayDate
    FunctionEmptyArrayDateTime
    FunctionEmptyArrayString
    FunctionEmptyArrayToSingle
    FunctionRange
    FunctionArrayReduce
    FunctionArrayReverse
    FunctionArrayConcat
    FunctionArraySlice
    FunctionArrayPushBack
    FunctionArrayPushFront
    FunctionArrayPopBack
    FunctionArrayPopFront
    FunctionArrayHasAll
    FunctionArrayHasAny
    FunctionArrayIntersect
    FunctionArrayResize
)

generate_function_register(Projection
    FunctionOneOrZero
    FunctionProject
    FunctionBuildProjectionComposition
    FunctionRestoreProjection
)


add_headers_and_sources(clickhouse_functions .)
add_headers_and_sources(clickhouse_functions ./GatherUtils)
add_headers_and_sources(clickhouse_functions ./Conditional)
#add_headers_and_sources(clickhouse_functions ${ClickHouse_BINARY_DIR}/dbms/src/Functions)
add_headers_and_sources(clickhouse_functions ${FUNCTIONS_GENERATED_DIR})


list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions PUBLIC dbms PRIVATE ${CONSISTENT_HASHING_LIBRARY} consistent-hashing-sumbur ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES} murmurhash)

target_include_directories (clickhouse_functions SYSTEM BEFORE PUBLIC ${DIVIDE_INCLUDE_DIR})

if (CONSISTENT_HASHING_INCLUDE_DIR)
    target_include_directories (clickhouse_functions PRIVATE ${CONSISTENT_HASHING_INCLUDE_DIR})
endif ()

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif ()

if (USE_ICU)
    #target_link_libraries (clickhouse_functions ${ICU_LIBS})
    target_include_directories (clickhouse_functions SYSTEM PRIVATE ${ICU_INCLUDE_DIR})
endif ()

if (USE_VECTORCLASS)
    target_include_directories (clickhouse_functions SYSTEM BEFORE PUBLIC ${VECTORCLASS_INCLUDE_DIR})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

if (USE_EMBEDDED_COMPILER)
    target_include_directories (clickhouse_functions SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()
