include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_generate_function.cmake)

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
  FunctionArrayUniq
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


add_headers_and_sources(clickhouse_functions .)
add_headers_and_sources(clickhouse_functions ./GatherUtils)
add_headers_and_sources(clickhouse_functions ./Conditional)
add_headers_and_sources(clickhouse_functions ${ClickHouse_BINARY_DIR}/dbms/src/Functions)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})
target_link_libraries(clickhouse_functions PUBLIC dbms PRIVATE libconsistent-hashing)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${DIVIDE_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif ()

if (USE_ICU)
    #target_link_libraries (clickhouse_functions ${ICU_LIBS})
    target_include_directories (clickhouse_functions PRIVATE ${ICU_INCLUDE_DIR})
endif ()

if (USE_VECTORCLASS)
    target_include_directories (clickhouse_functions BEFORE PUBLIC ${VECTORCLASS_INCLUDE_DIR})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
