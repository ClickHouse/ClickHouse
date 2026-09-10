set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")

add_library(_usearch INTERFACE)
target_include_directories(_usearch SYSTEM INTERFACE ${USEARCH_PROJECT_DIR}/include)

target_link_libraries(_usearch INTERFACE _fp16)
target_compile_definitions(_usearch INTERFACE USEARCH_USE_FP16LIB)

# SimSIMD supports x86 and ARM platforms. The latter requires Clang 19 because older versions had a buggy bf16 implementation.
if (ARCH_AMD64 OR (ARCH_AARCH64 AND NOT NO_ARMV81_OR_HIGHER AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 19))
    target_link_libraries(_usearch INTERFACE _simsimd)
    target_compile_definitions(_usearch INTERFACE USEARCH_USE_SIMSIMD)

    target_compile_definitions(_usearch INTERFACE USEARCH_CAN_COMPILE_FLOAT16)
    target_compile_definitions(_usearch INTERFACE USEARCH_CAN_COMPILE_BF16)
endif ()

add_library(ch_contrib::usearch ALIAS _usearch)
