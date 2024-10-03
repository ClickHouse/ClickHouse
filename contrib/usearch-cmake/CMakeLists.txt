set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")

add_library(_usearch INTERFACE)
target_include_directories(_usearch SYSTEM INTERFACE ${USEARCH_PROJECT_DIR}/include)

target_link_libraries(_usearch INTERFACE _fp16)
target_compile_definitions(_usearch INTERFACE USEARCH_USE_FP16LIB)

if (ARCH_AMD64)
    # only x86 for now because the _bf16 type seems unavailable on ARM
    target_link_libraries(_usearch INTERFACE _simsimd)
    target_compile_definitions(_usearch INTERFACE USEARCH_USE_SIMSIMD)
endif()

add_library(ch_contrib::usearch ALIAS _usearch)
